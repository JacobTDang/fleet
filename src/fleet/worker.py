"""The fleet daemon: every tick, run due watchers, detect change, escalate
failures. Alert discipline: change alerts on every change, error alerts exactly
once when a watcher crosses the failure threshold (flap-free), recovery alerts
when it comes back — so silence always means "quiet and healthy", never
"broken and nobody noticed"."""

import asyncio
import hashlib
import json
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

from fleet import cron, db
from fleet import jobs as jobs_db
from fleet.checkers import run_check
from fleet.jobrunner import FAR_FUTURE, process_job, run_judged
from fleet.llm import Llm
from fleet.notify import Notifier, NotifyError
from fleet.scheduler import DomainGate, next_run


def _trunc(s, n=200):
    s = s if s is not None else ""
    return s if len(s) <= n else s[: n - 1] + "…"


class Health:
    def __init__(self, tick_seconds):
        self._tick_seconds = tick_seconds
        self._lock = threading.Lock()
        self._last_tick_at = None
        self._ticks = 0
        self._notify_failures = 0

    def tick_done(self, now):
        with self._lock:
            self._last_tick_at = now
            self._ticks += 1

    def notify_failed(self):
        with self._lock:
            self._notify_failures += 1

    def payload(self, now):
        with self._lock:
            last, ticks, nf = self._last_tick_at, self._ticks, self._notify_failures
        stale_after = max(6 * self._tick_seconds, 60)
        age = None if last is None else now - last
        stale = age is None or age > stale_after
        body = {
            "status": "stale" if stale else "ok",
            "last_tick_age_s": age,
            "ticks": ticks,
            "notify_failures": nf,
        }
        return (503 if stale else 200), body


def start_health_server(health, *, port, now_fn=time.time, host="0.0.0.0"):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/health":
                self.send_error(404)
                return
            code, body = health.payload(now_fn())
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def _alert(conn, w, notifier, health, *, title, message, kind, priority="default"):
    # Record intent first: the audit row must exist even if delivery fails.
    db.record_alert(conn, w["id"], title=title, message=message, kind=kind)
    try:
        await notifier.send(title, message, priority=priority)
    except NotifyError as e:
        if health:
            health.notify_failed()
        print(f"[fleet] alert delivery failed for {w['name']!r}: {e}", file=sys.stderr)


async def process_watcher(conn, w, *, client, notifier, gate, rng,
                          fail_threshold=3, now_fn=time.time, health=None,
                          llm=None, global_budget=24, tz_name="UTC"):
    async with gate.slot(w["domain"]):
        result = await run_check(w, client)
    now = now_fn()
    prev_failures = w["consecutive_failures"]
    if w["kind"] == "webhook":
        state = {"next_run_at": FAR_FUTURE, "pushed_value": None}
    elif w.get("cron"):
        state = {"next_run_at": cron.next_fire(w["cron"], tz_name, now)}
    else:
        state = {"next_run_at": next_run(w["interval_seconds"], now=now, rng=rng)}

    if result.ok:
        if result.not_modified:
            db.record_check(conn, w["id"], "ok", detail="not modified (304)")
            state["etag"] = result.etag or w["etag"]
            state["last_modified"] = result.last_modified or w["last_modified"]
        else:
            new_hash = hashlib.sha256(result.value.encode()).hexdigest()
            state["etag"] = result.etag
            state["last_modified"] = result.last_modified
            if w["last_hash"] is None:
                db.record_check(conn, w["id"], "ok", detail="baseline")
                state.update(last_value=result.value, last_hash=new_hash)
            elif new_hash != w["last_hash"]:
                detail = f"{_trunc(w['last_value'])} -> {_trunc(result.value)}"
                db.record_check(conn, w["id"], "changed", detail=detail)
                message = detail
                if w.get("handler_prompt"):
                    context = (f"Watcher {w['name']} changed.\n"
                               f"Old: {_trunc(w['last_value'], 1000)}\n"
                               f"New: {_trunc(result.value, 1000)}")
                    message = await run_judged(
                        conn, llm, handler_prompt=w["handler_prompt"], context=context,
                        raw_message=detail, fallback_ok=bool(w["fallback_ok"]),
                        allow_fleetctl=bool(w["handler_allow_fleetctl"]),
                        watcher_id=w["id"], global_budget=global_budget)
                await _alert(conn, w, notifier, health,
                             title=w["notify_title"] or f"{w['name']} changed",
                             message=message, kind="change")
                state.update(last_value=result.value, last_hash=new_hash,
                             last_changed_at=str(now))
            else:
                db.record_check(conn, w["id"], "ok")
        if prev_failures >= fail_threshold:
            await _alert(conn, w, notifier, health,
                         title=f"{w['name']} recovered",
                         message=f"recovered after {prev_failures} failures",
                         kind="recovery")
        state["consecutive_failures"] = 0
    else:
        failures = prev_failures + 1
        db.record_check(conn, w["id"], "error", detail=result.error)
        if failures == fail_threshold:
            await _alert(conn, w, notifier, health,
                         title=f"{w['name']} failing",
                         message=f"{failures} consecutive failures: {_trunc(result.error)}",
                         kind="error", priority="high")
        state["consecutive_failures"] = failures

    db.update_state(conn, w["id"], **state)


async def tick(conn, *, client, notifier, gate, rng, llm=None, fail_threshold=3,
               max_concurrent=20, jobs_max_concurrent=5, grace_seconds=3600,
               defer_seconds=3600, global_budget=24, tz_name="UTC",
               now_fn=time.time, health=None):
    due = db.due_watchers(conn, now_fn())
    due_jobs = jobs_db.due_jobs(conn, now_fn())
    sem = asyncio.Semaphore(max_concurrent)
    jsem = asyncio.Semaphore(jobs_max_concurrent)  # a slow job never starves a watcher

    async def bounded(w):
        async with sem:
            await process_watcher(conn, w, client=client, notifier=notifier,
                                  gate=gate, rng=rng, fail_threshold=fail_threshold,
                                  now_fn=now_fn, health=health, llm=llm,
                                  global_budget=global_budget, tz_name=tz_name)

    async def bounded_job(j):
        async with jsem:
            await process_job(conn, j, llm=llm, notifier=notifier, now_fn=now_fn,
                              grace_seconds=grace_seconds, defer_seconds=defer_seconds,
                              global_budget=global_budget, health=health)

    work = [bounded(w) for w in due] + [bounded_job(j) for j in due_jobs]
    if work:
        await asyncio.gather(*work)
    if health:
        health.tick_done(now_fn())
    return len(due) + len(due_jobs)


async def _main():
    tick_seconds = float(os.environ.get("FLEET_TICK_SECONDS", "5"))
    health = Health(tick_seconds=tick_seconds)
    start_health_server(health, port=int(os.environ.get("FLEET_HEALTH_PORT", "8686")))
    conn = db.connect(os.environ["FLEET_DB"])
    client = httpx.AsyncClient(
        timeout=30.0,
        headers={"User-Agent": os.environ.get(
            "FLEET_USER_AGENT", "fleet-watcher/0.1 (personal monitoring)")},
    )
    notifier = Notifier(
        os.environ.get("NTFY_URL", "http://ntfy"),
        os.environ.get("NTFY_TOPIC", "fleet-alerts"),
        token=os.environ.get("NTFY_TOKEN") or None,
    )
    gate = DomainGate(min_gap=float(os.environ.get("FLEET_DOMAIN_MIN_GAP", "2.0")))
    rng = random.Random()
    llm = Llm(
        fallback_url=os.environ.get("FLEET_FALLBACK_LLM_URL") or None,
        fallback_key=os.environ.get("FLEET_FALLBACK_LLM_KEY") or None,
        fallback_models=[m.strip() for m in
                         os.environ.get("FLEET_FALLBACK_MODELS", "").split(",") if m.strip()],
    )
    fail_threshold = int(os.environ.get("FLEET_FAIL_THRESHOLD", "3"))
    max_concurrent = int(os.environ.get("FLEET_MAX_CONCURRENT", "20"))
    jobs_max_concurrent = int(os.environ.get("FLEET_JOBS_MAX_CONCURRENT", "5"))
    grace_seconds = float(os.environ.get("FLEET_GRACE_SECONDS", "3600"))
    defer_seconds = float(os.environ.get("FLEET_DEFER_SECONDS", "3600"))
    global_budget = int(os.environ.get("FLEET_CLAUDE_MAX_RUNS_PER_DAY", "24"))
    tz_name = os.environ.get("FLEET_TZ", "UTC")
    print(f"[fleet] worker up: db={os.environ['FLEET_DB']} tick={tick_seconds}s", flush=True)
    while True:
        n = await tick(conn, client=client, notifier=notifier, gate=gate, rng=rng,
                       llm=llm, fail_threshold=fail_threshold,
                       max_concurrent=max_concurrent,
                       jobs_max_concurrent=jobs_max_concurrent,
                       grace_seconds=grace_seconds, defer_seconds=defer_seconds,
                       global_budget=global_budget, tz_name=tz_name, health=health)
        if n:
            print(f"[fleet] tick: {n} item(s) run", flush=True)
        await asyncio.sleep(tick_seconds)


def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
