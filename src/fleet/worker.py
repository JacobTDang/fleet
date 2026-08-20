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
        # Park only if no newer push landed while this check ran; otherwise stay
        # due so the next tick consumes it (see db.consume_push).
        consumed = db.consume_push(conn, w["id"], w["pushed_value"])
        state = {"next_run_at": FAR_FUTURE if consumed else 0}
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


class JobPool:
    """Jobs outlive a tick. A claude job can legitimately run ten minutes, and
    a tick that waited for it would stall every watcher behind it and freeze the
    health heartbeat — which reads to the watchdog and the k8s liveness probe as
    "the box is down". So jobs run as background tasks; this pool owns them and
    caps how many run at once, across ticks rather than within one."""

    def __init__(self, max_concurrent=5):
        self._sem = asyncio.Semaphore(max_concurrent)
        self.tasks = set()

    def launch(self, run):
        task = asyncio.create_task(self._bounded(run))
        self.tasks.add(task)
        task.add_done_callback(self._finished)
        return task

    async def _bounded(self, run):
        async with self._sem:
            await run()

    def _finished(self, task):
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            print(f"[fleet] job task crashed: {task.exception()!r}", file=sys.stderr)

    async def drain(self):
        while self.tasks:
            await asyncio.gather(*tuple(self.tasks), return_exceptions=True)


async def tick(conn, *, client, notifier, gate, rng, llm=None, fail_threshold=3,
               max_concurrent=20, jobs_max_concurrent=5, grace_seconds=3600,
               defer_seconds=3600, global_budget=24, tz_name="UTC",
               now_fn=time.time, health=None, job_pool=None):
    """Check every due watcher, launch every due job. Without a job_pool the
    jobs are drained before returning (single-shot/test use); the daemon passes
    a long-lived pool so ticks never wait on job execution."""
    owns_pool = job_pool is None
    pool = job_pool or JobPool(jobs_max_concurrent)
    due = db.due_watchers(conn, now_fn())
    due_jobs = jobs_db.due_jobs(conn, now_fn())
    sem = asyncio.Semaphore(max_concurrent)

    async def bounded(w):
        async with sem:
            await process_watcher(conn, w, client=client, notifier=notifier,
                                  gate=gate, rng=rng, fail_threshold=fail_threshold,
                                  now_fn=now_fn, health=health, llm=llm,
                                  global_budget=global_budget, tz_name=tz_name)

    for j in due_jobs:
        if not j["running"]:
            # Claim it now: a job queued behind the pool's limit would otherwise
            # still look due on the next tick and be launched a second time.
            jobs_db.update_job_state(conn, j["id"], running=1)
        pool.launch(lambda job=j: process_job(
            conn, job, llm=llm, notifier=notifier, now_fn=now_fn,
            grace_seconds=grace_seconds, defer_seconds=defer_seconds,
            global_budget=global_budget, health=health))

    if due:
        await asyncio.gather(*(bounded(w) for w in due))
    if owns_pool:
        await pool.drain()
    if health:
        health.tick_done(now_fn())
    return len(due) + len(due_jobs)


async def _main():
    tick_seconds = float(os.environ.get("FLEET_TICK_SECONDS", "5"))
    health = Health(tick_seconds=tick_seconds)
    from fleet.webui import start_web_server  # deferred: webui imports jobrunner
    start_web_server(health, os.environ["FLEET_DB"],
                     port=int(os.environ.get("FLEET_HEALTH_PORT", "8686")))
    conn = db.connect(os.environ["FLEET_DB"])
    for interrupted in jobs_db.clear_running(conn):
        jobs_db.record_run(conn, job_id=interrupted["id"], status="fail",
                           error="interrupted by an engine restart")
        print(f"[fleet] recovered job {interrupted['name']!r} from an interrupted run",
              file=sys.stderr)
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
    job_pool = JobPool(jobs_max_concurrent)
    print(f"[fleet] worker up: db={os.environ['FLEET_DB']} tick={tick_seconds}s", flush=True)
    while True:
        n = await tick(conn, client=client, notifier=notifier, gate=gate, rng=rng,
                       llm=llm, fail_threshold=fail_threshold,
                       max_concurrent=max_concurrent,
                       jobs_max_concurrent=jobs_max_concurrent,
                       grace_seconds=grace_seconds, defer_seconds=defer_seconds,
                       global_budget=global_budget, tz_name=tz_name, health=health,
                       job_pool=job_pool)
        if n:
            print(f"[fleet] tick: {n} item(s) due", flush=True)
        await asyncio.sleep(tick_seconds)


def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
