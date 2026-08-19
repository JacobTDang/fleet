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

from fleet import db
from fleet.checkers import run_check
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
                          fail_threshold=3, now_fn=time.time, health=None):
    async with gate.slot(w["domain"]):
        result = await run_check(w, client)
    now = now_fn()
    prev_failures = w["consecutive_failures"]
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
                await _alert(conn, w, notifier, health,
                             title=w["notify_title"] or f"{w['name']} changed",
                             message=detail, kind="change")
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


async def tick(conn, *, client, notifier, gate, rng, fail_threshold=3,
               max_concurrent=20, now_fn=time.time, health=None):
    due = db.due_watchers(conn, now_fn())
    sem = asyncio.Semaphore(max_concurrent)

    async def bounded(w):
        async with sem:
            await process_watcher(conn, w, client=client, notifier=notifier,
                                  gate=gate, rng=rng, fail_threshold=fail_threshold,
                                  now_fn=now_fn, health=health)

    if due:
        await asyncio.gather(*(bounded(w) for w in due))
    if health:
        health.tick_done(now_fn())
    return len(due)


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
    fail_threshold = int(os.environ.get("FLEET_FAIL_THRESHOLD", "3"))
    max_concurrent = int(os.environ.get("FLEET_MAX_CONCURRENT", "20"))
    print(f"[fleet] worker up: db={os.environ['FLEET_DB']} tick={tick_seconds}s", flush=True)
    while True:
        n = await tick(conn, client=client, notifier=notifier, gate=gate, rng=rng,
                       fail_threshold=fail_threshold, max_concurrent=max_concurrent,
                       health=health)
        if n:
            print(f"[fleet] tick: {n} watcher(s) checked", flush=True)
        await asyncio.sleep(tick_seconds)


def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
