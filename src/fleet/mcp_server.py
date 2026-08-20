"""Management MCP server: the fleet's console. The worker runs the watchers;
this is how a Claude Code session (over the tailnet) creates, inspects, and
retires them. Every tool opens a short-lived connection — WAL mode makes that
safe alongside the worker."""

import contextlib
import os
import time

import httpx
from mcp.server.mcpserver import MCPServer

from fleet import db, jobs
from fleet.checkers import run_check

mcp = MCPServer("fleet")


@contextlib.contextmanager
def _conn():
    conn = db.connect(os.environ["FLEET_DB"])
    try:
        yield conn
    finally:
        conn.close()


def _resolve(conn, ident):
    w = db.get_watcher(conn, ident)
    if w is None:
        raise ValueError(f"no watcher matching {ident!r}")
    return w


def watcher_create(name: str, kind: str, target: str, extract: str | None = None,
                   interval_seconds: int = 300, notify_title: str | None = None,
                   cron: str | None = None, handler_prompt: str | None = None,
                   fallback_ok: bool = True, expect_pattern: str | None = None,
                   headers: str | None = None, min_change_pct: float | None = None,
                   timeout_seconds: int | None = None,
                   alert_max_per_hour: int = 0) -> dict:
    """Create a watcher. kind: http_json (extract=dot.path), http_text
    (extract=regex, first group), script (target=shell command, stdout is the
    value), or webhook (value is POSTed to /hook/<name>/<secret>; the secret
    is in the returned row — save it, it is shown here only). Alerts fire on
    change. cron (5-field) replaces the interval with exact-time scheduling.
    handler_prompt adds LLM triage of each change; fallback_ok=False keeps the
    handler off the free fallback tier for private data.

    Robustness guards: expect_pattern (regex the page MUST contain, else the
    check is an error — catches bot walls and soft 404s posing as changes);
    headers (JSON object; use "${ENV_VAR}" for API keys so secrets stay out of
    the database); min_change_pct (numeric values only alert on a move this
    large, measured from the last alerted value); timeout_seconds;
    alert_max_per_hour (0 = uncapped; the cap announces itself once)."""
    with _conn() as conn:
        w = db.create_watcher(conn, name=name, kind=kind, target=target,
                              extract=extract, interval_seconds=interval_seconds,
                              notify_title=notify_title, cron=cron,
                              handler_prompt=handler_prompt, fallback_ok=fallback_ok,
                              expect_pattern=expect_pattern, headers=headers,
                              min_change_pct=min_change_pct,
                              timeout_seconds=timeout_seconds,
                              alert_max_per_hour=alert_max_per_hour)
        db.record_audit(conn, source="mcp", entity="watcher", entity_id=w["id"],
                        action="create", detail={"kind": kind, "target": target})
        return w


def watcher_list(enabled_only: bool = False) -> list[dict]:
    """List watchers with their schedule and failure state."""
    with _conn() as conn:
        return db.list_watchers(conn, enabled=True if enabled_only else None)


def watcher_update(ident: int | str, name: str | None = None, target: str | None = None,
                   extract: str | None = None, interval_seconds: int | None = None,
                   notify_title: str | None = None, cron: str | None = None,
                   handler_prompt: str | None = None, expect_pattern: str | None = None,
                   headers: str | None = None, min_change_pct: float | None = None,
                   timeout_seconds: int | None = None,
                   alert_max_per_hour: int | None = None) -> dict:
    """Update a watcher (by id or name). Only provided fields change."""
    fields = {k: v for k, v in dict(name=name, target=target, extract=extract,
                                    interval_seconds=interval_seconds,
                                    notify_title=notify_title, cron=cron,
                                    handler_prompt=handler_prompt,
                                    expect_pattern=expect_pattern, headers=headers,
                                    min_change_pct=min_change_pct,
                                    timeout_seconds=timeout_seconds,
                                    alert_max_per_hour=alert_max_per_hour
                                    ).items() if v is not None}
    with _conn() as conn:
        w = _resolve(conn, ident)
        db.update_watcher(conn, w["id"], **fields)
        db.record_audit(conn, source="mcp", entity="watcher", entity_id=w["id"],
                        action="update", detail=fields)
        return db.get_watcher(conn, w["id"])


def _set_enabled(ident, enabled):
    with _conn() as conn:
        w = _resolve(conn, ident)
        db.set_enabled(conn, w["id"], enabled)
        db.record_audit(conn, source="mcp", entity="watcher", entity_id=w["id"],
                        action="resume" if enabled else "pause")
        return db.get_watcher(conn, w["id"])


def watcher_pause(ident: int | str) -> dict:
    """Pause a watcher (kept, not checked)."""
    return _set_enabled(ident, False)


def watcher_resume(ident: int | str) -> dict:
    """Resume a paused watcher."""
    return _set_enabled(ident, True)


def watcher_delete(ident: int | str) -> dict:
    """Delete a watcher and its history."""
    with _conn() as conn:
        w = _resolve(conn, ident)
        db.delete_watcher(conn, w["id"])
        db.record_audit(conn, source="mcp", entity="watcher", entity_id=w["id"],
                        action="delete")
        return {"deleted": w["name"]}


async def watcher_test(ident: int | str) -> dict:
    """Run one check right now and return the result — nothing is recorded
    and no alert fires. Use it to validate a watcher before trusting it."""
    with _conn() as conn:
        w = _resolve(conn, ident)
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await run_check(w, client)
    return {"ok": r.ok, "value": r.value, "error": r.error,
            "not_modified": r.not_modified}


def fleet_stats() -> dict:
    """Fleet-wide counts: watchers, enabled, failing, alerts/checks in 24h."""
    with _conn() as conn:
        return db.stats(conn)


def recent_alerts(limit: int = 20) -> list[dict]:
    """Most recent alerts, newest first."""
    with _conn() as conn:
        return db.recent_alerts(conn, limit=limit)


def domain_status() -> list[dict]:
    """Domains fleet is currently backing off from (a 429/403 pauses every
    watcher on that site, because they all share one egress IP). Empty is the
    healthy answer."""
    with _conn() as conn:
        return db.active_backoffs(conn, time.time())


def watcher_snapshots(ident: int | str, limit: int = 3) -> list[dict]:
    """Raw response bodies kept from checks that were refused or failed their
    expect_pattern — what the site actually served when it stopped working."""
    with _conn() as conn:
        w = _resolve(conn, ident)
        return db.recent_snapshots(conn, w["id"], limit=limit)


def recent_errors(limit: int = 20) -> list[dict]:
    """Most recent failed checks, newest first."""
    with _conn() as conn:
        return db.recent_errors(conn, limit=limit)


def _resolve_job(conn, ident):
    j = jobs.get_job(conn, ident)
    if j is None:
        raise ValueError(f"no job matching {ident!r}")
    return j


def job_create(name: str, kind: str, target: str, schedule: str, tz: str | None = None,
               notify_policy: str = "on_failure", notify_title: str | None = None,
               timeout_seconds: int | None = None, retries: int = 0,
               retry_delay_seconds: int = 60, defer_ok: bool = False,
               fallback_ok: bool = True, max_runs_per_day: int | None = None,
               model: str | None = None, allow_fleetctl: bool = False,
               handler_prompt: str | None = None) -> dict:
    """Create a scheduled job. kind: script (target = shell command) or claude
    (target = prompt; max_runs_per_day required — the budget gate). schedule is
    5-field cron evaluated in tz (default: the box timezone). notify_policy:
    on_failure | always | on_output | never. handler_prompt adds LLM triage."""
    with _conn() as conn:
        j = jobs.create_job(conn, now=time.time(), name=name, kind=kind, target=target,
                            schedule=schedule, tz=tz or os.environ.get("FLEET_TZ", "UTC"),
                            notify_policy=notify_policy, notify_title=notify_title,
                            timeout_seconds=timeout_seconds, retries=retries,
                            retry_delay_seconds=retry_delay_seconds, defer_ok=defer_ok,
                            fallback_ok=fallback_ok, max_runs_per_day=max_runs_per_day,
                            model=model, allow_fleetctl=allow_fleetctl,
                            handler_prompt=handler_prompt)
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"],
                        action="create", detail={"kind": kind, "schedule": schedule})
        return j


def job_list(enabled_only: bool = False) -> list[dict]:
    """List jobs with schedule state (next_run_at, last_status, failures)."""
    with _conn() as conn:
        return jobs.list_jobs(conn, enabled=True if enabled_only else None)


def job_update(ident: int | str, schedule: str | None = None, tz: str | None = None,
               target: str | None = None, notify_policy: str | None = None,
               notify_title: str | None = None, timeout_seconds: int | None = None,
               retries: int | None = None, max_runs_per_day: int | None = None,
               model: str | None = None, handler_prompt: str | None = None) -> dict:
    """Update a job (by id or name). Only provided fields change; a schedule/tz
    change recomputes the next fire."""
    fields = {k: v for k, v in dict(schedule=schedule, tz=tz, target=target,
                                    notify_policy=notify_policy, notify_title=notify_title,
                                    timeout_seconds=timeout_seconds, retries=retries,
                                    max_runs_per_day=max_runs_per_day, model=model,
                                    handler_prompt=handler_prompt).items() if v is not None}
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        jobs.update_job(conn, j["id"], now=time.time(), **fields)
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"],
                        action="update", detail=fields)
        return jobs.get_job(conn, j["id"])


def _set_job_enabled(ident, enabled):
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        jobs.set_job_enabled(conn, j["id"], enabled)
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"],
                        action="resume" if enabled else "pause")
        return jobs.get_job(conn, j["id"])


def job_pause(ident: int | str) -> dict:
    """Pause a job (kept, not run)."""
    return _set_job_enabled(ident, False)


def job_resume(ident: int | str) -> dict:
    """Resume a paused job."""
    return _set_job_enabled(ident, True)


def job_delete(ident: int | str) -> dict:
    """Delete a job and its run history."""
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        jobs.delete_job(conn, j["id"])
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"], action="delete")
        return {"deleted": j["name"]}


def job_run_now(ident: int | str) -> dict:
    """Fire a job on the next engine tick (a few seconds)."""
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        # "now", not 0: epoch 0 would look decades late and trip the grace window
        jobs.update_job_state(conn, j["id"], next_run_at=time.time())
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"], action="run-now")
        return {"queued": j["name"]}


def job_history(ident: int | str, limit: int = 20) -> list[dict]:
    """Recent runs for one job, newest first (status, output/error, tier)."""
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        return jobs.recent_runs(conn, limit=limit, job_id=j["id"])


for _fn in (watcher_create, watcher_list, watcher_update, watcher_pause,
            watcher_resume, watcher_delete, watcher_test,
            fleet_stats, recent_alerts, recent_errors, domain_status, watcher_snapshots,
            job_create, job_list, job_update, job_pause, job_resume,
            job_delete, job_run_now, job_history):
    mcp.tool()(_fn)


def main():
    transport = os.environ.get("FLEET_MCP_TRANSPORT", "streamable-http")
    if transport == "stdio":
        mcp.run("stdio")
        return
    host = os.environ.get("FLEET_MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("FLEET_MCP_PORT", "8765"))
    print(f"[fleet] mcp server up: {transport} on {host}:{port}", flush=True)
    mcp.run(transport, host=host, port=port)
