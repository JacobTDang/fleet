"""Management MCP server: the fleet's console. The worker runs the watchers;
this is how a Claude Code session (over the tailnet) creates, inspects, and
retires them. Every tool opens a short-lived connection — WAL mode makes that
safe alongside the worker."""

import contextlib
import os

import httpx
from mcp.server.mcpserver import MCPServer

from fleet import db
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
                   interval_seconds: int = 300, notify_title: str | None = None) -> dict:
    """Create a watcher. kind: http_json (extract=dot.path), http_text
    (extract=regex, first group), or script (target=shell command, stdout is
    the value). Alerts fire on change of the extracted value."""
    with _conn() as conn:
        return db.create_watcher(conn, name=name, kind=kind, target=target,
                                 extract=extract, interval_seconds=interval_seconds,
                                 notify_title=notify_title)


def watcher_list(enabled_only: bool = False) -> list[dict]:
    """List watchers with their schedule and failure state."""
    with _conn() as conn:
        return db.list_watchers(conn, enabled=True if enabled_only else None)


def watcher_update(ident: int | str, name: str | None = None, target: str | None = None,
                   extract: str | None = None, interval_seconds: int | None = None,
                   notify_title: str | None = None) -> dict:
    """Update a watcher (by id or name). Only provided fields change."""
    fields = {k: v for k, v in dict(name=name, target=target, extract=extract,
                                    interval_seconds=interval_seconds,
                                    notify_title=notify_title).items() if v is not None}
    with _conn() as conn:
        w = _resolve(conn, ident)
        db.update_watcher(conn, w["id"], **fields)
        return db.get_watcher(conn, w["id"])


def _set_enabled(ident, enabled):
    with _conn() as conn:
        w = _resolve(conn, ident)
        db.set_enabled(conn, w["id"], enabled)
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


def recent_errors(limit: int = 20) -> list[dict]:
    """Most recent failed checks, newest first."""
    with _conn() as conn:
        return db.recent_errors(conn, limit=limit)


for _fn in (watcher_create, watcher_list, watcher_update, watcher_pause,
            watcher_resume, watcher_delete, watcher_test,
            fleet_stats, recent_alerts, recent_errors):
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
