"""SQLite layer: watchers are rows; one engine schedules them all."""

import sqlite3
from urllib.parse import urlsplit

KINDS = ("http_json", "http_text", "script")
MIN_INTERVAL = 30

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watchers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL CHECK (kind IN ('http_json','http_text','script')),
  target TEXT NOT NULL,
  extract TEXT,
  interval_seconds INTEGER NOT NULL CHECK (interval_seconds >= 30),
  domain TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  notify_title TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS state (
  watcher_id INTEGER PRIMARY KEY REFERENCES watchers(id) ON DELETE CASCADE,
  last_value TEXT,
  last_hash TEXT,
  etag TEXT,
  last_modified TEXT,
  last_changed_at TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  next_run_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS checks (
  id INTEGER PRIMARY KEY,
  watcher_id INTEGER NOT NULL REFERENCES watchers(id) ON DELETE CASCADE,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  status TEXT NOT NULL CHECK (status IN ('ok','changed','error')),
  detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_checks_watcher_ts ON checks(watcher_id, ts);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY,
  watcher_id INTEGER REFERENCES watchers(id) ON DELETE SET NULL,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  title TEXT NOT NULL,
  message TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('change','error','recovery'))
);
"""

_STATE_FIELDS = {
    "last_value", "last_hash", "etag", "last_modified",
    "last_changed_at", "consecutive_failures", "next_run_at",
}
_WATCHER_FIELDS = {"name", "target", "extract", "interval_seconds", "notify_title", "kind"}


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _domain_for(kind, target):
    if kind == "script":
        return "local"
    netloc = urlsplit(target).netloc.lower()
    if not netloc:
        raise ValueError(f"target must be an absolute URL, got: {target!r}")
    return netloc


def create_watcher(conn, *, name, kind, target, extract=None,
                   interval_seconds=300, notify_title=None):
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got: {kind!r}")
    if interval_seconds < MIN_INTERVAL:
        raise ValueError(f"interval_seconds must be >= {MIN_INTERVAL} (politeness floor)")
    domain = _domain_for(kind, target)
    cur = conn.execute(
        "INSERT INTO watchers (name, kind, target, extract, interval_seconds, domain, notify_title)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, kind, target, extract, interval_seconds, domain, notify_title),
    )
    conn.execute("INSERT INTO state (watcher_id) VALUES (?)", (cur.lastrowid,))
    conn.commit()
    return get_watcher(conn, cur.lastrowid)


def get_watcher(conn, ident):
    col = "id" if isinstance(ident, int) else "name"
    row = conn.execute(f"SELECT * FROM watchers WHERE {col} = ?", (ident,)).fetchone()
    return dict(row) if row else None


def list_watchers(conn, enabled=None):
    q = ("SELECT w.*, s.next_run_at, s.consecutive_failures, s.last_changed_at"
         " FROM watchers w JOIN state s ON s.watcher_id = w.id")
    args = ()
    if enabled is not None:
        q += " WHERE w.enabled = ?"
        args = (1 if enabled else 0,)
    return [dict(r) for r in conn.execute(q + " ORDER BY w.id", args)]


def update_watcher(conn, watcher_id, **fields):
    unknown = set(fields) - _WATCHER_FIELDS
    if unknown:
        raise ValueError(f"unknown watcher fields: {sorted(unknown)}")
    if "interval_seconds" in fields and fields["interval_seconds"] < MIN_INTERVAL:
        raise ValueError(f"interval_seconds must be >= {MIN_INTERVAL} (politeness floor)")
    current = get_watcher(conn, watcher_id)
    if current is None:
        raise ValueError(f"no watcher with id {watcher_id}")
    kind = fields.get("kind", current["kind"])
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got: {kind!r}")
    if "target" in fields or "kind" in fields:
        fields["domain"] = _domain_for(kind, fields.get("target", current["target"]))
    cols = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE watchers SET {cols} WHERE id = ?", (*fields.values(), watcher_id))
    conn.commit()


def set_enabled(conn, watcher_id, enabled):
    conn.execute("UPDATE watchers SET enabled = ? WHERE id = ?",
                 (1 if enabled else 0, watcher_id))
    conn.commit()


def delete_watcher(conn, watcher_id):
    conn.execute("DELETE FROM watchers WHERE id = ?", (watcher_id,))
    conn.commit()


def due_watchers(conn, now):
    rows = conn.execute(
        "SELECT w.*, s.last_value, s.last_hash, s.etag, s.last_modified,"
        " s.last_changed_at, s.consecutive_failures, s.next_run_at"
        " FROM watchers w JOIN state s ON s.watcher_id = w.id"
        " WHERE w.enabled = 1 AND s.next_run_at <= ? ORDER BY s.next_run_at",
        (now,),
    )
    return [dict(r) for r in rows]


def get_state(conn, watcher_id):
    row = conn.execute("SELECT * FROM state WHERE watcher_id = ?", (watcher_id,)).fetchone()
    return dict(row) if row else None


def update_state(conn, watcher_id, **fields):
    unknown = set(fields) - _STATE_FIELDS
    if unknown:
        raise ValueError(f"unknown state fields: {sorted(unknown)}")
    cols = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE state SET {cols} WHERE watcher_id = ?",
                 (*fields.values(), watcher_id))
    conn.commit()


def record_check(conn, watcher_id, status, detail=None):
    conn.execute("INSERT INTO checks (watcher_id, status, detail) VALUES (?, ?, ?)",
                 (watcher_id, status, detail))
    conn.commit()


def record_alert(conn, watcher_id, *, title, message, kind):
    conn.execute("INSERT INTO alerts (watcher_id, title, message, kind) VALUES (?, ?, ?, ?)",
                 (watcher_id, title, message, kind))
    conn.commit()


def recent_alerts(conn, limit=20):
    rows = conn.execute(
        "SELECT a.*, w.name FROM alerts a LEFT JOIN watchers w ON w.id = a.watcher_id"
        " ORDER BY a.id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]


def recent_errors(conn, limit=20):
    rows = conn.execute(
        "SELECT c.*, w.name FROM checks c JOIN watchers w ON w.id = c.watcher_id"
        " WHERE c.status = 'error' ORDER BY c.id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]


def stats(conn):
    one = lambda q, *a: conn.execute(q, a).fetchone()[0]
    return {
        "watchers": one("SELECT COUNT(*) FROM watchers"),
        "enabled": one("SELECT COUNT(*) FROM watchers WHERE enabled = 1"),
        "failing": one("SELECT COUNT(*) FROM state WHERE consecutive_failures > 0"),
        "alerts_24h": one("SELECT COUNT(*) FROM alerts WHERE ts >= datetime('now','-1 day')"),
        "checks_24h": one("SELECT COUNT(*) FROM checks WHERE ts >= datetime('now','-1 day')"),
    }


def prune_checks(conn, keep_days=30):
    cur = conn.execute("DELETE FROM checks WHERE ts < datetime('now', ?)",
                       (f"-{int(keep_days)} day",))
    conn.commit()
    return cur.rowcount
