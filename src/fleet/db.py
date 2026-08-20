"""SQLite layer: watchers are rows; one engine schedules them all."""

import json
import re
import secrets as _secrets
import sqlite3
from urllib.parse import urlsplit

from fleet import cron as cron_mod

KINDS = ("http_json", "http_text", "script", "webhook")
MIN_INTERVAL = 30
SCHEMA_VERSION = 3
BASE_BACKOFF = 900.0      # 15 min after the first refusal
MAX_BACKOFF = 6 * 3600.0  # never sit on a domain longer than 6h

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watchers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL CHECK (kind IN ('http_json','http_text','script','webhook')),
  target TEXT NOT NULL,
  extract TEXT,
  interval_seconds INTEGER NOT NULL CHECK (interval_seconds >= 30),
  domain TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  notify_title TEXT,
  cron TEXT,
  handler_prompt TEXT,
  handler_allow_fleetctl INTEGER NOT NULL DEFAULT 0,
  fallback_ok INTEGER NOT NULL DEFAULT 1,
  webhook_secret TEXT,
  expect_pattern TEXT,
  min_change_pct REAL,
  headers TEXT,
  timeout_seconds INTEGER,
  alert_max_per_hour INTEGER NOT NULL DEFAULT 0,
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
  next_run_at REAL NOT NULL DEFAULT 0,
  pushed_value TEXT,
  alert_anchor TEXT
);
CREATE TABLE IF NOT EXISTS checks (
  id INTEGER PRIMARY KEY,
  watcher_id INTEGER NOT NULL REFERENCES watchers(id) ON DELETE CASCADE,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  status TEXT NOT NULL CHECK (status IN ('ok','changed','error')),
  detail TEXT,
  duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_checks_watcher_ts ON checks(watcher_id, ts);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY,
  watcher_id INTEGER REFERENCES watchers(id) ON DELETE SET NULL,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  title TEXT NOT NULL,
  message TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('change','error','recovery','job'))
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL CHECK (kind IN ('script','claude')),
  target TEXT NOT NULL,
  schedule TEXT NOT NULL,
  tz TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  notify_policy TEXT NOT NULL DEFAULT 'on_failure'
    CHECK (notify_policy IN ('on_failure','always','on_output','never')),
  notify_title TEXT,
  timeout_seconds INTEGER NOT NULL,
  retries INTEGER NOT NULL DEFAULT 0,
  retry_delay_seconds INTEGER NOT NULL DEFAULT 60,
  defer_ok INTEGER NOT NULL DEFAULT 0,
  fallback_ok INTEGER NOT NULL DEFAULT 1,
  max_runs_per_day INTEGER,
  model TEXT,
  allow_tools INTEGER NOT NULL DEFAULT 0,
  allow_fleetctl INTEGER NOT NULL DEFAULT 0,
  handler_prompt TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS job_state (
  job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  next_run_at REAL NOT NULL DEFAULT 0,
  running INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_status TEXT
);
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY,
  job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
  watcher_id INTEGER REFERENCES watchers(id) ON DELETE CASCADE,
  scheduled_for REAL,
  started_at REAL,
  finished_at REAL,
  status TEXT NOT NULL CHECK (status IN
    ('ok','fail','timeout','missed','skipped_overlap','budget_skipped','deferred')),
  exit_code INTEGER,
  output TEXT,
  error TEXT,
  attempt INTEGER NOT NULL DEFAULT 1,
  llm_tier TEXT,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  CHECK ((job_id IS NULL) != (watcher_id IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_runs_job_ts ON runs(job_id, ts);
CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  backoff_until REAL NOT NULL DEFAULT 0,
  strikes INTEGER NOT NULL DEFAULT 0,
  reason TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY,
  watcher_id INTEGER NOT NULL REFERENCES watchers(id) ON DELETE CASCADE,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_watcher ON snapshots(watcher_id, id);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  source TEXT NOT NULL CHECK (source IN ('mcp','fleetctl','engine')),
  entity TEXT NOT NULL CHECK (entity IN ('watcher','job')),
  entity_id INTEGER,
  action TEXT NOT NULL,
  detail TEXT
);
"""

_STATE_FIELDS = {
    "last_value", "last_hash", "etag", "last_modified",
    "last_changed_at", "consecutive_failures", "next_run_at", "pushed_value",
    "alert_anchor",
}
_WATCHER_FIELDS = {"name", "target", "extract", "interval_seconds", "notify_title", "kind",
                   "cron", "handler_prompt", "handler_allow_fleetctl", "fallback_ok",
                   "expect_pattern", "min_change_pct", "headers", "timeout_seconds",
                   "alert_max_per_hour"}


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    _migrate(conn)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def connect_ro(path):
    """Read-only connection for the dashboard: writes are impossible at the
    driver level, not merely avoided."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


_V3_COLUMNS = {
    "watchers": [("expect_pattern", "TEXT"), ("min_change_pct", "REAL"),
                 ("headers", "TEXT"), ("timeout_seconds", "INTEGER"),
                 ("alert_max_per_hour", "INTEGER NOT NULL DEFAULT 0")],
    "state": [("alert_anchor", "TEXT")],
    "checks": [("duration_ms", "INTEGER")],
}


def _add_missing_columns(conn):
    for table, cols in _V3_COLUMNS.items():
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, decl in cols:
            if name not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def _migrate(conn):
    v = conn.execute("PRAGMA user_version").fetchone()[0]
    if v >= SCHEMA_VERSION:
        return
    has_watchers = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='watchers'").fetchone()
    if has_watchers:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(watchers)")}
        if "cron" not in cols:
            _migrate_v1_to_v2(conn)
        _add_missing_columns(conn)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()


def _migrate_v1_to_v2(conn):
    # watchers and alerts carry CHECK constraints that must widen ('webhook',
    # 'job'); SQLite can't ALTER a CHECK, so rebuild those two tables.
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executescript("""
    BEGIN;
    CREATE TABLE watchers_v2 (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      kind TEXT NOT NULL CHECK (kind IN ('http_json','http_text','script','webhook')),
      target TEXT NOT NULL,
      extract TEXT,
      interval_seconds INTEGER NOT NULL CHECK (interval_seconds >= 30),
      domain TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      notify_title TEXT,
      cron TEXT,
      handler_prompt TEXT,
      handler_allow_fleetctl INTEGER NOT NULL DEFAULT 0,
      fallback_ok INTEGER NOT NULL DEFAULT 1,
      webhook_secret TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    INSERT INTO watchers_v2 (id, name, kind, target, extract, interval_seconds,
                             domain, enabled, notify_title, created_at)
      SELECT id, name, kind, target, extract, interval_seconds,
             domain, enabled, notify_title, created_at FROM watchers;
    DROP TABLE watchers;
    ALTER TABLE watchers_v2 RENAME TO watchers;
    CREATE TABLE alerts_v2 (
      id INTEGER PRIMARY KEY,
      watcher_id INTEGER REFERENCES watchers(id) ON DELETE SET NULL,
      ts TEXT NOT NULL DEFAULT (datetime('now')),
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN ('change','error','recovery','job'))
    );
    INSERT INTO alerts_v2 SELECT * FROM alerts;
    DROP TABLE alerts;
    ALTER TABLE alerts_v2 RENAME TO alerts;
    ALTER TABLE state ADD COLUMN pushed_value TEXT;
    COMMIT;
    """)
    conn.execute("PRAGMA foreign_keys=ON")


def _domain_for(kind, target):
    if kind == "script":
        return "local"
    if kind == "webhook":
        return "webhook"
    netloc = urlsplit(target).netloc.lower()
    if not netloc:
        raise ValueError(f"target must be an absolute URL, got: {target!r}")
    return netloc


def _validate_guards(expect_pattern=None, headers=None, min_change_pct=None,
                     timeout_seconds=None, alert_max_per_hour=None):
    """Reject bad guards at create time. A regex that never compiles or headers
    that are not JSON would otherwise fail on every check, forever."""
    if expect_pattern is not None:
        try:
            re.compile(expect_pattern)
        except re.error as e:
            raise ValueError(f"expect_pattern is not a valid regex: {e}") from e
    if headers is not None:
        try:
            parsed = json.loads(headers)
        except json.JSONDecodeError as e:
            raise ValueError(f"headers must be a JSON object: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("headers must be a JSON object")
    if min_change_pct is not None and min_change_pct < 0:
        raise ValueError("min_change_pct must be >= 0")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    if alert_max_per_hour is not None and alert_max_per_hour < 0:
        raise ValueError("alert_max_per_hour must be >= 0 (0 = no cap)")


def create_watcher(conn, *, name, kind, target, extract=None,
                   interval_seconds=300, notify_title=None, cron=None,
                   handler_prompt=None, handler_allow_fleetctl=False, fallback_ok=True,
                   expect_pattern=None, headers=None, min_change_pct=None,
                   timeout_seconds=None, alert_max_per_hour=0):
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got: {kind!r}")
    if interval_seconds < MIN_INTERVAL:
        raise ValueError(f"interval_seconds must be >= {MIN_INTERVAL} (politeness floor)")
    if cron is not None:
        cron_mod.validate(cron)
    _validate_guards(expect_pattern, headers, min_change_pct, timeout_seconds,
                     alert_max_per_hour)
    domain = _domain_for(kind, target)
    webhook_secret = _secrets.token_urlsafe(24) if kind == "webhook" else None
    cur = conn.execute(
        "INSERT INTO watchers (name, kind, target, extract, interval_seconds, domain,"
        " notify_title, cron, handler_prompt, handler_allow_fleetctl, fallback_ok,"
        " webhook_secret, expect_pattern, headers, min_change_pct, timeout_seconds,"
        " alert_max_per_hour) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, kind, target, extract, interval_seconds, domain, notify_title, cron,
         handler_prompt, 1 if handler_allow_fleetctl else 0, 1 if fallback_ok else 0,
         webhook_secret, expect_pattern, headers, min_change_pct, timeout_seconds,
         alert_max_per_hour or 0),
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
    rows = []
    for r in conn.execute(q + " ORDER BY w.id", args):
        row = dict(r)
        if row.get("headers"):
            n = len(json.loads(row["headers"]))
            row["headers"] = f"({n} header(s) set — hidden)"
        rows.append(row)
    return rows


def update_watcher(conn, watcher_id, **fields):
    unknown = set(fields) - _WATCHER_FIELDS
    if unknown:
        raise ValueError(f"unknown watcher fields: {sorted(unknown)}")
    if "interval_seconds" in fields and fields["interval_seconds"] < MIN_INTERVAL:
        raise ValueError(f"interval_seconds must be >= {MIN_INTERVAL} (politeness floor)")
    if fields.get("cron") is not None:
        cron_mod.validate(fields["cron"])
    _validate_guards(fields.get("expect_pattern"), fields.get("headers"),
                     fields.get("min_change_pct"), fields.get("timeout_seconds"),
                     fields.get("alert_max_per_hour"))
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
        " s.last_changed_at, s.consecutive_failures, s.next_run_at, s.pushed_value,"
        " s.alert_anchor"
        " FROM watchers w JOIN state s ON s.watcher_id = w.id"
        " LEFT JOIN domains d ON d.domain = w.domain"
        " WHERE w.enabled = 1 AND s.next_run_at <= ?"
        " AND COALESCE(d.backoff_until, 0) <= ? ORDER BY s.next_run_at",
        (now, now),
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


def domain_backoff(conn, domain):
    """The live backoff for a domain, or None. Backoff is per-domain because
    every watcher on a site shares one egress IP: throttling one watcher while
    nine others keep knocking is how a soft rate-limit becomes a hard ban."""
    row = conn.execute("SELECT * FROM domains WHERE domain = ?", (domain,)).fetchone()
    if row is None or row["backoff_until"] <= 0:
        return None
    return {"until": row["backoff_until"], "strikes": row["strikes"], "reason": row["reason"]}


def active_backoffs(conn, now):
    rows = conn.execute("SELECT * FROM domains WHERE backoff_until > ?"
                        " ORDER BY backoff_until DESC", (now,))
    return [dict(r) for r in rows]


def set_domain_backoff(conn, domain, *, until, reason):
    conn.execute(
        "INSERT INTO domains (domain, backoff_until, strikes, reason, updated_at)"
        " VALUES (?, ?, 1, ?, datetime('now'))"
        " ON CONFLICT(domain) DO UPDATE SET backoff_until = excluded.backoff_until,"
        " strikes = domains.strikes + 1, reason = excluded.reason,"
        " updated_at = datetime('now')",
        (domain, until, reason))
    conn.commit()
    return until


def bump_domain_backoff(conn, domain, *, now, reason):
    """Exponential per-domain backoff: a site that keeps refusing is left alone
    for longer each time, up to MAX_BACKOFF."""
    row = conn.execute("SELECT strikes FROM domains WHERE domain = ?", (domain,)).fetchone()
    strikes = (row["strikes"] if row else 0) + 1
    delay = min(BASE_BACKOFF * (2 ** (strikes - 1)), MAX_BACKOFF)
    return set_domain_backoff(conn, domain, until=now + delay, reason=reason)


def clear_domain_backoff(conn, domain):
    conn.execute("UPDATE domains SET backoff_until = 0, strikes = 0,"
                 " updated_at = datetime('now') WHERE domain = ?", (domain,))
    conn.commit()


def consume_push(conn, watcher_id, value):
    """Clear a webhook value only if it is still the one the engine processed.
    A push that lands mid-check would otherwise be erased by the clear — the
    event would vanish with no error anywhere. Returns False when that happened,
    so the caller leaves the watcher due instead of parking it."""
    cur = conn.execute(
        "UPDATE state SET pushed_value = NULL WHERE watcher_id = ? AND pushed_value IS ?",
        (watcher_id, value))
    conn.commit()
    return cur.rowcount > 0


def record_check(conn, watcher_id, status, detail=None, duration_ms=None):
    conn.execute("INSERT INTO checks (watcher_id, status, detail, duration_ms)"
                 " VALUES (?, ?, ?, ?)", (watcher_id, status, detail, duration_ms))
    conn.commit()


SNAPSHOT_MAX_BYTES = 64_000
SNAPSHOT_KEEP = 3


def add_snapshot(conn, watcher_id, body, keep=SNAPSHOT_KEEP):
    """Keep the raw body from the checks you cannot explain (a block page, a
    guard failure). Without it, diagnosing "why did my selector break" is
    guesswork against a page that has since changed again."""
    conn.execute("INSERT INTO snapshots (watcher_id, body) VALUES (?, ?)",
                 (watcher_id, (body or "")[:SNAPSHOT_MAX_BYTES]))
    conn.execute("DELETE FROM snapshots WHERE watcher_id = ? AND id NOT IN"
                 " (SELECT id FROM snapshots WHERE watcher_id = ?"
                 "  ORDER BY id DESC LIMIT ?)", (watcher_id, watcher_id, keep))
    conn.commit()


def recent_snapshots(conn, watcher_id, limit=SNAPSHOT_KEEP):
    rows = conn.execute("SELECT * FROM snapshots WHERE watcher_id = ?"
                        " ORDER BY id DESC LIMIT ?", (watcher_id, limit))
    return [dict(r) for r in rows]


def count_alerts(conn, watcher_id, *, kind=None, title_like=None, within_hours=1):
    q = ("SELECT COUNT(*) FROM alerts WHERE watcher_id = ?"
         " AND ts >= datetime('now', ?)")
    args = [watcher_id, f"-{int(within_hours)} hour"]
    if kind:
        q += " AND kind = ?"
        args.append(kind)
    if title_like:
        q += " AND title LIKE ?"
        args.append(f"%{title_like}%")
    return conn.execute(q, args).fetchone()[0]


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
    def one(q, *a):
        return conn.execute(q, a).fetchone()[0]

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


def record_audit(conn, *, source, entity, entity_id, action, detail=None):
    if isinstance(detail, (dict, list)):
        detail = json.dumps(detail, ensure_ascii=False)
    conn.execute("INSERT INTO audit (source, entity, entity_id, action, detail)"
                 " VALUES (?, ?, ?, ?, ?)", (source, entity, entity_id, action, detail))
    conn.commit()


def recent_audit(conn, limit=50):
    rows = conn.execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]
