import sqlite3

from fleet import db

V1_SCHEMA = """
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


def make_v1_db(path):
    c = sqlite3.connect(path)
    c.executescript(V1_SCHEMA)
    c.execute("INSERT INTO watchers (name, kind, target, extract, interval_seconds, domain)"
              " VALUES ('xrp', 'http_json', 'https://api.x.com/p', 'price', 300, 'api.x.com')")
    c.execute("INSERT INTO state (watcher_id, last_value, consecutive_failures, next_run_at)"
              " VALUES (1, '3.14', 2, 999.5)")
    c.execute("INSERT INTO checks (watcher_id, status, detail) VALUES (1, 'changed', 'a -> b')")
    c.execute("INSERT INTO alerts (watcher_id, title, message, kind) VALUES (1, 't', 'm', 'change')")
    c.commit()
    c.close()


def test_v1_database_migrates_in_place_with_data_intact(tmp_path):
    p = tmp_path / "v1.db"
    make_v1_db(p)
    conn = db.connect(p)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
    w = db.get_watcher(conn, "xrp")
    assert w["target"] == "https://api.x.com/p" and w["extract"] == "price"
    assert w["cron"] is None and w["fallback_ok"] == 1 and w["handler_allow_fleetctl"] == 0
    s = db.get_state(conn, 1)
    assert s["last_value"] == "3.14" and s["consecutive_failures"] == 2
    assert s["next_run_at"] == 999.5 and s["pushed_value"] is None
    assert db.recent_alerts(conn)[0]["kind"] == "change"
    assert conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0] == 1
    # v2 tables exist and accept rows
    db.record_audit(conn, source="mcp", entity="watcher", entity_id=1, action="create")
    assert db.recent_audit(conn)[0]["action"] == "create"
    conn.close()


def test_migration_is_idempotent(tmp_path):
    p = tmp_path / "v1.db"
    make_v1_db(p)
    db.connect(p).close()
    conn = db.connect(p)  # second connect must not duplicate or fail
    assert conn.execute("SELECT COUNT(*) FROM watchers").fetchone()[0] == 1
    conn.close()


def test_fresh_db_is_already_v2(tmp_path):
    conn = db.connect(tmp_path / "fresh.db")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
    cols = {r[1] for r in conn.execute("PRAGMA table_info(watchers)")}
    assert {"cron", "handler_prompt", "handler_allow_fleetctl",
            "fallback_ok", "webhook_secret"} <= cols
    conn.close()


def test_job_alert_kind_accepted_after_migration(tmp_path):
    p = tmp_path / "v1.db"
    make_v1_db(p)
    conn = db.connect(p)
    db.record_alert(conn, None, title="digest", message="hello", kind="job")
    assert db.recent_alerts(conn)[0]["kind"] == "job"
    conn.close()
