# Fleet v2 (jobs, handlers, k3s) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend fleet with cron-scheduled jobs, LLM event handlers with a degradation ladder, webhook watchers, a read-only dashboard, an audit trail, and a `fleetctl` CLI — then a k3s deploy kit.

**Architecture:** Jobs share fleet's plumbing, not its identity: new module `jobs.py` + tables `jobs`/`job_state`/`runs`/`audit`; the existing worker tick gains a second (smaller) concurrency pool for jobs. All LLM use is short-lived headless runs behind a budget gate; the raw notification never depends on the LLM. Spec: `docs/specs/2026-08-20-jobs-k3s-design.md`.

**Tech Stack:** Python 3.12, uv, SQLite (WAL), httpx, `mcp` SDK, `cronsim` (new, approved), pytest + pytest-asyncio (`asyncio_mode = "auto"`), stdlib HTTP server, k3s + Tailscale operator.

## Global Constraints

- Dependencies: exactly one new runtime dep, `cronsim` (user-approved). No others.
- TDD: every task writes the failing test first and shows the red run before implementing.
- All 65 existing v1 tests must stay green; run the full suite at the end of every task.
- Fail loud: no swallowed exceptions. The two deliberate never-raise seams (LLM ladder, notify delivery) must return/record explicit errors.
- Mock data only in tests. Clean up debug artifacts before each commit.
- Commands: `uv run pytest`, `uv sync`. Commit after each task; message style matches `git log` (e.g. `fleet: …`, `docs: …`); commit as the user's configured git author, no AI mentions — in commit messages say "llm", not the model vendor name.
- Interval floor stays 30s; politeness gate untouched.
- Constant `FAR_FUTURE = 4_102_444_800.0` (2100-01-01) = "parked until pushed/rescheduled".
- New env vars: `FLEET_TZ` (default `UTC`), `FLEET_JOBS_MAX_CONCURRENT=5`, `FLEET_GRACE_SECONDS=3600`, `FLEET_DEFER_SECONDS=3600`, `FLEET_CLAUDE_MAX_RUNS_PER_DAY=24`, `FLEET_FALLBACK_LLM_URL`, `FLEET_FALLBACK_LLM_KEY`, `FLEET_FALLBACK_MODELS` (comma list), `CLAUDE_CODE_OAUTH_TOKEN` (consumed by the CLI itself).

## Spec refinements (decided while planning — treat as spec)

1. **Job failure alerts are immediate**, per policy, after the final retry attempt (not threshold-3 — cron cadences would mean days of silence). Recovery alert when a previously-failing job succeeds (policy ≠ `never`). `FLEET_FAIL_THRESHOLD` stays watcher-only.
2. **Webhook ingress is write-minimal**: the POST handler (sync thread) only stores `state.pushed_value` and sets `next_run_at=0`; the async engine does detection/alerts/handlers on the next tick. At-least-once via SQLite.
3. **Claude auth via `CLAUDE_CODE_OAUTH_TOKEN`** (user runs `claude setup-token` once) — no credential files mounted.
4. **Budget counts only `llm_tier='subscription'` runs** (the fallback tier has its own provider-side caps).
5. **Deferral** reschedules `next_run_at = now + FLEET_DEFER_SECONDS` without advancing the cron schedule.
6. `alerts.kind` CHECK gains `'job'` (job output/success notifications); job failures use `'error'`, recoveries `'recovery'`.
7. Tools-off is assembled by one pure function (`llm.cli_args`); exact flag names are verified against the installed CLI on deploy day (checklist item), since only our arg-assembly is unit-testable.

## File structure

- Create: `src/fleet/cron.py` (cron→next fire, tz/DST), `src/fleet/jobs.py` (jobs/runs/job_state DB layer + budget), `src/fleet/llm.py` (ladder: claude CLI, fallback endpoint, never raises), `src/fleet/jobrunner.py` (job lifecycle + `run_judged` triage helper), `src/fleet/webui.py` (dashboard pages + webhook ingress + server), `src/fleet/fleetctl.py` (CLI door)
- Modify: `src/fleet/db.py` (v2 schema + migration + audit + `connect_ro`), `src/fleet/checkers.py` (webhook kind), `src/fleet/worker.py` (jobs pool, cron watchers, handlers, web server), `src/fleet/mcp_server.py` (job tools, new watcher params, audit), `pyproject.toml`, `Dockerfile`, `docker-compose.yml` comment, `.env.example`, `README.md`
- Create: `deploy/k8s/` (kustomization, namespace, config, secrets example, fleet-core, ntfy, changedetection, services, backup-cronjob, build-import.sh, README.md)
- Tests: `tests/test_cron.py`, `tests/test_jobs_db.py`, `tests/test_llm.py`, `tests/test_jobrunner.py`, `tests/test_webui.py`, `tests/test_fleetctl.py`, `tests/test_migration.py`; extend `tests/test_checkers.py`, `tests/test_worker.py`, `tests/test_mcp.py`

---

### Task 1: `cron.py` — next-fire with timezones and DST

**Files:**
- Modify: `pyproject.toml` (add `cronsim>=2.6` to `dependencies`)
- Create: `src/fleet/cron.py`
- Test: `tests/test_cron.py`

**Interfaces:**
- Produces: `cron.validate(expr: str) -> None` (raises `ValueError` on a bad 5-field expression); `cron.next_fire(expr: str, tz_name: str, after_epoch: float) -> float` (UTC epoch of first fire strictly after `after_epoch`, computed in `tz_name`).

- [ ] **Step 1: Add the approved dependency**

In `pyproject.toml` change the dependencies list to:

```toml
dependencies = [
    "httpx>=0.27",
    "mcp>=1.2",
    "cronsim>=2.6",
]
```

Run: `cd ~/projects/fleet && uv sync`
Expected: cronsim installed.

- [ ] **Step 2: Write the failing tests**

Create `tests/test_cron.py`:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fleet import cron

NY = "America/New_York"


def epoch(y, m, d, hh, mm, tz=NY):
    return datetime(y, m, d, hh, mm, tzinfo=ZoneInfo(tz)).timestamp()


def test_next_fire_basic_daily():
    assert cron.next_fire("0 9 * * *", NY, epoch(2026, 8, 20, 8, 0)) == epoch(2026, 8, 20, 9, 0)


def test_next_fire_is_strictly_after():
    t = cron.next_fire("0 9 * * *", NY, epoch(2026, 8, 20, 9, 0))
    assert t == epoch(2026, 8, 21, 9, 0)


def test_weekday_schedule_skips_weekend():
    # 2026-08-21 is a Friday; past 9am Friday -> Monday 9am
    t = cron.next_fire("0 9 * * 1-5", NY, epoch(2026, 8, 21, 10, 0))
    assert t == epoch(2026, 8, 24, 9, 0)


def test_timezone_matters():
    after = epoch(2026, 8, 20, 8, 0, tz="UTC")
    assert cron.next_fire("0 9 * * *", "UTC", after) == epoch(2026, 8, 20, 9, 0, tz="UTC")
    assert cron.next_fire("0 9 * * *", NY, after) == epoch(2026, 8, 20, 9, 0, tz=NY)


def test_spring_forward_nonexistent_time_fires_once_after_gap():
    # US spring-forward 2026-03-08: 02:00 -> 03:00, so 02:30 does not exist.
    t1 = cron.next_fire("30 2 * * *", NY, epoch(2026, 3, 8, 1, 0))
    d1 = datetime.fromtimestamp(t1, ZoneInfo(NY))
    assert (d1.month, d1.day) == (3, 8) and d1.hour == 3  # first valid instant after the gap
    t2 = cron.next_fire("30 2 * * *", NY, t1)
    d2 = datetime.fromtimestamp(t2, ZoneInfo(NY))
    assert (d2.month, d2.day) == (3, 9)  # exactly one fire on transition day


def test_fall_back_ambiguous_time_fires_once():
    # US fall-back 2026-11-01: 01:30 occurs twice; must fire exactly once.
    t1 = cron.next_fire("30 1 * * *", NY, epoch(2026, 11, 1, 0, 0))
    d1 = datetime.fromtimestamp(t1, ZoneInfo(NY))
    assert (d1.month, d1.day) == (11, 1)
    t2 = cron.next_fire("30 1 * * *", NY, t1)
    d2 = datetime.fromtimestamp(t2, ZoneInfo(NY))
    assert (d2.month, d2.day) == (11, 2)


def test_validate_rejects_garbage_and_accepts_good():
    with pytest.raises(ValueError):
        cron.validate("not a cron")
    with pytest.raises(ValueError):
        cron.validate("61 * * * *")
    cron.validate("*/5 * * * *")
    cron.validate("0 9 * * 1-5")
```

- [ ] **Step 3: Run to verify it fails**

Run: `uv run pytest tests/test_cron.py -v`
Expected: FAIL at collection — `ImportError: cannot import name 'cron'` / `No module named 'fleet.cron'`.

- [ ] **Step 4: Implement `src/fleet/cron.py`**

```python
"""Cron-expression scheduling: next fire times computed in the job's timezone,
returned as UTC epochs. cronsim does the parsing and the DST arithmetic
(nonexistent times fire once after the gap; ambiguous times fire once)."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronsim import CronSim, CronSimError


def validate(expr):
    try:
        CronSim(expr, datetime(2026, 1, 1, tzinfo=timezone.utc))
    except CronSimError as e:
        raise ValueError(f"invalid cron expression {expr!r}: {e}") from e


def validate_tz(tz_name):
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
        raise ValueError(f"unknown timezone {tz_name!r}") from e


def next_fire(expr, tz_name, after_epoch):
    """UTC epoch of the first fire strictly after after_epoch."""
    after = datetime.fromtimestamp(after_epoch, ZoneInfo(tz_name))
    return next(CronSim(expr, after)).timestamp()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_cron.py -v`
Expected: 7 passed. If the two DST tests fail on the exact instant, print the actual fired datetime, confirm it is *within the transition day and fired exactly once*, and adjust only the asserted hour/minute to cronsim's documented convention — the invariants (once per day, next fire on the following day) must hold as written.

- [ ] **Step 6: Full suite + commit**

Run: `uv run pytest -q` — expected: 72 passed.

```bash
git add pyproject.toml uv.lock src/fleet/cron.py tests/test_cron.py
git commit -m "fleet: cron next-fire with per-tz DST handling"
```

---

### Task 2: db v2 — migration, audit, watcher columns, webhook kind

**Files:**
- Modify: `src/fleet/db.py`
- Test: `tests/test_migration.py` (new), `tests/test_db.py` (extend)

**Interfaces:**
- Produces: `db.SCHEMA_VERSION = 2`; `db.connect(path)` migrates v1 files in place; `db.connect_ro(path)` (read-only URI connection, no schema exec); `db.record_audit(conn, *, source, entity, entity_id, action, detail=None)`; `db.recent_audit(conn, limit=50) -> list[dict]`; `db.create_watcher(..., cron=None, handler_prompt=None, handler_allow_fleetctl=False, fallback_ok=True)` — `kind="webhook"` allowed, generates `webhook_secret`, domain `"webhook"`; `_STATE_FIELDS` gains `pushed_value`; `_WATCHER_FIELDS` gains the four new columns; `due_watchers` rows include `pushed_value`.
- Consumes: `cron.validate` (Task 1).

- [ ] **Step 1: Write the failing migration test**

Create `tests/test_migration.py`. `V1_SCHEMA` is the v1 `_SCHEMA` copied verbatim from git (shown complete here):

```python
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
```

And add to `tests/test_db.py`:

```python
def test_webhook_watcher_gets_secret_and_domain(conn):
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tradingview")
    assert w["domain"] == "webhook"
    assert isinstance(w["webhook_secret"], str) and len(w["webhook_secret"]) >= 20


def test_create_watcher_validates_cron(conn):
    with pytest.raises(ValueError):
        db.create_watcher(conn, name="bad", kind="script", target="echo hi", cron="nope")
    w = db.create_watcher(conn, name="mkt", kind="script", target="echo hi", cron="0 9 * * 1-5")
    assert w["cron"] == "0 9 * * 1-5"


def test_connect_ro_cannot_write(tmp_path):
    path = tmp_path / "ro.db"
    db.connect(path).close()
    ro = db.connect_ro(path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO audit (source, entity, action) VALUES ('mcp','watcher','x')")
    ro.close()
```

(`tests/test_db.py` already has a `conn` fixture and imports `pytest` and `db`; add `import sqlite3` if missing.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_migration.py tests/test_db.py -v`
Expected: new tests FAIL (`AttributeError: module 'fleet.db' has no attribute 'SCHEMA_VERSION'`, unknown column errors); the 18 old db tests still pass.

- [ ] **Step 3: Implement in `src/fleet/db.py`**

Top of file — imports and constants become:

```python
import json
import secrets as _secrets
import sqlite3
from urllib.parse import urlsplit

from fleet import cron as cron_mod

KINDS = ("http_json", "http_text", "script", "webhook")
MIN_INTERVAL = 30
SCHEMA_VERSION = 2
```

Replace `_SCHEMA` with the v2 schema (watchers/state gain columns; alerts CHECK gains `'job'`; new tables):

```python
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
  pushed_value TEXT
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
```

Field whitelists:

```python
_STATE_FIELDS = {
    "last_value", "last_hash", "etag", "last_modified",
    "last_changed_at", "consecutive_failures", "next_run_at", "pushed_value",
}
_WATCHER_FIELDS = {"name", "target", "extract", "interval_seconds", "notify_title", "kind",
                   "cron", "handler_prompt", "handler_allow_fleetctl", "fallback_ok"}
```

`connect` migrates before applying the v2 schema; add `connect_ro` and `_migrate`:

```python
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
```

`_domain_for` handles webhook before the URL check:

```python
def _domain_for(kind, target):
    if kind == "script":
        return "local"
    if kind == "webhook":
        return "webhook"
    netloc = urlsplit(target).netloc.lower()
    if not netloc:
        raise ValueError(f"target must be an absolute URL, got: {target!r}")
    return netloc
```

`create_watcher` gains the new parameters (validating cron, generating the secret):

```python
def create_watcher(conn, *, name, kind, target, extract=None,
                   interval_seconds=300, notify_title=None, cron=None,
                   handler_prompt=None, handler_allow_fleetctl=False, fallback_ok=True):
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got: {kind!r}")
    if interval_seconds < MIN_INTERVAL:
        raise ValueError(f"interval_seconds must be >= {MIN_INTERVAL} (politeness floor)")
    if cron is not None:
        cron_mod.validate(cron)
    domain = _domain_for(kind, target)
    webhook_secret = _secrets.token_urlsafe(24) if kind == "webhook" else None
    cur = conn.execute(
        "INSERT INTO watchers (name, kind, target, extract, interval_seconds, domain,"
        " notify_title, cron, handler_prompt, handler_allow_fleetctl, fallback_ok,"
        " webhook_secret) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, kind, target, extract, interval_seconds, domain, notify_title, cron,
         handler_prompt, 1 if handler_allow_fleetctl else 0, 1 if fallback_ok else 0,
         webhook_secret),
    )
    conn.execute("INSERT INTO state (watcher_id) VALUES (?)", (cur.lastrowid,))
    conn.commit()
    return get_watcher(conn, cur.lastrowid)
```

In `update_watcher`, after the existing interval check add:

```python
    if fields.get("cron") is not None:
        cron_mod.validate(fields["cron"])
```

In `due_watchers`, add `s.pushed_value` to the selected state columns (after `s.next_run_at`). Append the audit functions at the end of the file:

```python
def record_audit(conn, *, source, entity, entity_id, action, detail=None):
    if isinstance(detail, (dict, list)):
        detail = json.dumps(detail, ensure_ascii=False)
    conn.execute("INSERT INTO audit (source, entity, entity_id, action, detail)"
                 " VALUES (?, ?, ?, ?, ?)", (source, entity, entity_id, action, detail))
    conn.commit()


def recent_audit(conn, limit=50):
    rows = conn.execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (limit,))
    return [dict(r) for r in rows]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_migration.py tests/test_db.py -v`
Expected: all pass (18 old + 7 new).

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 79 passed (v1 worker/checkers/mcp tests must be untouched by the schema change).

```bash
git add src/fleet/db.py tests/test_migration.py tests/test_db.py
git commit -m "fleet: schema v2 with in-place migration, audit trail, webhook watcher kind"
```

---

### Task 3: `jobs.py` — the jobs DB layer and budget gate

**Files:**
- Create: `src/fleet/jobs.py`
- Test: `tests/test_jobs_db.py`

**Interfaces:**
- Consumes: `cron.validate`, `cron.validate_tz`, `cron.next_fire` (Task 1); `db.connect` (Task 2).
- Produces: `jobs.create_job(conn, *, now, name, kind, target, schedule, tz, ...) -> dict`; `jobs.get_job(conn, ident: int | str) -> dict | None`; `jobs.list_jobs(conn, enabled=None) -> list[dict]` (joined with `job_state`); `jobs.update_job(conn, job_id, *, now, **fields) -> None`; `jobs.set_job_enabled(conn, job_id, enabled)`; `jobs.delete_job(conn, job_id)`; `jobs.due_jobs(conn, now) -> list[dict]` (joined rows incl. `next_run_at`, `running`, `consecutive_failures`, `last_status`); `jobs.update_job_state(conn, job_id, **fields)` (whitelist `next_run_at`, `running`, `consecutive_failures`, `last_status`); `jobs.record_run(conn, *, job_id=None, watcher_id=None, status, scheduled_for=None, started_at=None, finished_at=None, exit_code=None, output=None, error=None, attempt=1, llm_tier=None) -> int`; `jobs.recent_runs(conn, limit=50, job_id=None) -> list[dict]`; `jobs.claude_runs_today(conn, job_id=None) -> int` (counts `llm_tier='subscription'` rows with `ts >= date('now')`); `jobs.budget_ok(conn, *, global_max, job=None) -> bool`; constants `jobs.JOB_KINDS`, `jobs.POLICIES`, `jobs.DEFAULT_TIMEOUT = {"script": 60, "claude": 600}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_jobs_db.py`:

```python
import pytest

from fleet import db, jobs

NOW = 1_755_000_000.0  # 2025-08-12ish; any fixed epoch works


@pytest.fixture()
def conn(tmp_path):
    c = db.connect(tmp_path / "j.db")
    yield c
    c.close()


def make(conn, **kw):
    args = dict(now=NOW, name="digest", kind="script", target="echo hi",
                schedule="0 9 * * *", tz="America/New_York")
    args.update(kw)
    return jobs.create_job(conn, **args)


def test_create_job_computes_first_fire_and_defaults(conn):
    j = make(conn)
    assert j["notify_policy"] == "on_failure" and j["timeout_seconds"] == 60
    assert j["retries"] == 0 and j["fallback_ok"] == 1 and j["defer_ok"] == 0
    st = jobs.due_jobs(conn, NOW + 7 * 86400)
    assert len(st) == 1 and st[0]["next_run_at"] > NOW  # first fire in the future, not 0


def test_create_job_validates(conn):
    with pytest.raises(ValueError):
        make(conn, name="a", kind="cronjob")
    with pytest.raises(ValueError):
        make(conn, name="b", schedule="99 * * * *")
    with pytest.raises(ValueError):
        make(conn, name="c", tz="Mars/Olympus")
    with pytest.raises(ValueError):
        make(conn, name="d", notify_policy="sometimes")


def test_claude_job_requires_budget_and_forces_no_retries(conn):
    with pytest.raises(ValueError):
        make(conn, name="ai", kind="claude", target="summarize the day")
    j = make(conn, name="ai", kind="claude", target="summarize the day",
             max_runs_per_day=3, retries=5)
    assert j["retries"] == 0 and j["model"] == "haiku" and j["timeout_seconds"] == 600


def test_get_update_delete_roundtrip(conn):
    j = make(conn)
    assert jobs.get_job(conn, "digest")["id"] == j["id"]
    jobs.update_job(conn, j["id"], now=NOW, schedule="0 8 * * *", notify_policy="always")
    j2 = jobs.get_job(conn, j["id"])
    assert j2["schedule"] == "0 8 * * *" and j2["notify_policy"] == "always"
    with pytest.raises(ValueError):
        jobs.update_job(conn, j["id"], now=NOW, nonsense=1)
    jobs.set_job_enabled(conn, j["id"], False)
    assert jobs.list_jobs(conn, enabled=True) == []
    jobs.delete_job(conn, j["id"])
    assert jobs.get_job(conn, "digest") is None
    assert conn.execute("SELECT COUNT(*) FROM job_state").fetchone()[0] == 0


def test_update_schedule_recomputes_next_fire(conn):
    j = make(conn)
    before = jobs.list_jobs(conn)[0]["next_run_at"]
    jobs.update_job(conn, j["id"], now=NOW, schedule="30 23 * * *")
    after = jobs.list_jobs(conn)[0]["next_run_at"]
    assert after != before


def test_due_jobs_only_enabled_and_due(conn):
    make(conn, name="due")
    make(conn, name="off")
    jobs.set_job_enabled(conn, jobs.get_job(conn, "off")["id"], False)
    fire = jobs.list_jobs(conn)[0]["next_run_at"]
    due = jobs.due_jobs(conn, fire + 1)
    assert [d["name"] for d in due] == ["due"]


def test_record_run_and_budget(conn):
    j = make(conn, name="ai", kind="claude", target="p", max_runs_per_day=2)
    assert jobs.budget_ok(conn, global_max=10, job=j)
    jobs.record_run(conn, job_id=j["id"], status="ok", llm_tier="subscription")
    jobs.record_run(conn, job_id=j["id"], status="ok", llm_tier="subscription")
    assert jobs.claude_runs_today(conn) == 2
    assert not jobs.budget_ok(conn, global_max=10, job=j)      # per-job cap hit
    assert not jobs.budget_ok(conn, global_max=2, job=None)    # global cap hit
    jobs.record_run(conn, job_id=j["id"], status="ok", llm_tier="fallback")
    assert jobs.claude_runs_today(conn) == 2                    # fallback doesn't count
    with pytest.raises(ValueError):
        jobs.record_run(conn, status="ok")                      # neither id set


def test_recent_runs_newest_first(conn):
    j = make(conn)
    jobs.record_run(conn, job_id=j["id"], status="ok", output="one")
    jobs.record_run(conn, job_id=j["id"], status="fail", error="two")
    rs = jobs.recent_runs(conn, job_id=j["id"])
    assert [r["status"] for r in rs] == ["fail", "ok"]
    assert rs[0]["name"] == "digest"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_jobs_db.py -v`
Expected: FAIL at collection — `No module named 'fleet.jobs'`.

- [ ] **Step 3: Implement `src/fleet/jobs.py`**

```python
"""Jobs are rows too: definitions in `jobs`, moving parts in `job_state`, every
attempt in `runs`. Same doctrine as watchers — one engine, loud failures."""

from fleet import cron

JOB_KINDS = ("script", "claude")
POLICIES = ("on_failure", "always", "on_output", "never")
DEFAULT_TIMEOUT = {"script": 60, "claude": 600}

_JOB_FIELDS = {"name", "target", "schedule", "tz", "notify_policy", "notify_title",
               "timeout_seconds", "retries", "retry_delay_seconds", "defer_ok",
               "fallback_ok", "max_runs_per_day", "model", "allow_tools",
               "allow_fleetctl", "handler_prompt"}
_JOB_STATE_FIELDS = {"next_run_at", "running", "consecutive_failures", "last_status"}


def _validate(kind, schedule, tz, notify_policy):
    if kind not in JOB_KINDS:
        raise ValueError(f"kind must be one of {JOB_KINDS}, got: {kind!r}")
    if notify_policy not in POLICIES:
        raise ValueError(f"notify_policy must be one of {POLICIES}, got: {notify_policy!r}")
    cron.validate(schedule)
    cron.validate_tz(tz)


def create_job(conn, *, now, name, kind, target, schedule, tz,
               notify_policy="on_failure", notify_title=None, timeout_seconds=None,
               retries=0, retry_delay_seconds=60, defer_ok=False, fallback_ok=True,
               max_runs_per_day=None, model=None, allow_tools=False,
               allow_fleetctl=False, handler_prompt=None):
    _validate(kind, schedule, tz, notify_policy)
    if kind == "claude":
        if max_runs_per_day is None:
            raise ValueError("claude jobs require max_runs_per_day (the budget gate)")
        retries = 0  # never auto-retry a metered run
        model = model or "haiku"
    timeout_seconds = timeout_seconds or DEFAULT_TIMEOUT[kind]
    cur = conn.execute(
        "INSERT INTO jobs (name, kind, target, schedule, tz, notify_policy, notify_title,"
        " timeout_seconds, retries, retry_delay_seconds, defer_ok, fallback_ok,"
        " max_runs_per_day, model, allow_tools, allow_fleetctl, handler_prompt)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, kind, target, schedule, tz, notify_policy, notify_title,
         timeout_seconds, retries, retry_delay_seconds, 1 if defer_ok else 0,
         1 if fallback_ok else 0, max_runs_per_day, model,
         1 if allow_tools else 0, 1 if allow_fleetctl else 0, handler_prompt),
    )
    conn.execute("INSERT INTO job_state (job_id, next_run_at) VALUES (?, ?)",
                 (cur.lastrowid, cron.next_fire(schedule, tz, now)))
    conn.commit()
    return get_job(conn, cur.lastrowid)


def get_job(conn, ident):
    col = "id" if isinstance(ident, int) else "name"
    row = conn.execute(f"SELECT * FROM jobs WHERE {col} = ?", (ident,)).fetchone()
    return dict(row) if row else None


def list_jobs(conn, enabled=None):
    q = ("SELECT j.*, s.next_run_at, s.running, s.consecutive_failures, s.last_status"
         " FROM jobs j JOIN job_state s ON s.job_id = j.id")
    args = ()
    if enabled is not None:
        q += " WHERE j.enabled = ?"
        args = (1 if enabled else 0,)
    return [dict(r) for r in conn.execute(q + " ORDER BY j.id", args)]


def update_job(conn, job_id, *, now, **fields):
    unknown = set(fields) - _JOB_FIELDS
    if unknown:
        raise ValueError(f"unknown job fields: {sorted(unknown)}")
    current = get_job(conn, job_id)
    if current is None:
        raise ValueError(f"no job with id {job_id}")
    merged = {**current, **fields}
    _validate(merged["kind"], merged["schedule"], merged["tz"], merged["notify_policy"])
    cols = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE jobs SET {cols} WHERE id = ?", (*fields.values(), job_id))
    if "schedule" in fields or "tz" in fields:
        conn.execute("UPDATE job_state SET next_run_at = ? WHERE job_id = ?",
                     (cron.next_fire(merged["schedule"], merged["tz"], now), job_id))
    conn.commit()


def set_job_enabled(conn, job_id, enabled):
    conn.execute("UPDATE jobs SET enabled = ? WHERE id = ?",
                 (1 if enabled else 0, job_id))
    conn.commit()


def delete_job(conn, job_id):
    conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()


def due_jobs(conn, now):
    rows = conn.execute(
        "SELECT j.*, s.next_run_at, s.running, s.consecutive_failures, s.last_status"
        " FROM jobs j JOIN job_state s ON s.job_id = j.id"
        " WHERE j.enabled = 1 AND s.next_run_at <= ? ORDER BY s.next_run_at", (now,))
    return [dict(r) for r in rows]


def update_job_state(conn, job_id, **fields):
    unknown = set(fields) - _JOB_STATE_FIELDS
    if unknown:
        raise ValueError(f"unknown job_state fields: {sorted(unknown)}")
    cols = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE job_state SET {cols} WHERE job_id = ?",
                 (*fields.values(), job_id))
    conn.commit()


def record_run(conn, *, job_id=None, watcher_id=None, status, scheduled_for=None,
               started_at=None, finished_at=None, exit_code=None, output=None,
               error=None, attempt=1, llm_tier=None):
    if (job_id is None) == (watcher_id is None):
        raise ValueError("record_run needs exactly one of job_id / watcher_id")
    cur = conn.execute(
        "INSERT INTO runs (job_id, watcher_id, scheduled_for, started_at, finished_at,"
        " status, exit_code, output, error, attempt, llm_tier)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (job_id, watcher_id, scheduled_for, started_at, finished_at, status,
         exit_code, output, error, attempt, llm_tier))
    conn.commit()
    return cur.lastrowid


def recent_runs(conn, limit=50, job_id=None):
    q = ("SELECT r.*, COALESCE(j.name, w.name) AS name FROM runs r"
         " LEFT JOIN jobs j ON j.id = r.job_id"
         " LEFT JOIN watchers w ON w.id = r.watcher_id")
    args = []
    if job_id is not None:
        q += " WHERE r.job_id = ?"
        args.append(job_id)
    q += " ORDER BY r.id DESC LIMIT ?"
    args.append(limit)
    return [dict(r) for r in conn.execute(q, args)]


def claude_runs_today(conn, job_id=None):
    q = ("SELECT COUNT(*) FROM runs"
         " WHERE llm_tier = 'subscription' AND ts >= date('now')")
    args = ()
    if job_id is not None:
        q += " AND job_id = ?"
        args = (job_id,)
    return conn.execute(q, args).fetchone()[0]


def budget_ok(conn, *, global_max, job=None):
    if claude_runs_today(conn) >= global_max:
        return False
    if job is not None and job.get("max_runs_per_day") is not None:
        if claude_runs_today(conn, job_id=job["id"]) >= job["max_runs_per_day"]:
            return False
    return True
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_jobs_db.py -v` — expected: 8 passed.

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 87 passed.

```bash
git add src/fleet/jobs.py tests/test_jobs_db.py
git commit -m "fleet: jobs db layer with cron scheduling and llm budget gate"
```

---

### Task 4: `llm.py` — the degradation ladder (never raises)

**Files:**
- Create: `src/fleet/llm.py`
- Test: `tests/test_llm.py`

**Interfaces:**
- Produces: `llm.LlmResult` dataclass (`ok: bool, text: str | None, tier: str | None, error: str | None, usage_limited: bool`); `llm.cli_args(prompt, *, model, allow_fleetctl=False, allow_tools=False) -> list[str]`; class `llm.Llm(fallback_url=None, fallback_key=None, fallback_models=(), client=None, exec_fn=None, cwd="/tmp")` with `has_fallback: bool` property and async methods `claude(prompt, *, model="haiku", timeout=600, allow_fleetctl=False, allow_tools=False) -> LlmResult`, `fallback(prompt) -> LlmResult`, `complete(prompt, *, model="haiku", timeout=600, allow_fleetctl=False, allow_tools=False, fallback_ok=True) -> LlmResult`. `exec_fn(args: list[str], timeout: float, cwd: str) -> (returncode, stdout, stderr)` is injectable for tests. **No method may ever raise.**
- Consumes: nothing from earlier tasks (pure execution layer; the budget lives in `jobs.budget_ok`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_llm.py`:

```python
import httpx
import pytest

from fleet.llm import Llm, cli_args


def fake_exec(code, out="", err=""):
    async def _exec(args, timeout, cwd):
        return code, out, err
    return _exec


def fallback_client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def ok_fallback(req):
    assert req.url.path.endswith("/chat/completions")
    return httpx.Response(200, json={"choices": [{"message": {"content": "judged"}}]})


def test_cli_args_tools_off_by_default():
    args = cli_args("hi", model="haiku")
    assert args[:3] == ["claude", "-p", "hi"]
    assert "--model" in args and "haiku" in args
    assert "--disallowedTools" in args           # locked down
    assert "--allowedTools" not in args


def test_cli_args_fleetctl_grant_is_narrow():
    args = cli_args("hi", model="haiku", allow_fleetctl=True)
    i = args.index("--allowedTools")
    assert "fleetctl" in args[i + 1]
    assert "--disallowedTools" not in args


def test_cli_args_allow_tools_lifts_lockdown():
    args = cli_args("hi", model="haiku", allow_tools=True)
    assert "--disallowedTools" not in args and "--allowedTools" not in args


async def test_claude_success():
    llm = Llm(exec_fn=fake_exec(0, out="the answer\n"))
    r = await llm.claude("q")
    assert r.ok and r.text == "the answer" and r.tier == "subscription"


async def test_claude_usage_limit_detected():
    llm = Llm(exec_fn=fake_exec(1, err="Claude usage limit reached, resets at 5pm"))
    r = await llm.claude("q")
    assert not r.ok and r.usage_limited


async def test_claude_other_failure_not_usage_limited():
    llm = Llm(exec_fn=fake_exec(1, err="boom"))
    r = await llm.claude("q")
    assert not r.ok and not r.usage_limited and "boom" in r.error


async def test_exec_crash_never_raises():
    async def explode(args, timeout, cwd):
        raise OSError("no such binary")
    r = await Llm(exec_fn=explode).claude("q")
    assert not r.ok and "OSError" in r.error


async def test_complete_falls_back_and_sends_models_array():
    seen = {}

    def handler(req):
        import json
        seen.update(json.loads(req.content))
        assert req.headers["Authorization"] == "Bearer k"
        return ok_fallback(req)

    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_key="k",
              fallback_models=["m1", "m2"], client=fallback_client(handler),
              exec_fn=fake_exec(1, err="usage limit reached"))
    r = await llm.complete("q")
    assert r.ok and r.tier == "fallback" and r.text == "judged"
    assert seen["models"] == ["m1", "m2"] and seen["model"] == "m1"


async def test_complete_respects_fallback_ok_false():
    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_models=["m"],
              client=fallback_client(ok_fallback), exec_fn=fake_exec(1, err="usage limit"))
    r = await llm.complete("q", fallback_ok=False)
    assert not r.ok and r.usage_limited


async def test_fallback_http_error_never_raises():
    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_models=["m"],
              client=fallback_client(lambda req: httpx.Response(429)),
              exec_fn=fake_exec(1, err="usage limit"))
    r = await llm.complete("q")
    assert not r.ok and "429" in r.error and r.usage_limited


async def test_no_fallback_configured():
    llm = Llm(exec_fn=fake_exec(1, err="usage limit"))
    assert not llm.has_fallback
    r = await llm.complete("q")
    assert not r.ok and r.usage_limited
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_llm.py -v`
Expected: FAIL at collection — `No module named 'fleet.llm'`.

- [ ] **Step 3: Implement `src/fleet/llm.py`**

```python
"""The intelligence layer: headless Claude CLI first, OpenAI-compatible
fallback second (OpenRouter's `models` array does server-side failover), and
never an exception — the caller must always be able to fall through to the raw
alert (the prime invariant). Tools are OFF by default: handler prompts contain
attacker-influenceable scraped text, and a tools-off session can only produce
odd prose, never actions."""

import asyncio
from dataclasses import dataclass

import httpx

USAGE_LIMIT_MARKERS = ("usage limit", "rate limit", "limit reached", "out of usage")
_LOCKED_TOOLS = "Bash,Edit,Write,NotebookEdit,WebFetch,WebSearch,Task"


@dataclass
class LlmResult:
    ok: bool
    text: str | None = None
    tier: str | None = None  # 'subscription' | 'fallback'
    error: str | None = None
    usage_limited: bool = False


def cli_args(prompt, *, model, allow_fleetctl=False, allow_tools=False):
    args = ["claude", "-p", prompt, "--model", model, "--output-format", "text"]
    if allow_fleetctl:
        args += ["--allowedTools", "Bash(fleetctl *)"]
    elif not allow_tools:
        args += ["--disallowedTools", _LOCKED_TOOLS]
    return args


async def _exec_claude(args, timeout, cwd):
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=cwd)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 1, "", f"claude timed out after {timeout}s"
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


class Llm:
    def __init__(self, *, fallback_url=None, fallback_key=None, fallback_models=(),
                 client=None, exec_fn=None, cwd="/tmp"):
        self._fallback_url = fallback_url.rstrip("/") if fallback_url else None
        self._fallback_key = fallback_key
        self._fallback_models = [m for m in fallback_models if m]
        self._client = client
        self._exec = exec_fn or _exec_claude
        self._cwd = cwd

    @property
    def has_fallback(self):
        return bool(self._fallback_url and self._fallback_models)

    async def claude(self, prompt, *, model="haiku", timeout=600,
                     allow_fleetctl=False, allow_tools=False):
        try:
            code, out, err = await self._exec(
                cli_args(prompt, model=model, allow_fleetctl=allow_fleetctl,
                         allow_tools=allow_tools), timeout, self._cwd)
        except Exception as e:  # noqa: BLE001 — the ladder must never raise
            return LlmResult(ok=False, error=f"{type(e).__name__}: {e}")
        if code == 0:
            return LlmResult(ok=True, text=out.strip(), tier="subscription")
        blob = f"{out} {err}".lower()
        limited = any(m in blob for m in USAGE_LIMIT_MARKERS)
        return LlmResult(ok=False, usage_limited=limited,
                         error=(err or out).strip()[-300:] or f"claude exit {code}")

    async def fallback(self, prompt):
        if not self.has_fallback:
            return LlmResult(ok=False, error="no fallback endpoint configured")
        headers = {}
        if self._fallback_key:
            headers["Authorization"] = f"Bearer {self._fallback_key}"
        body = {"model": self._fallback_models[0], "models": self._fallback_models,
                "messages": [{"role": "user", "content": prompt}]}
        client = self._client or httpx.AsyncClient(timeout=60.0)
        try:
            resp = await client.post(f"{self._fallback_url}/chat/completions",
                                     json=body, headers=headers)
            if not resp.is_success:
                return LlmResult(ok=False, error=f"fallback HTTP {resp.status_code}")
            text = resp.json()["choices"][0]["message"]["content"]
            return LlmResult(ok=True, text=text.strip(), tier="fallback")
        except Exception as e:  # noqa: BLE001 — the ladder must never raise
            return LlmResult(ok=False, error=f"fallback {type(e).__name__}: {e}")
        finally:
            if self._client is None:
                await client.aclose()

    async def complete(self, prompt, *, model="haiku", timeout=600,
                       allow_fleetctl=False, allow_tools=False, fallback_ok=True):
        r = await self.claude(prompt, model=model, timeout=timeout,
                              allow_fleetctl=allow_fleetctl, allow_tools=allow_tools)
        if r.ok or not (fallback_ok and self.has_fallback):
            return r
        fb = await self.fallback(prompt)
        if fb.ok:
            return fb
        return LlmResult(ok=False, usage_limited=r.usage_limited,
                         error=f"{r.error}; {fb.error}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_llm.py -v` — expected: 12 passed.

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 99 passed.

```bash
git add src/fleet/llm.py tests/test_llm.py
git commit -m "fleet: llm ladder — headless cli, usage-limit detection, openai-compatible fallback"
```

---

### Task 5: `jobrunner.py` — job lifecycle + `run_judged` triage helper

**Files:**
- Create: `src/fleet/jobrunner.py`
- Test: `tests/test_jobrunner.py`

**Interfaces:**
- Consumes: `jobs.*` (Task 3), `cron.next_fire` (Task 1), `db.record_alert`, `llm.Llm`-shaped object (only `.claude/.fallback/.complete` + `has_fallback` are called — tests stub it), `notify.NotifyError`.
- Produces: `jobrunner.FAR_FUTURE = 4_102_444_800.0`; `async jobrunner.run_judged(conn, llm, *, handler_prompt, context, raw_message, fallback_ok=True, allow_fleetctl=False, model="haiku", watcher_id=None, job_id=None, global_budget=24) -> str` (**never raises**; returns judged text or `"[unjudged] " + raw_message`); `async jobrunner.process_job(conn, job, *, llm, notifier, now_fn=time.time, grace_seconds=3600, defer_seconds=3600, global_budget=24, sleep=None, health=None)` where `job` is a `jobs.due_jobs` row.
- Job notification titles: success/output alerts use `job["notify_title"] or job["name"]` with alert kind `'job'`; failures use title `f"{job['name']} failed"` kind `'error'` priority `high`; recovery `f"{job['name']} recovered"` kind `'recovery'`; missed `f"{job['name']} missed"` kind `'error'`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_jobrunner.py`:

```python
import pytest

from fleet import db, jobs
from fleet.jobrunner import FAR_FUTURE, process_job, run_judged
from fleet.llm import LlmResult
from fleet.notify import NotifyError

NOW = 1_755_000_000.0


class StubNotifier:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def send(self, title, message, **kw):
        if self.fail:
            raise NotifyError("down")
        self.sent.append((title, message, kw))


class StubLlm:
    def __init__(self, claude=None, fb=None):
        self._claude = claude or LlmResult(ok=False, error="unconfigured")
        self._fb = fb or LlmResult(ok=False, error="no fallback")
        self.calls = []

    @property
    def has_fallback(self):
        return self._fb.ok

    async def claude(self, prompt, **kw):
        self.calls.append(("claude", prompt, kw))
        return self._claude

    async def fallback(self, prompt):
        self.calls.append(("fallback", prompt))
        return self._fb

    async def complete(self, prompt, **kw):
        self.calls.append(("complete", prompt, kw))
        if self._claude.ok:
            return self._claude
        if kw.get("fallback_ok", True) and self._fb.ok:
            return self._fb
        return self._claude


@pytest.fixture()
def conn(tmp_path):
    c = db.connect(tmp_path / "r.db")
    yield c
    c.close()


def make_job(conn, **kw):
    args = dict(now=NOW - 100, name="j", kind="script", target="echo out",
                schedule="* * * * *", tz="UTC")
    args.update(kw)
    j = jobs.create_job(conn, **args)
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)  # due exactly now
    return jobs.due_jobs(conn, NOW)[-1]


async def run(conn, job, *, llm=None, notifier=None, **kw):
    notifier = notifier or StubNotifier()
    args = dict(llm=llm or StubLlm(), notifier=notifier, now_fn=lambda: NOW,
                sleep=_no_sleep)
    args.update(kw)
    await process_job(conn, job, **args)
    return notifier


async def _no_sleep(_):
    pass


async def test_script_ok_on_failure_policy_is_quiet_and_advances(conn):
    j = make_job(conn)
    n = await run(conn, j)
    assert n.sent == []
    rs = jobs.recent_runs(conn, job_id=j["id"])
    assert [r["status"] for r in rs] == ["ok"]
    assert rs[0]["output"] == "out" and rs[0]["exit_code"] == 0
    st = jobs.list_jobs(conn)[0]
    assert st["next_run_at"] > NOW and st["last_status"] == "ok"


async def test_policy_always_notifies_with_output_as_body(conn):
    j = make_job(conn, notify_policy="always", notify_title="Digest")
    n = await run(conn, j)
    assert n.sent == [("Digest", "out", {"priority": "default"})]
    assert db.recent_alerts(conn)[0]["kind"] == "job"


async def test_policy_on_output_quiet_when_empty(conn):
    j = make_job(conn, target="true", notify_policy="on_output")
    n = await run(conn, j)
    assert n.sent == []
    j2 = make_job(conn, name="loud", target="echo found", notify_policy="on_output")
    n2 = await run(conn, j2)
    assert len(n2.sent) == 1 and "found" in n2.sent[0][1]


async def test_failure_alerts_after_final_attempt_with_retries(conn):
    j = make_job(conn, target="sh -c 'echo nope >&2; exit 3'", retries=2)
    n = await run(conn, j)
    rs = jobs.recent_runs(conn, job_id=j["id"])
    assert [r["status"] for r in rs] == ["fail", "fail", "fail"]
    assert [r["attempt"] for r in rs] == [3, 2, 1]
    assert rs[0]["exit_code"] == 3 and "nope" in rs[0]["error"]
    assert len(n.sent) == 1  # only after the FINAL attempt
    title, msg, kw = n.sent[0]
    assert title == "j failed" and kw["priority"] == "high"
    assert jobs.list_jobs(conn)[0]["consecutive_failures"] == 1


async def test_policy_never_is_fully_silent_even_on_failure(conn):
    j = make_job(conn, target="false", notify_policy="never")
    n = await run(conn, j)
    assert n.sent == []
    assert jobs.recent_runs(conn, job_id=j["id"])[0]["status"] == "fail"


async def test_recovery_alert_after_failure(conn):
    j = make_job(conn, target="false")
    await run(conn, j)
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    j2 = jobs.due_jobs(conn, NOW)[0]
    j2["target"] = "echo back"  # row already fetched; simulate fixed script
    n = await run(conn, j2)
    assert any(t == "j recovered" for t, _, _ in n.sent)
    assert jobs.list_jobs(conn)[0]["consecutive_failures"] == 0


async def test_timeout_kills_and_records(conn):
    j = make_job(conn, target="sleep 30", timeout_seconds=1)
    # timeout_seconds has a floor of 1 in create; run with real sleep-less path
    import asyncio
    n = StubNotifier()
    await process_job(conn, j, llm=StubLlm(), notifier=n, now_fn=lambda: NOW,
                      sleep=_no_sleep)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "timeout" and "timed out" in r["error"]


async def test_overlap_skips_and_advances(conn):
    j = make_job(conn)
    jobs.update_job_state(conn, j["id"], running=1)
    j = jobs.due_jobs(conn, NOW)[0]
    n = await run(conn, j)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "skipped_overlap"
    assert jobs.list_jobs(conn)[0]["next_run_at"] > NOW
    assert n.sent == []


async def test_missed_beyond_grace_alerts_and_skips(conn):
    j = make_job(conn)
    jobs.update_job_state(conn, j["id"], next_run_at=NOW - 7200)  # 2h late, grace 1h
    j = jobs.due_jobs(conn, NOW)[0]
    n = await run(conn, j)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "missed"
    assert n.sent and "missed" in n.sent[0][0]
    assert jobs.list_jobs(conn)[0]["next_run_at"] > NOW


async def test_late_within_grace_still_runs(conn):
    j = make_job(conn)
    jobs.update_job_state(conn, j["id"], next_run_at=NOW - 120)  # 2 min late
    j = jobs.due_jobs(conn, NOW)[0]
    await run(conn, j)
    assert jobs.recent_runs(conn, job_id=j["id"])[0]["status"] == "ok"


async def test_claude_job_success_records_tier(conn):
    j = make_job(conn, name="ai", kind="claude", target="summarize",
                 max_runs_per_day=5, notify_policy="always")
    llm = StubLlm(claude=LlmResult(ok=True, text="all quiet", tier="subscription"))
    n = await run(conn, j, llm=llm)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "ok" and r["llm_tier"] == "subscription"
    assert n.sent[0][1] == "all quiet"


async def test_claude_budget_skip_alerts_once(conn):
    j = make_job(conn, name="ai", kind="claude", target="p", max_runs_per_day=0)
    n = await run(conn, j)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "budget_skipped"
    assert len(n.sent) == 1
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    j = jobs.due_jobs(conn, NOW)[0]
    n2 = await run(conn, j)
    assert n2.sent == []  # flap-free: already in budget_skipped state


async def test_claude_usage_limit_defers_without_advancing_schedule(conn):
    j = make_job(conn, name="ai", kind="claude", target="p", max_runs_per_day=5,
                 defer_ok=True)
    llm = StubLlm(claude=LlmResult(ok=False, usage_limited=True, error="usage limit"))
    n = await run(conn, j, llm=llm, defer_seconds=1800)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "deferred"
    st = jobs.list_jobs(conn)[0]
    assert st["next_run_at"] == NOW + 1800  # retry after reset, NOT next cron fire
    assert len(n.sent) == 1 and "defer" in n.sent[0][1].lower()


async def test_claude_falls_back_when_not_deferrable(conn):
    j = make_job(conn, name="ai", kind="claude", target="p", max_runs_per_day=5,
                 notify_policy="always")
    llm = StubLlm(claude=LlmResult(ok=False, usage_limited=True, error="usage limit"),
                  fb=LlmResult(ok=True, text="fb judged", tier="fallback"))
    n = await run(conn, j, llm=llm)
    r = jobs.recent_runs(conn, job_id=j["id"])[0]
    assert r["status"] == "ok" and r["llm_tier"] == "fallback"
    assert n.sent[0][1] == "fb judged"


async def test_run_judged_returns_text_and_records(conn):
    w = db.create_watcher(conn, name="w", kind="script", target="echo hi")
    llm = StubLlm(claude=LlmResult(ok=True, text="big move", tier="subscription"))
    out = await run_judged(conn, llm, handler_prompt="judge this",
                           context="old 1 new 2", raw_message="1 -> 2",
                           watcher_id=w["id"])
    assert out == "big move"
    r = jobs.recent_runs(conn)[0]
    assert r["watcher_id"] == w["id"] and r["llm_tier"] == "subscription"


async def test_run_judged_prime_invariant_on_any_failure(conn):
    w = db.create_watcher(conn, name="w", kind="script", target="echo hi")

    class Exploding:
        has_fallback = False

        async def complete(self, *a, **k):
            raise RuntimeError("must be caught upstream? no — stub violates contract")

    # contract-level failure (LlmResult not ok)
    llm = StubLlm(claude=LlmResult(ok=False, error="dead"))
    out = await run_judged(conn, llm, handler_prompt="j", context="c",
                           raw_message="1 -> 2", watcher_id=w["id"])
    assert out == "[unjudged] 1 -> 2"
    # budget-level failure
    out2 = await run_judged(conn, llm, handler_prompt="j", context="c",
                            raw_message="1 -> 2", watcher_id=w["id"], global_budget=0)
    assert out2 == "[unjudged] 1 -> 2"
    assert jobs.recent_runs(conn)[0]["status"] == "budget_skipped"


async def test_notify_failure_never_crashes_job(conn):
    j = make_job(conn, notify_policy="always")
    n = await run(conn, j, notifier=StubNotifier(fail=True))  # must not raise
    assert db.recent_alerts(conn)[0]["kind"] == "job"  # intent audited before send
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_jobrunner.py -v`
Expected: FAIL at collection — `No module named 'fleet.jobrunner'`.

- [ ] **Step 3: Implement `src/fleet/jobrunner.py`**

```python
"""Runs one due job through its whole lifecycle: overlap, grace/missed,
attempts with retries, notify policy, recovery, triage handler, next fire.
Prime invariant: the raw notification NEVER depends on the LLM — every LLM
failure degrades to the raw message tagged [unjudged]."""

import asyncio
import sys
import time

from fleet import cron, db, jobs
from fleet.notify import NotifyError

FAR_FUTURE = 4_102_444_800.0  # 2100-01-01: "parked until pushed/rescheduled"


def _trunc(s, n=400):
    s = s if s is not None else ""
    return s if len(s) <= n else s[: n - 1] + "…"


async def _notify(conn, notifier, health, *, title, message, kind,
                  priority="default", watcher_id=None):
    # Record intent first: the audit row must exist even if delivery fails.
    db.record_alert(conn, watcher_id, title=title, message=message, kind=kind)
    try:
        await notifier.send(title, message, priority=priority)
    except NotifyError as e:
        if health:
            health.notify_failed()
        print(f"[fleet] alert delivery failed for {title!r}: {e}", file=sys.stderr)


async def run_judged(conn, llm, *, handler_prompt, context, raw_message,
                     fallback_ok=True, allow_fleetctl=False, model="haiku",
                     watcher_id=None, job_id=None, global_budget=24):
    """Replace raw_message with an LLM judgment, or degrade loudly-but-safely:
    on ANY failure (budget, usage, network, crash) return '[unjudged] <raw>'."""
    raw = f"[unjudged] {raw_message}"
    if llm is None:
        return raw
    if not jobs.budget_ok(conn, global_max=global_budget):
        jobs.record_run(conn, watcher_id=watcher_id, job_id=job_id,
                        status="budget_skipped", error="global claude budget exhausted")
        return raw
    try:
        r = await llm.complete(f"{handler_prompt}\n\n{context}", model=model,
                               allow_fleetctl=allow_fleetctl, fallback_ok=fallback_ok)
    except Exception as e:  # noqa: BLE001 — prime invariant
        jobs.record_run(conn, watcher_id=watcher_id, job_id=job_id,
                        status="fail", error=f"handler {type(e).__name__}: {e}")
        return raw
    jobs.record_run(conn, watcher_id=watcher_id, job_id=job_id,
                    status="ok" if r.ok else "fail",
                    output=r.text, error=r.error, llm_tier=r.tier)
    return r.text if (r.ok and r.text) else raw


async def _run_script(cmd, timeout):
    """-> (exit_code | None, stdout, stderr_tail, timed_out)"""
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return None, "", f"timed out after {timeout}s", True
    return (proc.returncode, stdout.decode(errors="replace").strip(),
            stderr.decode(errors="replace").strip()[-500:], False)


async def _script_attempts(conn, job, *, scheduled_for, now_fn, sleep):
    """Retry loop for script jobs. -> (ok, output, error)"""
    attempts = job["retries"] + 1
    for attempt in range(1, attempts + 1):
        started = now_fn()
        code, out, err, timed_out = await _run_script(job["target"], job["timeout_seconds"])
        status = "ok" if code == 0 else ("timeout" if timed_out else "fail")
        jobs.record_run(conn, job_id=job["id"], status=status, scheduled_for=scheduled_for,
                        started_at=started, finished_at=now_fn(), exit_code=code,
                        output=_trunc(out, 4096), error=err or None, attempt=attempt)
        if code == 0:
            return True, out, None
        if attempt < attempts:
            await sleep(job["retry_delay_seconds"])
    error = err if code is None else f"exit {code}: {err}"
    return False, out, error


async def _claude_attempt(conn, job, llm, *, scheduled_for, now_fn,
                          defer_seconds, notifier, health):
    """One (never retried) claude attempt. -> (ok, output, error) or None if deferred."""
    started = now_fn()
    r = await llm.claude(job["target"], model=job["model"] or "haiku",
                         timeout=job["timeout_seconds"],
                         allow_fleetctl=bool(job["allow_fleetctl"]),
                         allow_tools=bool(job["allow_tools"]))
    if not r.ok and r.usage_limited and job["defer_ok"]:
        jobs.record_run(conn, job_id=job["id"], status="deferred",
                        scheduled_for=scheduled_for, started_at=started,
                        finished_at=now_fn(), error=r.error)
        if job["last_status"] != "deferred":
            await _notify(conn, notifier, health, title=f"{job['name']} deferred",
                          message=f"usage limit hit; deferring {defer_seconds}s"
                                  f" until the window resets", kind="error")
        jobs.update_job_state(conn, job["id"], last_status="deferred",
                              next_run_at=now_fn() + defer_seconds)
        return None
    if not r.ok and job["fallback_ok"] and llm.has_fallback:
        r = await llm.fallback(job["target"])
    jobs.record_run(conn, job_id=job["id"], status="ok" if r.ok else "fail",
                    scheduled_for=scheduled_for, started_at=started,
                    finished_at=now_fn(), output=_trunc(r.text, 4096),
                    error=r.error, llm_tier=r.tier)
    return r.ok, r.text or "", r.error


async def process_job(conn, job, *, llm, notifier, now_fn=time.time,
                      grace_seconds=3600, defer_seconds=3600, global_budget=24,
                      sleep=None, health=None):
    sleep = sleep or asyncio.sleep
    now = now_fn()
    scheduled_for = job["next_run_at"]

    def advance(from_ts):
        jobs.update_job_state(conn, job["id"],
                              next_run_at=cron.next_fire(job["schedule"], job["tz"], from_ts))

    if job["running"]:
        jobs.record_run(conn, job_id=job["id"], status="skipped_overlap",
                        scheduled_for=scheduled_for)
        advance(now)
        return

    if now - scheduled_for > grace_seconds:
        late = int(now - scheduled_for)
        jobs.record_run(conn, job_id=job["id"], status="missed", scheduled_for=scheduled_for,
                        error=f"missed by {late}s (grace {int(grace_seconds)}s)")
        await _notify(conn, notifier, health, title=f"{job['name']} missed",
                      message=f"fire time passed {late}s ago; skipped (grace window)",
                      kind="error", priority="high")
        advance(now)
        return

    if job["kind"] == "claude":
        if not jobs.budget_ok(conn, global_max=global_budget, job=job):
            jobs.record_run(conn, job_id=job["id"], status="budget_skipped",
                            scheduled_for=scheduled_for)
            if job["last_status"] != "budget_skipped":
                await _notify(conn, notifier, health,
                              title=f"{job['name']} over claude budget",
                              message="daily claude run budget exhausted; skipping",
                              kind="error", priority="high")
            jobs.update_job_state(conn, job["id"], last_status="budget_skipped")
            advance(now)
            return

    jobs.update_job_state(conn, job["id"], running=1)
    try:
        if job["kind"] == "claude":
            result = await _claude_attempt(conn, job, llm, scheduled_for=scheduled_for,
                                           now_fn=now_fn, defer_seconds=defer_seconds,
                                           notifier=notifier, health=health)
            if result is None:  # deferred; schedule already set, do not advance
                return
            ok, output, error = result
        else:
            ok, output, error = await _script_attempts(
                conn, job, scheduled_for=scheduled_for, now_fn=now_fn, sleep=sleep)
    finally:
        jobs.update_job_state(conn, job["id"], running=0)

    policy = job["notify_policy"]
    title = job["notify_title"] or job["name"]
    if ok:
        if job["consecutive_failures"] > 0 and policy != "never":
            await _notify(conn, notifier, health, title=f"{job['name']} recovered",
                          message=f"succeeded after {job['consecutive_failures']} failure(s)",
                          kind="recovery")
        if policy == "always" or (policy == "on_output" and output):
            body = output or "(no output)"
            if job["handler_prompt"]:
                body = await run_judged(conn, llm, handler_prompt=job["handler_prompt"],
                                        context=f"Job {job['name']} output:\n{output}",
                                        raw_message=body, fallback_ok=bool(job["fallback_ok"]),
                                        allow_fleetctl=bool(job["allow_fleetctl"]),
                                        model=job["model"] or "haiku",
                                        job_id=job["id"], global_budget=global_budget)
            await _notify(conn, notifier, health, title=title,
                          message=_trunc(body, 2000), kind="job")
        jobs.update_job_state(conn, job["id"], consecutive_failures=0, last_status="ok")
    else:
        failures = job["consecutive_failures"] + 1
        if policy != "never":
            body = _trunc(error, 1000)
            if job["handler_prompt"]:
                body = await run_judged(conn, llm, handler_prompt=job["handler_prompt"],
                                        context=f"Job {job['name']} FAILED:\n{error}",
                                        raw_message=body, fallback_ok=bool(job["fallback_ok"]),
                                        allow_fleetctl=bool(job["allow_fleetctl"]),
                                        model=job["model"] or "haiku",
                                        job_id=job["id"], global_budget=global_budget)
            await _notify(conn, notifier, health, title=f"{job['name']} failed",
                          message=body, kind="error", priority="high")
        jobs.update_job_state(conn, job["id"], consecutive_failures=failures,
                              last_status="fail")
    advance(now_fn())
```

Note for the implementer: `_script_attempts` references `code`/`err` after the loop — that's reachable only when the loop ended without `return`, i.e. after a final failed attempt, so both names are bound. Keep it; do not "fix" it into a pre-initialized variable without need.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_jobrunner.py -v` — expected: 17 passed. (`test_timeout_kills_and_records` really sleeps ~1s; acceptable.)

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 116 passed.

```bash
git add src/fleet/jobrunner.py tests/test_jobrunner.py
git commit -m "fleet: job lifecycle — grace, overlap, retries, policies, llm triage with raw passthrough"
```

---

### Task 6: webhook checker kind + worker integration (two pools, cron watchers, handlers)

**Files:**
- Modify: `src/fleet/checkers.py` (webhook branch), `src/fleet/worker.py` (`process_watcher` signature/behavior, `tick`, `_main` env wiring)
- Test: extend `tests/test_checkers.py`, `tests/test_worker.py`

**Interfaces:**
- Consumes: `jobrunner.process_job`, `jobrunner.run_judged`, `jobrunner.FAR_FUTURE`, `jobs.due_jobs`, `cron.next_fire`, `llm.Llm`.
- Produces: `run_check` handles `kind="webhook"` via the joined row's `pushed_value` (None → `not_modified=True`); `process_watcher(conn, w, *, client, notifier, gate, rng, fail_threshold=3, now_fn=time.time, health=None, llm=None, global_budget=24, tz_name="UTC")`; `tick(conn, *, client, notifier, gate, rng, llm=None, fail_threshold=3, max_concurrent=20, jobs_max_concurrent=5, grace_seconds=3600, defer_seconds=3600, global_budget=24, tz_name="UTC", now_fn=time.time, health=None) -> int` (due watchers + due jobs, two semaphores). Existing keyword call sites (v1 tests) keep working — every new parameter has a default.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_checkers.py`:

```python
async def test_webhook_kind_uses_pushed_value():
    w = {"kind": "webhook", "target": "hook", "pushed_value": "42"}
    r = await run_check(w, client=None)
    assert r.ok and r.value == "42"


async def test_webhook_kind_without_push_is_not_modified():
    w = {"kind": "webhook", "target": "hook", "pushed_value": None}
    r = await run_check(w, client=None)
    assert r.ok and r.not_modified
```

Append to `tests/test_worker.py` (uses the file's existing `conn` fixture, `make_env`, `run_once`, `NOW`):

```python
from fleet import jobs
from fleet.jobrunner import FAR_FUTURE
from fleet.llm import LlmResult
from fleet.worker import tick as worker_tick


async def test_webhook_watcher_full_pipeline(conn):
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tv")
    env = make_env(lambda req: httpx.Response(500))  # HTTP client must not be touched
    await run_once(conn, env)                        # first tick parks it
    s = db.get_state(conn, w["id"])
    assert s["next_run_at"] == FAR_FUTURE and env["notifier"].sent == []
    db.update_state(conn, w["id"], pushed_value="a", next_run_at=0)
    await run_once(conn, env)                        # baseline
    db.update_state(conn, w["id"], pushed_value="b", next_run_at=0)
    await run_once(conn, env)                        # change -> alert
    assert len(env["notifier"].sent) == 1
    s = db.get_state(conn, w["id"])
    assert s["pushed_value"] is None and s["next_run_at"] == FAR_FUTURE


async def test_cron_watcher_next_run_uses_schedule(conn):
    db.create_watcher(conn, name="mkt", kind="script", target="echo v",
                      cron="0 9 * * *", interval_seconds=60)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    await run_once(conn, env)
    nr = db.get_state(conn, 1)["next_run_at"]
    assert nr > NOW + 3600  # next 9am, far beyond interval+jitter


async def test_change_handler_judges_message(conn):
    class GoodLlm:
        has_fallback = False

        async def complete(self, prompt, **kw):
            assert "judge" in prompt and "v2" in prompt
            return LlmResult(ok=True, text="big deal", tier="subscription")

    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com",
                      interval_seconds=60, handler_prompt="judge this change")
    env = make_env(lambda req: httpx.Response(200, text="v1"))
    env["llm"] = GoodLlm()
    await run_once(conn, env)
    env2 = make_env(lambda req: httpx.Response(200, text="v2"))
    env2["llm"] = GoodLlm()
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env2)
    assert env2["notifier"].sent[0][1] == "big deal"
    assert jobs.recent_runs(conn)[0]["llm_tier"] == "subscription"


async def test_change_handler_failure_passes_raw_unjudged(conn):
    class DeadLlm:
        has_fallback = False

        async def complete(self, prompt, **kw):
            return LlmResult(ok=False, error="usage limit")

    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com",
                      interval_seconds=60, handler_prompt="judge")
    env = make_env(lambda req: httpx.Response(200, text="v1"))
    env["llm"] = DeadLlm()
    await run_once(conn, env)
    env2 = make_env(lambda req: httpx.Response(200, text="v2"))
    env2["llm"] = DeadLlm()
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env2)
    title, msg, _ = env2["notifier"].sent[0]
    assert msg.startswith("[unjudged] ") and "v1" in msg and "v2" in msg


async def test_tick_runs_jobs_and_watchers(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com",
                      interval_seconds=60)
    j = jobs.create_job(conn, now=NOW - 100, name="j", kind="script",
                        target="echo done", schedule="* * * * *", tz="UTC",
                        notify_policy="always")
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    n = await worker_tick(conn, **env)
    assert n == 2
    assert jobs.recent_runs(conn, job_id=j["id"])[0]["status"] == "ok"
    assert any(m == "done" for _, m, _ in env["notifier"].sent)
```

`run_once` in the file iterates due watchers with `process_watcher(conn, w, **env)`; since `env` now may carry `llm`, add `llm` to `make_env`'s dict with default `None` — change `make_env`'s returned dict to include `"llm": None` and in the new handler tests overwrite it. (`worker_tick` accepts the same keys.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_checkers.py tests/test_worker.py -v`
Expected: new tests FAIL (`unknown kind: webhook` path returns error result; `process_watcher() got an unexpected keyword argument 'llm'`); all old ones still pass.

- [ ] **Step 3: Implement**

In `src/fleet/checkers.py`, in `run_check` before the `unknown kind` fallthrough:

```python
    if kind == "webhook":
        pushed = watcher.get("pushed_value")
        if pushed is None:
            return CheckResult(ok=True, not_modified=True)
        return CheckResult(ok=True, value=pushed)
```

In `src/fleet/worker.py`:

1. Imports: add `from fleet import jobs as jobs_db`, `from fleet import cron`, `from fleet.jobrunner import FAR_FUTURE, process_job, run_judged`, `from fleet.llm import Llm`.
2. `process_watcher` signature gains `llm=None, global_budget=24, tz_name="UTC"`. Replace the single `state = {...next_run...}` line with:

```python
    if w["kind"] == "webhook":
        state = {"next_run_at": FAR_FUTURE, "pushed_value": None}
    elif w.get("cron"):
        state = {"next_run_at": cron.next_fire(w["cron"], tz_name, now)}
    else:
        state = {"next_run_at": next_run(w["interval_seconds"], now=now, rng=rng)}
```

3. In the changed-value branch, replace the `await _alert(...)` call with:

```python
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
```

4. `tick` gains the job pool (new signature per the Interfaces block above); after building `due`, add:

```python
    due_jobs = jobs_db.due_jobs(conn, now_fn())
    jsem = asyncio.Semaphore(jobs_max_concurrent)

    async def bounded_job(j):
        async with jsem:
            await process_job(conn, j, llm=llm, notifier=notifier, now_fn=now_fn,
                              grace_seconds=grace_seconds, defer_seconds=defer_seconds,
                              global_budget=global_budget, health=health)
```

and gather both lists (`bounded(w)` calls also pass `llm=llm, global_budget=global_budget, tz_name=tz_name` through to `process_watcher`); return `len(due) + len(due_jobs)`.

5. `_main` wiring — after the `gate = ...` line add:

```python
    llm = Llm(
        fallback_url=os.environ.get("FLEET_FALLBACK_LLM_URL") or None,
        fallback_key=os.environ.get("FLEET_FALLBACK_LLM_KEY") or None,
        fallback_models=[m.strip() for m in
                         os.environ.get("FLEET_FALLBACK_MODELS", "").split(",") if m.strip()],
    )
```

and pass into `tick`: `llm=llm, jobs_max_concurrent=int(os.environ.get("FLEET_JOBS_MAX_CONCURRENT", "5")), grace_seconds=float(os.environ.get("FLEET_GRACE_SECONDS", "3600")), defer_seconds=float(os.environ.get("FLEET_DEFER_SECONDS", "3600")), global_budget=int(os.environ.get("FLEET_CLAUDE_MAX_RUNS_PER_DAY", "24")), tz_name=os.environ.get("FLEET_TZ", "UTC")`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_checkers.py tests/test_worker.py -v` — expected: all pass (old 27 + new 8).

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 124 passed.

```bash
git add src/fleet/checkers.py src/fleet/worker.py tests/test_checkers.py tests/test_worker.py
git commit -m "fleet: engine runs jobs in their own pool; cron watchers, webhook kind, change triage"
```

---

### Task 7: `webui.py` — read-only dashboard + webhook ingress

**Files:**
- Create: `src/fleet/webui.py`
- Test: `tests/test_webui.py`

**Interfaces:**
- Consumes: `db.connect_ro` (Task 2), `db.list_watchers/stats/recent_alerts/recent_audit/get_watcher/update_state/connect`, `jobs.list_jobs/recent_runs/claude_runs_today` (Task 3), `checkers._dot_path/_as_text`, `jobrunner.FAR_FUTURE`, `worker.Health.payload`.
- Produces: `webui.render_index(conn, now) -> str`, `webui.render_runs(conn) -> str`, `webui.render_alerts(conn) -> str`, `webui.render_audit(conn) -> str` (full HTML pages); `webui.ingest_webhook(conn_rw, name, secret, body) -> tuple[int, str]` (204/400/403/404, never raises); `webui.start_web_server(health, db_path, *, port, now_fn=time.time, host="0.0.0.0") -> ThreadingHTTPServer` (GET `/health` JSON exactly as v1, GET pages from a per-request read-only connection, POST `/hook/<name>/<secret>` from a per-request read-write connection, 64 KB body cap → 413). Replaces `worker.start_health_server` in `_main` (old function stays for its tests).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_webui.py`:

```python
import json
import urllib.request

import pytest

from fleet import db, jobs, webui
from fleet.jobrunner import FAR_FUTURE
from fleet.worker import Health

NOW = 1_755_000_000.0


@pytest.fixture()
def dbpath(tmp_path):
    p = tmp_path / "ui.db"
    conn = db.connect(p)
    db.create_watcher(conn, name="xrp-price", kind="http_json",
                      target="https://api.x.com/p", extract="price")
    jobs.create_job(conn, now=NOW, name="morning-digest", kind="script",
                    target="echo hi", schedule="0 9 * * *", tz="UTC",
                    notify_policy="always")
    db.record_alert(conn, 1, title="xrp-price changed", message="1 -> 2", kind="change")
    db.record_audit(conn, source="mcp", entity="watcher", entity_id=1, action="create")
    jobs.record_run(conn, job_id=1, status="ok", output="hi", llm_tier="subscription")
    conn.close()
    return p


def test_pages_render_fixture_data(dbpath):
    ro = db.connect_ro(dbpath)
    idx = webui.render_index(ro, NOW)
    assert "xrp-price" in idx and "morning-digest" in idx
    assert "claude runs today" in idx and ">1<" in idx or "1 claude" in idx.replace("&nbsp;", " ")
    assert "text/html" not in idx  # it's a document, not headers
    assert "xrp-price changed" in webui.render_alerts(ro)
    assert "create" in webui.render_audit(ro)
    assert "morning-digest" in webui.render_runs(ro)
    ro.close()


def test_pages_escape_html(dbpath):
    conn = db.connect(dbpath)
    db.create_watcher(conn, name="x<script>alert(1)</script>", kind="script", target="echo hi")
    conn.close()
    ro = db.connect_ro(dbpath)
    assert "<script>alert" not in webui.render_index(ro, NOW)
    ro.close()


def test_ingest_happy_path_parks_value_and_wakes_watcher(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tv")
    code, _ = webui.ingest_webhook(conn, "hook", w["webhook_secret"], "signal-up")
    assert code == 204
    s = db.get_state(conn, w["id"])
    assert s["pushed_value"] == "signal-up" and s["next_run_at"] == 0
    conn.close()


def test_ingest_extract_dot_path(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook2", kind="webhook", target="tv",
                          extract="alert.price")
    code, _ = webui.ingest_webhook(conn, "hook2", w["webhook_secret"],
                                   json.dumps({"alert": {"price": 3.14}}))
    assert code == 204
    assert db.get_state(conn, w["id"])["pushed_value"] == "3.14"
    code, _ = webui.ingest_webhook(conn, "hook2", w["webhook_secret"], "not json")
    assert code == 400
    conn.close()


def test_ingest_rejects_bad_secret_and_unknown(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook3", kind="webhook", target="tv")
    code, _ = webui.ingest_webhook(conn, "hook3", "wrong-secret", "v")
    assert code == 403
    assert db.get_state(conn, w["id"])["pushed_value"] is None  # zero side effects
    assert webui.ingest_webhook(conn, "nope", "s", "v")[0] == 404
    # a non-webhook watcher is not a hook
    assert webui.ingest_webhook(conn, "xrp-price", "s", "v")[0] == 404
    conn.close()


def test_server_serves_pages_health_and_hook(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook4", kind="webhook", target="tv")
    conn.close()
    h = Health(tick_seconds=5)
    h.tick_done(NOW)
    server = webui.start_web_server(h, dbpath, port=0, now_fn=lambda: NOW + 1)
    try:
        port = server.server_address[1]
        base = f"http://127.0.0.1:{port}"
        with urllib.request.urlopen(f"{base}/health") as r:
            assert r.status == 200 and json.loads(r.read())["status"] == "ok"
        with urllib.request.urlopen(base) as r:
            body = r.read().decode()
            assert r.status == 200 and "xrp-price" in body
        req = urllib.request.Request(f"{base}/hook/hook4/{w['webhook_secret']}",
                                     data=b"pushed", method="POST")
        with urllib.request.urlopen(req) as r:
            assert r.status == 204
        ro = db.connect_ro(dbpath)
        assert ro.execute("SELECT pushed_value FROM state WHERE watcher_id = ?",
                          (w["id"],)).fetchone()[0] == "pushed"
        ro.close()
        bad = urllib.request.Request(f"{base}/hook/hook4/wrong", data=b"x", method="POST")
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(bad)
        assert e.value.code == 403
    finally:
        server.shutdown()
```

(Adjust the fragile `claude runs today` assertion to simply `assert "claude runs today" in idx` if the count formatting differs — the count itself is covered by `render_index` using `jobs.claude_runs_today`.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_webui.py -v`
Expected: FAIL at collection — `No module named 'fleet.webui'`.

- [ ] **Step 3: Implement `src/fleet/webui.py`**

```python
"""Read-only dashboard + webhook ingress on one stdlib HTTP server.

Pages render from a mode=ro connection — a dashboard bug physically cannot
write. The webhook POST is the single writer and does the minimum possible:
store the pushed value, wake the watcher (next_run_at=0). The async engine
does detection/alerts/handlers on its next tick, so events survive restarts."""

import hmac
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fleet import db, jobs
from fleet.checkers import _as_text, _dot_path
from fleet.jobrunner import FAR_FUTURE

MAX_BODY = 64 * 1024

_CSS = ("body{font-family:system-ui,sans-serif;margin:2rem;background:#14161a;color:#d6d8dc}"
        "table{border-collapse:collapse;width:100%;margin:0 0 2rem}"
        "td,th{border-bottom:1px solid #2c2f36;padding:.35rem .6rem;text-align:left;"
        "font-size:.9rem}th{color:#8a8f98}h1,h2{font-weight:600}"
        ".bad{color:#ff6b6b;font-weight:600}.ok{color:#69db7c}"
        "a{color:#74c0fc;text-decoration:none}small{color:#8a8f98}")


def _page(title, body):
    nav = ("<p><a href='/'>fleet</a> · <a href='/runs'>runs</a> · "
           "<a href='/alerts'>alerts</a> · <a href='/audit'>audit</a></p>")
    return ("<!doctype html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='30'>"
            f"<title>{html.escape(title)}</title><style>{_CSS}</style></head>"
            f"<body><h1>{html.escape(title)}</h1>{nav}{body}</body></html>")


def _table(headers, rows):
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def _esc(v):
    return html.escape(str(v if v is not None else ""))


def _eta(ts, now):
    if ts is None:
        return ""
    if ts >= FAR_FUTURE:
        return "on push"
    d = int(ts - now)
    return "due" if d <= 0 else f"in {d}s"


def render_index(conn, now):
    ws = sorted(db.list_watchers(conn),
                key=lambda w: (-w["consecutive_failures"], w["name"]))
    js = sorted(jobs.list_jobs(conn),
                key=lambda j: (0 if j["last_status"] in ("fail", "budget_skipped") else 1,
                               j["name"]))
    s = db.stats(conn)
    fail_cls = "bad" if s["failing"] else "ok"
    head = (f"<p>{s['watchers']} watchers ({s['enabled']} enabled, "
            f"<span class='{fail_cls}'>{s['failing']} failing</span>) · "
            f"{s['alerts_24h']} alerts/24h · "
            f"{jobs.claude_runs_today(conn)} claude runs today</p>")
    wrows = []
    for w in ws:
        st = (f"<span class='bad'>{w['consecutive_failures']} fails</span>"
              if w["consecutive_failures"] else "<span class='ok'>ok</span>")
        sched = w["cron"] or f"{w['interval_seconds']}s"
        wrows.append((_esc(w["name"]), w["kind"], st,
                      "paused" if not w["enabled"] else _eta(w["next_run_at"], now),
                      f"<small>{_esc(sched)}</small>"))
    jrows = []
    for j in js:
        st = (f"<span class='bad'>{_esc(j['last_status'])}</span>"
              if j["last_status"] in ("fail", "budget_skipped")
              else f"<span class='ok'>{_esc(j['last_status'] or 'new')}</span>")
        jrows.append((_esc(j["name"]), j["kind"], st,
                      "paused" if not j["enabled"] else _eta(j["next_run_at"], now),
                      f"<small>{_esc(j['schedule'])} {_esc(j['tz'])}</small>"))
    body = (head + "<h2>watchers</h2>"
            + _table(("name", "kind", "status", "next", "schedule"), wrows)
            + "<h2>jobs</h2>"
            + _table(("name", "kind", "status", "next", "schedule"), jrows))
    return _page("fleet", body)


def render_runs(conn):
    rows = [(_esc(r["name"]), _esc(r["status"]), _esc(r["attempt"]),
             _esc(r["exit_code"]), _esc(r["llm_tier"]),
             f"<small>{_esc((r['output'] or r['error'] or '')[:160])}</small>",
             f"<small>{_esc(r['ts'])}</small>")
            for r in jobs.recent_runs(conn, limit=100)]
    return _page("runs", _table(
        ("name", "status", "attempt", "exit", "tier", "output/error", "at"), rows))


def render_alerts(conn):
    rows = [(_esc(a["name"]), _esc(a["kind"]), _esc(a["title"]),
             f"<small>{_esc(a['message'][:160])}</small>", f"<small>{_esc(a['ts'])}</small>")
            for a in db.recent_alerts(conn, limit=100)]
    return _page("alerts", _table(("watcher", "kind", "title", "message", "at"), rows))


def render_audit(conn):
    rows = [(_esc(a["ts"]), _esc(a["source"]), _esc(a["entity"]), _esc(a["entity_id"]),
             _esc(a["action"]), f"<small>{_esc((a['detail'] or '')[:160])}</small>")
            for a in db.recent_audit(conn, limit=100)]
    return _page("audit", _table(("at", "source", "entity", "id", "action", "detail"), rows))


def ingest_webhook(conn, name, secret, body):
    w = db.get_watcher(conn, name)
    if w is None or w["kind"] != "webhook":
        return 404, "no such hook"
    if not secret or not hmac.compare_digest(secret, w["webhook_secret"] or ""):
        return 403, "bad secret"
    value = body
    if w["extract"]:
        try:
            value = _as_text(_dot_path(json.loads(body), w["extract"]))
        except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
            return 400, f"extract failed: {type(e).__name__}"
    db.update_state(conn, w["id"], pushed_value=value, next_run_at=0)
    return 204, ""


def start_web_server(health, db_path, *, port, now_fn=time.time, host="0.0.0.0"):
    pages = {"/": render_index, "/runs": render_runs,
             "/alerts": render_alerts, "/audit": render_audit}

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype="text/html; charset=utf-8"):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                code, body = health.payload(now_fn())
                self._send(code, json.dumps(body), "application/json")
                return
            fn = pages.get("/" + self.path.strip("/") if self.path != "/" else "/")
            if fn is None:
                self.send_error(404)
                return
            conn = db.connect_ro(db_path)
            try:
                body = fn(conn, now_fn()) if fn is render_index else fn(conn)
            finally:
                conn.close()
            self._send(200, body)

        def do_POST(self):
            parts = self.path.strip("/").split("/")
            if len(parts) != 3 or parts[0] != "hook":
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                self.send_error(413)
                return
            body = self.rfile.read(length).decode(errors="replace")
            conn = db.connect(db_path)
            try:
                code, detail = ingest_webhook(conn, parts[1], parts[2], body)
            finally:
                conn.close()
            if code == 204:
                self.send_response(204)
                self.end_headers()
            else:
                self.send_error(code, detail)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
```

Then in `src/fleet/worker.py` `_main`, replace the `start_health_server(...)` line with:

```python
    from fleet.webui import start_web_server
    start_web_server(health, os.environ["FLEET_DB"],
                     port=int(os.environ.get("FLEET_HEALTH_PORT", "8686")))
```

(import at top of `_main` to avoid a module-level cycle: webui imports jobrunner, worker imports webui only here). `start_health_server` and its tests remain.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_webui.py -v` — expected: 6 passed.

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 130 passed.

```bash
git add src/fleet/webui.py src/fleet/worker.py tests/test_webui.py
git commit -m "fleet: read-only dashboard, webhook ingress, single web server on the health port"
```

---

### Task 8: `fleetctl` — the third door

**Files:**
- Create: `src/fleet/fleetctl.py`
- Modify: `pyproject.toml` (`[project.scripts]` gains `fleetctl = "fleet.fleetctl:main"`)
- Test: `tests/test_fleetctl.py`

**Interfaces:**
- Consumes: `db.*`, `jobs.*`; env `FLEET_DB`.
- Produces: console script `fleetctl` with subcommands `watchers`, `jobs`, `runs <job> [-n N]`, `alerts [-n N]`, `set-interval <watcher> <seconds>`, `pause {watcher|job} <name>`, `resume {watcher|job} <name>`, `run-now <job>`; `main(argv=None) -> int` (0 ok, 2 unknown entity). Output: one JSON object per line (machine-readable for jobs that parse it). Every mutation writes an audit row with `source="fleetctl"`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_fleetctl.py`:

```python
import json

import pytest

from fleet import db, jobs
from fleet.fleetctl import main

NOW_SCHED = "0 9 * * *"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    p = tmp_path / "ctl.db"
    monkeypatch.setenv("FLEET_DB", str(p))
    conn = db.connect(p)
    db.create_watcher(conn, name="tickets", kind="http_text",
                      target="https://t.com", interval_seconds=600)
    jobs.create_job(conn, now=0, name="digest", kind="script", target="echo hi",
                    schedule=NOW_SCHED, tz="UTC")
    yield p
    conn.close()


def out_lines(capsys):
    return [json.loads(l) for l in capsys.readouterr().out.strip().splitlines()]


def test_listing_watchers_and_jobs(env, capsys):
    assert main(["watchers"]) == 0
    assert out_lines(capsys)[0]["name"] == "tickets"
    assert main(["jobs"]) == 0
    assert out_lines(capsys)[0]["name"] == "digest"


def test_set_interval_mutates_and_audits(env, capsys):
    assert main(["set-interval", "tickets", "30"]) == 0
    conn = db.connect(env)
    assert db.get_watcher(conn, "tickets")["interval_seconds"] == 30
    a = db.recent_audit(conn)[0]
    assert a["source"] == "fleetctl" and a["action"] == "set-interval"
    assert "600" in a["detail"] and "30" in a["detail"]
    conn.close()


def test_pause_resume_both_entities(env):
    assert main(["pause", "watcher", "tickets"]) == 0
    assert main(["pause", "job", "digest"]) == 0
    conn = db.connect(env)
    assert db.get_watcher(conn, "tickets")["enabled"] == 0
    assert jobs.get_job(conn, "digest")["enabled"] == 0
    conn.close()
    assert main(["resume", "watcher", "tickets"]) == 0
    conn = db.connect(env)
    assert db.get_watcher(conn, "tickets")["enabled"] == 1
    conn.close()


def test_run_now_wakes_job(env):
    assert main(["run-now", "digest"]) == 0
    conn = db.connect(env)
    assert jobs.list_jobs(conn)[0]["next_run_at"] == 0
    assert db.recent_audit(conn)[0]["action"] == "run-now"
    conn.close()


def test_runs_and_alerts_listing(env, capsys):
    conn = db.connect(env)
    jobs.record_run(conn, job_id=1, status="ok", output="hi")
    db.record_alert(conn, None, title="t", message="m", kind="job")
    conn.close()
    assert main(["runs", "digest"]) == 0
    assert out_lines(capsys)[0]["status"] == "ok"
    assert main(["alerts"]) == 0
    assert out_lines(capsys)[0]["title"] == "t"


def test_unknown_entity_exits_2(env, capsys):
    assert main(["run-now", "ghost"]) == 2
    assert "no job" in capsys.readouterr().err
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fleetctl.py -v`
Expected: FAIL at collection — `No module named 'fleet.fleetctl'`.

- [ ] **Step 3: Implement `src/fleet/fleetctl.py`**

```python
"""The third door: same db functions, argparse skin. Used over SSH and by jobs
themselves (the burst pattern: a 9:55 job tightens a watcher, a noon job
relaxes it). Every mutation is audited as source='fleetctl'."""

import argparse
import json
import os
import sys

from fleet import db, jobs


def _emit(rows):
    for r in rows:
        print(json.dumps(r, ensure_ascii=False, default=str))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fleetctl")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("watchers")
    sub.add_parser("jobs")
    p = sub.add_parser("runs")
    p.add_argument("job")
    p.add_argument("-n", type=int, default=20)
    p = sub.add_parser("alerts")
    p.add_argument("-n", type=int, default=20)
    p = sub.add_parser("set-interval")
    p.add_argument("watcher")
    p.add_argument("seconds", type=int)
    for name in ("pause", "resume"):
        p = sub.add_parser(name)
        p.add_argument("entity", choices=["watcher", "job"])
        p.add_argument("name")
    p = sub.add_parser("run-now")
    p.add_argument("job")
    args = ap.parse_args(argv)

    conn = db.connect(os.environ["FLEET_DB"])
    try:
        return _dispatch(conn, args)
    finally:
        conn.close()


def _fail(msg):
    print(msg, file=sys.stderr)
    return 2


def _dispatch(conn, args):
    if args.cmd == "watchers":
        _emit(db.list_watchers(conn))
    elif args.cmd == "jobs":
        _emit(jobs.list_jobs(conn))
    elif args.cmd == "alerts":
        _emit(db.recent_alerts(conn, limit=args.n))
    elif args.cmd == "runs":
        j = jobs.get_job(conn, args.job)
        if j is None:
            return _fail(f"no job {args.job!r}")
        _emit(jobs.recent_runs(conn, limit=args.n, job_id=j["id"]))
    elif args.cmd == "set-interval":
        w = db.get_watcher(conn, args.watcher)
        if w is None:
            return _fail(f"no watcher {args.watcher!r}")
        db.update_watcher(conn, w["id"], interval_seconds=args.seconds)
        db.record_audit(conn, source="fleetctl", entity="watcher", entity_id=w["id"],
                        action="set-interval",
                        detail={"old": w["interval_seconds"], "new": args.seconds})
        _emit([db.get_watcher(conn, w["id"])])
    elif args.cmd in ("pause", "resume"):
        enabled = args.cmd == "resume"
        if args.entity == "watcher":
            w = db.get_watcher(conn, args.name)
            if w is None:
                return _fail(f"no watcher {args.name!r}")
            db.set_enabled(conn, w["id"], enabled)
            db.record_audit(conn, source="fleetctl", entity="watcher",
                            entity_id=w["id"], action=args.cmd)
        else:
            j = jobs.get_job(conn, args.name)
            if j is None:
                return _fail(f"no job {args.name!r}")
            jobs.set_job_enabled(conn, j["id"], enabled)
            db.record_audit(conn, source="fleetctl", entity="job",
                            entity_id=j["id"], action=args.cmd)
    elif args.cmd == "run-now":
        j = jobs.get_job(conn, args.job)
        if j is None:
            return _fail(f"no job {args.job!r}")
        jobs.update_job_state(conn, j["id"], next_run_at=0)
        db.record_audit(conn, source="fleetctl", entity="job", entity_id=j["id"],
                        action="run-now")
    return 0
```

Add to `pyproject.toml` `[project.scripts]`:

```toml
fleetctl = "fleet.fleetctl:main"
```

Run `uv sync` so the entry point installs.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_fleetctl.py -v` — expected: 6 passed. Also sanity-check the entry point: `FLEET_DB=/tmp/ctl-smoke.db uv run fleetctl watchers` prints nothing and exits 0; then `rm -f /tmp/ctl-smoke.db*`.

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 136 passed.

```bash
git add src/fleet/fleetctl.py pyproject.toml uv.lock tests/test_fleetctl.py
git commit -m "fleet: fleetctl cli — audited mutations for ssh and self-tuning jobs"
```

---

### Task 9: MCP tools — jobs + new watcher params, audited

**Files:**
- Modify: `src/fleet/mcp_server.py`
- Test: `tests/test_mcp.py` (extend)

**Interfaces:**
- Consumes: `jobs.*` (Task 3), `db.record_audit` (Task 2).
- Produces: MCP tools `job_create, job_list, job_update, job_pause, job_resume, job_delete, job_run_now, job_history`; `watcher_create`/`watcher_update` gain `cron: str | None`, `handler_prompt: str | None`, `fallback_ok: bool = True`. Every mutation records audit `source="mcp"`. `job_create` defaults `tz` to env `FLEET_TZ` (else `UTC`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_mcp.py` (self-contained fixture; module imported as in the existing file — match its import alias, assumed `import fleet.mcp_server as m` plus `from fleet import db, jobs`):

```python
@pytest.fixture()
def jobdb(tmp_path, monkeypatch):
    p = tmp_path / "mcpjobs.db"
    monkeypatch.setenv("FLEET_DB", str(p))
    monkeypatch.setenv("FLEET_TZ", "America/New_York")
    return p


async def test_job_tools_registered():
    names = {t.name for t in await m.mcp.list_tools()}
    assert {"job_create", "job_list", "job_update", "job_pause", "job_resume",
            "job_delete", "job_run_now", "job_history"} <= names


def test_job_create_defaults_tz_and_audits(jobdb):
    j = m.job_create(name="digest", kind="script", target="echo hi",
                     schedule="0 9 * * *")
    assert j["tz"] == "America/New_York" and j["notify_policy"] == "on_failure"
    conn = db.connect(jobdb)
    a = db.recent_audit(conn)[0]
    assert (a["source"], a["entity"], a["action"]) == ("mcp", "job", "create")
    conn.close()


def test_job_lifecycle_tools(jobdb):
    m.job_create(name="digest", kind="script", target="echo hi", schedule="0 9 * * *")
    assert m.job_pause("digest")["enabled"] == 0
    assert m.job_resume("digest")["enabled"] == 1
    m.job_run_now("digest")
    conn = db.connect(jobdb)
    assert jobs.list_jobs(conn)[0]["next_run_at"] == 0
    jobs.record_run(conn, job_id=1, status="ok", output="hi")
    conn.close()
    assert m.job_history("digest")[0]["status"] == "ok"
    assert m.job_update("digest", notify_policy="always")["notify_policy"] == "always"
    assert m.job_delete("digest") == {"deleted": "digest"}


def test_watcher_create_accepts_v2_params_and_audits(jobdb):
    w = m.watcher_create(name="hook", kind="webhook", target="tv",
                         handler_prompt="judge it", fallback_ok=False)
    assert w["webhook_secret"] and w["fallback_ok"] == 0
    w2 = m.watcher_create(name="mkt", kind="script", target="echo v",
                          cron="0 9 * * 1-5")
    assert w2["cron"] == "0 9 * * 1-5"
    conn = db.connect(jobdb)
    assert len([a for a in db.recent_audit(conn) if a["action"] == "create"]) == 2
    conn.close()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_mcp.py -v`
Expected: new tests FAIL (`AttributeError: module has no attribute 'job_create'`, unexpected keyword `cron`); the 9 old tests pass.

- [ ] **Step 3: Implement in `src/fleet/mcp_server.py`**

Add imports `import time` and `from fleet import jobs`. Extend `watcher_create` signature/docstring with `cron: str | None = None, handler_prompt: str | None = None, fallback_ok: bool = True`, pass them to `db.create_watcher`, and audit after creation:

```python
        w = db.create_watcher(conn, name=name, kind=kind, target=target,
                              extract=extract, interval_seconds=interval_seconds,
                              notify_title=notify_title, cron=cron,
                              handler_prompt=handler_prompt, fallback_ok=fallback_ok)
        db.record_audit(conn, source="mcp", entity="watcher", entity_id=w["id"],
                        action="create", detail={"kind": kind, "target": target})
        return w
```

`watcher_update` gains `cron: str | None = None, handler_prompt: str | None = None` (folded into its existing `fields` dict-comprehension) and records audit `action="update", detail=fields`. `watcher_pause/resume/delete` audit their actions via `_set_enabled`/inline.

Add the job tools (same short-lived `_conn` pattern; docstrings are the MCP tool help):

```python
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
        jobs.update_job_state(conn, j["id"], next_run_at=0)
        db.record_audit(conn, source="mcp", entity="job", entity_id=j["id"], action="run-now")
        return {"queued": j["name"]}


def job_history(ident: int | str, limit: int = 20) -> list[dict]:
    """Recent runs for one job, newest first (status, output/error, tier)."""
    with _conn() as conn:
        j = _resolve_job(conn, ident)
        return jobs.recent_runs(conn, limit=limit, job_id=j["id"])
```

Extend the registration loop tuple with the eight job tools.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_mcp.py -v` — expected: 13 passed.

- [ ] **Step 5: Full suite + commit**

Run: `uv run pytest -q` — expected: 140 passed.

```bash
git add src/fleet/mcp_server.py tests/test_mcp.py
git commit -m "fleet: mcp job tools and v2 watcher params, all mutations audited"
```

---

### Task 10: Packaging + k3s deploy kit

**Files:**
- Modify: `Dockerfile`, `.env.example`, `docker-compose.yml` (comment only), `.gitignore`, `README.md`
- Create: `deploy/k8s/namespace.yaml`, `deploy/k8s/kustomization.yaml`, `deploy/k8s/config.yaml`, `deploy/k8s/secrets.example.yaml`, `deploy/k8s/fleet-core.yaml`, `deploy/k8s/services.yaml`, `deploy/k8s/ntfy.yaml`, `deploy/k8s/changedetection.yaml`, `deploy/k8s/backup-cronjob.yaml`, `deploy/k8s/build-import.sh`, `deploy/k8s/README.md`

No unit tests (config task); verification = `docker compose config -q`, `bash -n`, and a fresh `docker build`.

- [ ] **Step 1: Dockerfile — Claude Code CLI**

After the apt layer add:

```dockerfile
# Claude Code CLI for `claude` jobs and event handlers (native binary, no
# Node). Auth arrives at runtime via CLAUDE_CODE_OAUTH_TOKEN; tools stay off
# by default (see fleet/llm.py).
RUN curl -fsSL https://claude.ai/install.sh | bash
ENV PATH="/root/.local/bin:${PATH}"
```

Verify: `docker build -t fleet:latest .` then `docker run --rm fleet:latest claude --version` prints a version.

- [ ] **Step 2: `.env.example` additions + compose comment**

Append to `.env.example`:

```
# Timezone job schedules are interpreted in by default
FLEET_TZ=America/New_York

# Jobs engine
FLEET_JOBS_MAX_CONCURRENT=5
FLEET_GRACE_SECONDS=3600
FLEET_DEFER_SECONDS=3600
FLEET_CLAUDE_MAX_RUNS_PER_DAY=24

# Headless Claude auth: run `claude setup-token` on your Mac, paste the token.
CLAUDE_CODE_OAUTH_TOKEN=

# LLM fallback (OpenAI-compatible; OpenRouter free tier). Comma-separated
# model list — OpenRouter fails over across them server-side.
FLEET_FALLBACK_LLM_URL=https://openrouter.ai/api/v1
FLEET_FALLBACK_LLM_KEY=
FLEET_FALLBACK_MODELS=
```

In `docker-compose.yml`, extend the worker port comment: the `8686` port now serves the dashboard (`/`, `/runs`, `/alerts`, `/audit`), webhook ingress (`/hook/...`), and `/health`. Verify: `docker compose config -q` exits 0.

- [ ] **Step 3: k8s manifests**

`deploy/k8s/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: fleet
```

`deploy/k8s/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: fleet
resources:
  - namespace.yaml
  - config.yaml
  - fleet-core.yaml
  - services.yaml
  - ntfy.yaml
  - changedetection.yaml
  - backup-cronjob.yaml
# secrets.yaml is applied separately (copy secrets.example.yaml, fill in, never commit)
```

`deploy/k8s/config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fleet-config
data:
  FLEET_DB: /data/fleet.db
  NTFY_URL: http://ntfy
  NTFY_TOPIC: fleet-alerts
  FLEET_TZ: America/New_York          # schedules are interpreted here
  FLEET_TICK_SECONDS: "5"
  FLEET_FAIL_THRESHOLD: "3"
  FLEET_DOMAIN_MIN_GAP: "2.0"
  FLEET_MAX_CONCURRENT: "20"
  FLEET_JOBS_MAX_CONCURRENT: "5"
  FLEET_GRACE_SECONDS: "3600"
  FLEET_DEFER_SECONDS: "3600"
  FLEET_CLAUDE_MAX_RUNS_PER_DAY: "24"
  FLEET_HEALTH_PORT: "8686"
  FLEET_USER_AGENT: fleet-watcher/0.2 (personal monitoring)
  FLEET_FALLBACK_LLM_URL: https://openrouter.ai/api/v1
  FLEET_FALLBACK_MODELS: ""           # e.g. deepseek/deepseek-chat:free,qwen/qwen3-235b:free
```

`deploy/k8s/secrets.example.yaml`:

```yaml
# cp secrets.example.yaml secrets.yaml && edit && kubectl apply -f secrets.yaml
# secrets.yaml is gitignored. Values live ONLY here (k8s Secret), never in the
# image, git, or the data volume's backups.
apiVersion: v1
kind: Secret
metadata:
  name: fleet-secrets
  namespace: fleet
stringData:
  NTFY_TOKEN: ""                 # after `ntfy user add` + `ntfy token add`
  CLAUDE_CODE_OAUTH_TOKEN: ""    # from `claude setup-token` on your Mac
  FLEET_FALLBACK_LLM_KEY: ""     # OpenRouter key (top up $10 once for 1000 req/day)
```

`deploy/k8s/fleet-core.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fleet-data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 2Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-core
spec:
  replicas: 1
  strategy:
    type: Recreate            # SQLite: never let a rolling update run two writers
  selector:
    matchLabels: {app: fleet-core}
  template:
    metadata:
      labels: {app: fleet-core}
    spec:
      containers:
        - name: worker
          image: fleet:latest
          imagePullPolicy: IfNotPresent   # imported into containerd, no registry
          command: ["fleet-worker"]
          envFrom:
            - configMapRef: {name: fleet-config}
            - secretRef: {name: fleet-secrets}
          ports: [{containerPort: 8686}]
          volumeMounts: [{name: data, mountPath: /data}]
          livenessProbe:                  # /health goes 503 when the engine stalls
            httpGet: {path: /health, port: 8686}
            initialDelaySeconds: 30
            periodSeconds: 60
            failureThreshold: 5
        - name: mcp
          image: fleet:latest
          imagePullPolicy: IfNotPresent
          command: ["fleet-mcp"]
          envFrom:
            - configMapRef: {name: fleet-config}
            - secretRef: {name: fleet-secrets}
          env:
            - {name: FLEET_MCP_HOST, value: "0.0.0.0"}
          ports: [{containerPort: 8765}]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: fleet-data}
```

`deploy/k8s/services.yaml` (Tailscale operator: each Service gets its own tailnet hostname; nothing ever listens on the LAN):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fleet-dash
  annotations:
    tailscale.com/hostname: fleet-dash
spec:
  type: LoadBalancer
  loadBalancerClass: tailscale
  selector: {app: fleet-core}
  ports: [{name: http, port: 80, targetPort: 8686}]
---
apiVersion: v1
kind: Service
metadata:
  name: fleet-mcp
  annotations:
    tailscale.com/hostname: fleet-mcp
spec:
  type: LoadBalancer
  loadBalancerClass: tailscale
  selector: {app: fleet-core}
  ports: [{name: http, port: 80, targetPort: 8765}]
```

`deploy/k8s/ntfy.yaml` (ClusterIP `ntfy` for the worker + a tailnet Service for the phone):

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ntfy-data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ntfy
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector:
    matchLabels: {app: ntfy}
  template:
    metadata:
      labels: {app: ntfy}
    spec:
      containers:
        - name: ntfy
          image: binwiederhier/ntfy:latest   # pin after first pull
          args: ["serve"]
          env:
            - {name: NTFY_BASE_URL, value: "http://ntfy.YOUR-TAILNET.ts.net"}  # fill in
            - {name: NTFY_CACHE_FILE, value: /var/lib/ntfy/cache.db}
            - {name: NTFY_AUTH_FILE, value: /var/lib/ntfy/user.db}
            - {name: NTFY_AUTH_DEFAULT_ACCESS, value: deny-all}
            - {name: NTFY_ENABLE_LOGIN, value: "true"}
          ports: [{containerPort: 80}]
          volumeMounts: [{name: data, mountPath: /var/lib/ntfy}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: ntfy-data}
---
apiVersion: v1
kind: Service
metadata:
  name: ntfy            # in-cluster DNS name the worker uses (NTFY_URL)
spec:
  selector: {app: ntfy}
  ports: [{port: 80, targetPort: 80}]
---
apiVersion: v1
kind: Service
metadata:
  name: ntfy-ts         # tailnet door for the phone app
  annotations:
    tailscale.com/hostname: ntfy
spec:
  type: LoadBalancer
  loadBalancerClass: tailscale
  selector: {app: ntfy}
  ports: [{name: http, port: 80, targetPort: 80}]
```

`deploy/k8s/changedetection.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: changedetection-data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 2Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: changedetection
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector:
    matchLabels: {app: changedetection}
  template:
    metadata:
      labels: {app: changedetection}
    spec:
      containers:
        - name: changedetection
          image: dgtlmoon/changedetection.io:latest   # pin after first pull
          ports: [{containerPort: 5000}]
          volumeMounts: [{name: data, mountPath: /datastore}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: changedetection-data}
---
apiVersion: v1
kind: Service
metadata:
  name: changedetection
  annotations:
    tailscale.com/hostname: changedetection
spec:
  type: LoadBalancer
  loadBalancerClass: tailscale
  selector: {app: changedetection}
  ports: [{name: http, port: 80, targetPort: 5000}]
```

`deploy/k8s/backup-cronjob.yaml` (WAL-safe `.backup` to a hostPath the watchdog rsyncs — its `pull-backup.sh` just needs `FLEET_REPO` pointed at `/var/fleet-backups`):

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: fleet-backup
spec:
  schedule: "17 3 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: backup
              image: fleet:latest
              imagePullPolicy: IfNotPresent
              command:
                - python
                - -c
                - |
                  import datetime, glob, os, shutil, sqlite3
                  src = sqlite3.connect("/data/fleet.db")
                  day = datetime.date.today().isoformat()
                  dst = sqlite3.connect(f"/backups/fleet-{day}.db")
                  src.backup(dst)
                  dst.close()
                  import fleet.db as d
                  n = d.prune_checks(src, keep_days=30)
                  src.close()
                  shutil.copy(f"/backups/fleet-{day}.db", "/backups/fleet-latest.db")
                  for p in sorted(glob.glob("/backups/fleet-2*.db"), reverse=True)[14:]:
                      os.remove(p)
                  print(f"backup ok: fleet-{day}.db (pruned {n} old checks)")
              volumeMounts:
                - {name: data, mountPath: /data}
                - {name: backups, mountPath: /backups}
          volumes:
            - name: data
              persistentVolumeClaim: {claimName: fleet-data}
            - name: backups
              hostPath: {path: /var/fleet-backups, type: DirectoryOrCreate}
```

`deploy/k8s/build-import.sh`:

```bash
#!/bin/bash
# Build the fleet image on the VM and import it straight into k3s's
# containerd — no registry, public or otherwise, ever sees it.
set -euo pipefail
cd "$(dirname "$0")/../.."
docker build -t fleet:latest .
docker save fleet:latest | sudo k3s ctr images import -
echo "imported fleet:latest; restart to pick up: kubectl -n fleet rollout restart deploy/fleet-core"
```

Add to `.gitignore`: `deploy/k8s/secrets.yaml`

- [ ] **Step 4: `deploy/k8s/README.md`** — the runbook. Write it with these sections (full prose, not placeholders):

1. **Install k3s** on the Ubuntu VM: `curl -sfL https://get.k3s.io | sh -s - --tls-san <vm-tailscale-name>`; copy `/etc/rancher/k3s/k3s.yaml` to the Mac, change `server:` to `https://<vm-tailscale-name>:6443` — `kubectl`/`k9s` now work from anywhere on the tailnet.
2. **Tailscale operator**: create an OAuth client (scopes `devices` + tag owner) in the Tailscale admin console, install the operator per its docs (helm one-liner), confirm annotated Services acquire tailnet hostnames. Note the operator's API-server proxy as the polished alternative to step 1's kubeconfig.
3. **Deploy fleet**: `./deploy/k8s/build-import.sh`, `cp secrets.example.yaml secrets.yaml` + fill + `kubectl apply -f secrets.yaml`, then `kubectl apply -k deploy/k8s`. ntfy user/token creation via `kubectl -n fleet exec deploy/ntfy -- ntfy user add ...` mirrors the compose runbook; put the token in secrets.yaml and re-apply.
4. **Tailscale ACLs** (scope the flat tailnet down) — include this snippet, adjusted to the user's device names:

```json
"acls": [
  {"action": "accept", "src": ["your-mac"], "dst": ["fleet-mcp:80", "fleet-dash:80", "ntfy:80", "changedetection:80", "<vm>:22", "<vm>:6443"]},
  {"action": "accept", "src": ["your-phone"], "dst": ["fleet-dash:80", "ntfy:80"]}
]
```

5. **Connect Claude Code**: `claude mcp add --transport http fleet http://fleet-mcp/mcp` (tailnet MagicDNS resolves it).
6. **Deploy-day checklist** (each item is a command + expected output): `claude --version` in the worker container; **verify tool flags** — `kubectl -n fleet exec deploy/fleet-core -c worker -- claude --help | grep -E 'disallowedTools|allowedTools'` and reconcile `fleet/llm.py cli_args` if names differ; `claude -p "say ok"` with the token env; create a `* * * * *` script job with `notify_policy=always` and see the phone alert; POST to a webhook watcher twice and see the change alert; `fleetctl runs` shows history surviving `kubectl rollout restart`; watchdog on the GCP box points at `http://<vm-tailscale-name>:8686/health` — **fire its failure path once** (`kubectl -n fleet scale deploy/fleet-core --replicas=0`, wait for the urgent ntfy.sh alert, scale back).
7. **Fallback door** (operator outage): tailscaled runs on the VM itself, so SSH always works; to expose the dashboard without the operator: `kubectl -n fleet patch deploy fleet-core --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/ports/0/hostPort","value":8686}]'` — reachable at the VM's own tailscale IP.
8. **Restore drill** (box loss): new VM → k3s + operator → `build-import.sh` → apply manifests → `kubectl -n fleet cp <backup>.db fleet-core-<pod>:/data/fleet.db -c worker` → `kubectl -n fleet rollout restart deploy/fleet-core` → re-create ntfy token. Practice once.
9. **Public webhooks later**: Cloudflare Tunnel fronting only `/hook/...` — documented, not built.

- [ ] **Step 5: `README.md` update** — replace the architecture diagram box list with worker/mcp/ntfy/changedetection *plus jobs + dashboard*, note the home-lab k3s deploy as primary (`deploy/k8s/README.md`) with compose as local dev, and add a "Jobs" section mirroring the watcher-kinds table: kinds `script`/`claude`, notify policies, `handler_prompt` triage, the degradation ladder one-liner, and the burst pattern example (9:55 tighten / noon relax via `fleetctl`).

- [ ] **Step 6: Verify + commit**

Run: `bash -n deploy/k8s/build-import.sh` (exit 0), `docker compose config -q` (exit 0), `docker build -t fleet:latest .` (succeeds; `docker run --rm fleet:latest claude --version` prints a version), `uv run pytest -q` (140 passed).

```bash
git add Dockerfile .env.example docker-compose.yml .gitignore README.md deploy/k8s
git commit -m "fleet: k3s deploy kit — tailscale-operator services, backup cronjob, runbook"
```

---

### Task 11: E2E smoke on compose, cleanup, version bump

**Files:**
- Modify: `pyproject.toml` (version `0.2.0`)

- [ ] **Step 1: Full stack up**

```bash
cd ~/projects/fleet
grep -q NTFY_DEFAULT_ACCESS .env || echo "NTFY_DEFAULT_ACCESS=read-write" >> .env
docker compose up -d --build
curl -s localhost:8686/health   # {"status": "ok"or"stale" while first tick lands}
```

- [ ] **Step 2: Seed a job + webhook watcher inside the stack**

```bash
docker compose exec worker python -c "
from fleet import db, jobs
import time
c = db.connect('/data/fleet.db')
w = db.create_watcher(c, name='hook', kind='webhook', target='smoke')
print('SECRET=' + w['webhook_secret'])
jobs.create_job(c, now=time.time(), name='minutely', kind='script',
                target='date', schedule='* * * * *', tz='UTC', notify_policy='always')
"
docker compose exec worker fleetctl run-now minutely
```

- [ ] **Step 3: Verify the loop**

- `sleep 10; docker compose exec worker fleetctl runs minutely` → a run with `"status": "ok"` and a date in `output`.
- `curl -s "localhost:8666/fleet-alerts/json?poll=1"` → the job notification arrived in ntfy.
- `curl -s -X POST -d "v1" localhost:8686/hook/hook/$SECRET` → 204; repeat with `-d "v2"`; `docker compose exec worker fleetctl alerts` shows the change alert; dashboard `curl -s localhost:8686/ | grep -o hook` and `/runs`, `/audit` render.
- Wrong secret: `curl -s -o /dev/null -w '%{http_code}' -X POST -d x localhost:8686/hook/hook/nope` → `403`.
- Persistence: `docker compose restart worker`, then `fleetctl runs minutely` still lists history (named volume + migration intact).

- [ ] **Step 4: Teardown + cleanup**

```bash
docker compose down -v
rm -rf data backups .pytest_cache
git status --short   # only intended files
```

- [ ] **Step 5: Version bump, final suite, commit**

Set `version = "0.2.0"` in `pyproject.toml`. Run `uv run pytest -q` — expected: 140 passed.

```bash
git add pyproject.toml uv.lock
git commit -m "fleet: v0.2.0"
```

---

## Plan self-review notes

- **Spec coverage**: §2 engine/pools/cron/dashboard/MCP/image → Tasks 6/7/9/10; §3 schema → 2/3; §4 semantics → 1/5 (escalation refined per refinement #1); §5 kinds → 4/5; §6 handlers → 5/6; §7 webhook → 2/6/7; §8 ladder → 4/5; §9 hardening → 3 (budget), 4 (tools-off), 7 (ro dashboard), 2/8/9 (audit), 10 (secrets/ACLs/fallback door); §10 doors → 7/8/9; §11 deploy → 10; §12 migration → 2 + restore drill in 10; §13 tests → distributed + Task 11 E2E; §14 config → 6/10; §15 respected (no n8n, no local model, no Helm).
- **Accepted simplification** (spec §8 "distinct, named alert if the model list vanishes"): a vanished fallback model surfaces as `fallback HTTP 4xx` inside the failure/deferral alert body rather than a dedicated alert kind — the signal still reaches the phone, named. Revisit only if it proves confusing in practice.
- **Known judgment calls for the implementer**: cronsim's exact DST-fire instant (Task 1 Step 5 tells you how to adjust asserts without weakening invariants); Claude CLI flag names verified on deploy day (Task 10 README checklist) since only our arg assembly is unit-testable.




