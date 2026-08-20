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
