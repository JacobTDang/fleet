import pytest

from fleet import db, jobs

NOW = 1_755_000_000.0


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
