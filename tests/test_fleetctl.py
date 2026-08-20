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
    return [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]


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


def test_run_now_schedules_for_now_not_epoch_zero(env):
    import time
    before = time.time()
    assert main(["run-now", "digest"]) == 0
    conn = db.connect(env)
    nr = jobs.list_jobs(conn)[0]["next_run_at"]
    # epoch 0 would look 50 years late and trip the grace window ("missed");
    # a manual fire must be scheduled for the present moment
    assert before <= nr <= time.time()
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
