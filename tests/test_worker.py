import json
import random
import urllib.request

import httpx
import pytest

from fleet import db
from fleet.notify import NotifyError
from fleet.scheduler import DomainGate
from fleet.worker import Health, process_watcher, start_health_server, tick

NOW = 1_000_000.0


class StubNotifier:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def send(self, title, message, **kw):
        if self.fail:
            raise NotifyError("ntfy down")
        self.sent.append((title, message, kw))


@pytest.fixture()
def conn(tmp_path):
    c = db.connect(tmp_path / "w.db")
    yield c
    c.close()


def make_env(handler, fail_notify=False):
    return {
        "client": httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        "notifier": StubNotifier(fail=fail_notify),
        "gate": DomainGate(min_gap=0),
        "rng": random.Random(1),
        "fail_threshold": 3,
        "now_fn": lambda: NOW,
        "health": Health(tick_seconds=5),
    }


async def run_once(conn, env):
    due = db.due_watchers(conn, now=NOW)
    for w in due:
        await process_watcher(conn, w, **env)
    return due


async def test_first_run_sets_baseline_without_alert(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    env = make_env(lambda req: httpx.Response(200, text="v1"))
    await run_once(conn, env)
    s = db.get_state(conn, 1)
    assert s["last_hash"] is not None and s["last_value"] == "v1"
    assert env["notifier"].sent == []
    assert conn.execute("SELECT status FROM checks").fetchone()[0] == "ok"


async def test_change_fires_one_alert_and_updates_state(conn):
    db.create_watcher(conn, name="price", kind="http_text", target="https://a.com", interval_seconds=60)
    val = {"v": "100"}
    env = make_env(lambda req: httpx.Response(200, text=val["v"]))
    await run_once(conn, env)
    val["v"] = "84.99"
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert len(env["notifier"].sent) == 1
    title, message, _ = env["notifier"].sent[0]
    assert "price" in title and "100" in message and "84.99" in message
    assert db.get_state(conn, 1)["last_value"] == "84.99"
    assert db.recent_alerts(conn)[0]["kind"] == "change"
    assert conn.execute(
        "SELECT status FROM checks ORDER BY id DESC LIMIT 1").fetchone()[0] == "changed"


async def test_unchanged_value_is_quiet(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    env = make_env(lambda req: httpx.Response(200, text="same"))
    await run_once(conn, env)
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert env["notifier"].sent == []
    assert db.get_state(conn, 1)["consecutive_failures"] == 0


async def test_errors_alert_exactly_once_at_threshold(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    env = make_env(lambda req: httpx.Response(500))
    for _ in range(4):
        db.update_state(conn, 1, next_run_at=0)
        await run_once(conn, env)
    assert len(env["notifier"].sent) == 1  # fired at 3rd failure, silent at 4th
    title, message, kw = env["notifier"].sent[0]
    assert "w" in title and "3" in message and "HTTP 500" in message
    assert kw.get("priority") == "high"
    assert db.get_state(conn, 1)["consecutive_failures"] == 4
    assert db.recent_alerts(conn)[0]["kind"] == "error"


async def test_recovery_alert_after_threshold_failures(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    resp = {"status": 500}
    env = make_env(lambda req: httpx.Response(resp["status"], text="v"))
    for _ in range(3):
        db.update_state(conn, 1, next_run_at=0)
        await run_once(conn, env)
    resp["status"] = 200
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    kinds = [a["kind"] for a in db.recent_alerts(conn)]
    assert kinds == ["recovery", "error"]  # newest first
    assert db.get_state(conn, 1)["consecutive_failures"] == 0


async def test_no_recovery_alert_below_threshold(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    resp = {"status": 500}
    env = make_env(lambda req: httpx.Response(resp["status"], text="v"))
    await run_once(conn, env)
    resp["status"] = 200
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert env["notifier"].sent == []


async def test_next_run_advances_with_jitter(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=100)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    await run_once(conn, env)
    nr = db.get_state(conn, 1)["next_run_at"]
    assert NOW + 90 <= nr <= NOW + 110


async def test_304_keeps_validators_and_counts_ok(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    db.update_state(conn, 1, etag='W/"v1"', last_hash="h", last_value="v")
    env = make_env(lambda req: httpx.Response(304))
    await run_once(conn, env)
    s = db.get_state(conn, 1)
    assert s["etag"] == 'W/"v1"'
    assert conn.execute("SELECT status FROM checks").fetchone()[0] == "ok"
    assert env["notifier"].sent == []


async def test_notify_failure_does_not_crash_and_is_counted(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com", interval_seconds=60)
    val = {"v": "a"}
    env = make_env(lambda req: httpx.Response(200, text=val["v"]), fail_notify=True)
    await run_once(conn, env)
    val["v"] = "b"
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)  # must not raise
    assert db.recent_alerts(conn)[0]["kind"] == "change"  # intent still audited
    assert env["health"].payload(NOW)[1]["notify_failures"] == 1


async def test_tick_processes_only_due_watchers(conn):
    db.create_watcher(conn, name="due", kind="http_text", target="https://a.com", interval_seconds=60)
    db.create_watcher(conn, name="later", kind="http_text", target="https://b.com", interval_seconds=60)
    db.update_state(conn, 2, next_run_at=NOW + 999)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    n = await tick(conn, **env)
    assert n == 1
    assert db.get_state(conn, 1)["last_hash"] is not None
    assert db.get_state(conn, 2)["last_hash"] is None


def test_health_payload_detects_staleness():
    h = Health(tick_seconds=5)
    code, body = h.payload(NOW)
    assert code == 503 and body["status"] == "stale"  # never ticked
    h.tick_done(NOW)
    code, body = h.payload(NOW + 10)
    assert code == 200 and body["status"] == "ok"
    code, body = h.payload(NOW + 3600)
    assert code == 503 and body["status"] == "stale"


def test_health_server_serves_json():
    h = Health(tick_seconds=5)
    h.tick_done(NOW)
    server = start_health_server(h, port=0, now_fn=lambda: NOW + 1)
    try:
        port = server.server_address[1]
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as resp:
            body = json.loads(resp.read())
        assert resp.status == 200 and body["status"] == "ok"
    finally:
        server.shutdown()


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
    j = jobs.create_job(conn, now=NOW, name="j", kind="script",
                        target="echo done", schedule="* * * * *", tz="UTC",
                        notify_policy="always")
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    n = await worker_tick(conn, **env)
    assert n == 2
    assert jobs.recent_runs(conn, job_id=j["id"])[0]["status"] == "ok"
    assert any(m == "done" for _, m, _ in env["notifier"].sent)
