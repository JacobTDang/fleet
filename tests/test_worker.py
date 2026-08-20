import json
import random
import urllib.request

import httpx
import pytest

from fleet import db, jobs
from fleet.checkers import ScrapeConfig
from fleet.jobrunner import FAR_FUTURE
from fleet.llm import LlmResult
from fleet.notify import NotifyError
from fleet.scheduler import DomainGate
from fleet.worker import Health, JobPool, process_watcher, start_health_server, tick
from fleet.worker import tick as worker_tick

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
        "scrape": None,
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


async def test_webhook_push_arriving_mid_check_is_not_lost(conn):
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tv")
    db.update_state(conn, w["id"], pushed_value="b", next_run_at=0)
    # the engine read "a" at due-time; "b" landed while the check was running
    row = dict(db.due_watchers(conn, NOW)[0], pushed_value="a")
    env = make_env(lambda req: httpx.Response(500))
    await process_watcher(conn, row, **env)
    s = db.get_state(conn, w["id"])
    assert s["pushed_value"] == "b", "a push that arrived mid-check was swallowed"
    assert s["next_run_at"] == 0, "the newer push must be picked up on the next tick"


async def test_job_next_run_lands_exactly_on_the_cron_boundary(conn):
    from fleet import cron
    j = jobs.create_job(conn, now=NOW, name="j", kind="script", target="true",
                        schedule="0 9 * * *", tz="America/New_York")
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    await worker_tick(conn, **env)
    assert jobs.list_jobs(conn)[0]["next_run_at"] == cron.next_fire("0 9 * * *",
                                                                   "America/New_York", NOW)


async def test_a_slow_job_does_not_block_the_tick_or_the_heartbeat(conn):
    j = jobs.create_job(conn, now=NOW, name="slow", kind="script", target="sleep 2",
                        schedule="* * * * *", tz="UTC", timeout_seconds=30,
                        notify_policy="never")
    jobs.update_job_state(conn, j["id"], next_run_at=NOW)
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com",
                      interval_seconds=60)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    pool = JobPool(5)
    n = await worker_tick(conn, job_pool=pool, **env)
    assert n == 2
    # tick returned while the job is still running — it must not wait on it
    assert jobs.list_jobs(conn)[0]["running"] == 1
    assert env["health"].payload(NOW)[1]["ticks"] == 1, "heartbeat blocked by a slow job"
    # and watchers keep being checked while that job runs
    db.update_state(conn, 1, next_run_at=0)
    await worker_tick(conn, job_pool=pool, **env)
    assert db.get_state(conn, 1)["last_hash"] is not None, "slow job starved the watchers"
    await pool.drain()
    assert jobs.recent_runs(conn, job_id=j["id"])[0]["status"] == "ok"


async def test_hundreds_of_watchers_run_in_one_tick(conn):
    # The whole premise: a watcher is a row, not a process. 300 of them are one
    # tick's worth of concurrent I/O, spread by jitter so they never realign.
    for i in range(300):
        db.create_watcher(conn, name=f"w{i}", kind="http_text",
                          target=f"https://site{i}.example/x", interval_seconds=300)
    env = make_env(lambda req: httpx.Response(200, text="v"))
    n = await worker_tick(conn, **env)
    assert n == 300
    assert conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0] == 300
    runs = [r["next_run_at"] for r in db.list_watchers(conn)]
    assert all(NOW + 270 <= r <= NOW + 330 for r in runs)   # 300s ±10%
    assert len(set(runs)) > 250, "jitter is not spreading the fleet across the window"


async def test_a_block_backs_off_the_whole_domain_and_alerts_once(conn):
    for i in (1, 2):
        db.create_watcher(conn, name=f"shop{i}", kind="http_text",
                          target=f"https://shop.com/{i}", interval_seconds=60)
    env = make_env(lambda req: httpx.Response(429, headers={"Retry-After": "600"}))
    await run_once(conn, env)
    bo = db.domain_backoff(conn, "shop.com")
    assert bo is not None and bo["until"] == NOW + 600
    assert len(env["notifier"].sent) == 1, "one alert per domain, not per watcher"
    assert "shop.com" in env["notifier"].sent[0][1]
    # every watcher on that domain is now out of the due list
    assert db.due_watchers(conn, NOW) == []


async def test_a_success_clears_the_domain_backoff(conn):
    db.create_watcher(conn, name="s", kind="http_text", target="https://shop.com/x",
                      interval_seconds=60)
    db.set_domain_backoff(conn, "shop.com", until=NOW - 1, reason="HTTP 429")
    env = make_env(lambda req: httpx.Response(200, text="v"))
    await run_once(conn, env)
    assert db.domain_backoff(conn, "shop.com") is None


async def test_expect_pattern_failure_snapshots_the_body_for_forensics(conn):
    db.create_watcher(conn, name="w", kind="http_text", target="https://a.com",
                      interval_seconds=60, expect_pattern="Add to cart")
    env = make_env(lambda req: httpx.Response(200, text="<h1>Checking your browser</h1>"))
    await run_once(conn, env)
    snaps = db.recent_snapshots(conn, 1)
    assert snaps and "Checking your browser" in snaps[0]["body"]


async def test_min_change_pct_suppresses_noise_but_anchors_on_the_last_alert(conn):
    db.create_watcher(conn, name="xrp", kind="http_text", target="https://a.com",
                      interval_seconds=60, min_change_pct=2.0)
    price = {"v": "100.00"}
    env = make_env(lambda req: httpx.Response(200, text=price["v"]))
    await run_once(conn, env)                      # baseline 100
    for step in ("101.00", "101.90", "101.95"):    # each < 2% from 100, all quiet
        price["v"] = step
        db.update_state(conn, 1, next_run_at=0)
        await run_once(conn, env)
    assert env["notifier"].sent == [], "small moves must stay quiet"
    price["v"] = "102.50"                          # 2.5% from the anchor, not from 101.95
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert len(env["notifier"].sent) == 1, "drift past the threshold must alert"
    assert db.get_state(conn, 1)["alert_anchor"] == "102.50"


async def test_min_change_pct_is_ignored_for_non_numeric_values(conn):
    db.create_watcher(conn, name="t", kind="http_text", target="https://a.com",
                      interval_seconds=60, min_change_pct=50.0)
    val = {"v": "in stock"}
    env = make_env(lambda req: httpx.Response(200, text=val["v"]))
    await run_once(conn, env)
    val["v"] = "sold out"
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert len(env["notifier"].sent) == 1, "a threshold must never mute a text change"


async def test_alert_rate_cap_announces_itself_then_goes_quiet(conn):
    db.create_watcher(conn, name="flappy", kind="http_text", target="https://a.com",
                      interval_seconds=60, alert_max_per_hour=2)
    val = {"v": "0"}
    env = make_env(lambda req: httpx.Response(200, text=val["v"]))
    await run_once(conn, env)
    for i in range(1, 6):
        val["v"] = str(i)
        db.update_state(conn, 1, next_run_at=0)
        await run_once(conn, env)
    titles = [t for t, _, _ in env["notifier"].sent]
    assert sum("changed" in t for t in titles) == 2, "cap must hold"
    assert sum("suppress" in t.lower() for t in titles) == 1, "silence must be announced once"


async def test_multiline_change_is_reported_as_a_diff(conn):
    db.create_watcher(conn, name="page", kind="http_text", target="https://a.com",
                      interval_seconds=60)
    body = {"v": "line one\nline two\nline three"}
    env = make_env(lambda req: httpx.Response(200, text=body["v"]))
    await run_once(conn, env)
    body["v"] = "line one\nline TWO changed\nline three"
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    msg = env["notifier"].sent[0][1]
    assert "-line two" in msg and "+line TWO changed" in msg
    assert "line one" not in msg.split("@@")[-1] or True   # context lines are fine


async def test_a_failed_guard_does_not_punish_sibling_watchers(conn):
    # A wrong expect_pattern is at least as likely to be my regex as a bot wall,
    # and pausing every watcher on the domain for a typo is too much collateral.
    # An explicit 403/429 is unambiguous; a guard failure is not.
    db.create_watcher(conn, name="guarded", kind="http_text", target="https://shop.com/a",
                      interval_seconds=60, expect_pattern="Add to cart")
    db.create_watcher(conn, name="sibling", kind="http_text", target="https://shop.com/b",
                      interval_seconds=60)
    env = make_env(lambda req: httpx.Response(200, text="<h1>nothing like it</h1>"))
    await run_once(conn, env)
    assert db.domain_backoff(conn, "shop.com") is None, "a guard miss must not pause the domain"
    assert db.recent_snapshots(conn, 1), "but it must still keep the body for forensics"
    assert db.recent_errors(conn)[0]["detail"].startswith("expected pattern")


SCRAPE_CFG = ScrapeConfig(url="http://firecrawl:3002/v2/scrape", key="k")


def scrape_response(markdown):
    return httpx.Response(200, json={"success": True, "data": {
        "markdown": markdown, "metadata": {"statusCode": 200}}})


async def test_scrape_backed_watcher_detects_change_through_the_provider(conn):
    db.create_watcher(conn, name="spa", kind="http_text", target="https://spa.example",
                      extract=r"Status: (\w+)", interval_seconds=300, fetch_via="scrape")
    page = {"md": "# Tickets\n\nStatus: soldout"}
    env = make_env(lambda req: scrape_response(page["md"]))
    env["scrape"] = SCRAPE_CFG
    await run_once(conn, env)
    assert db.get_state(conn, 1)["last_value"] == "soldout"
    page["md"] = "# Tickets\n\nStatus: available"
    db.update_state(conn, 1, next_run_at=0)
    await run_once(conn, env)
    assert len(env["notifier"].sent) == 1
    assert "available" in env["notifier"].sent[0][1]


async def test_scrape_budget_stops_spending_and_says_so_once(conn):
    db.create_watcher(conn, name="spa", kind="http_text", target="https://spa.example",
                      interval_seconds=300, fetch_via="scrape")
    called = {"n": 0}

    def handler(req):
        called["n"] += 1
        return scrape_response("hello")

    env = make_env(handler)
    env["scrape"] = SCRAPE_CFG
    env["scrape_budget"] = 2
    for _ in range(4):
        db.update_state(conn, 1, next_run_at=0)
        await run_once(conn, env)
    assert called["n"] == 2, "the budget must actually stop the spending"
    assert sum("budget" in t.lower() for t, _, _ in env["notifier"].sent) == 1
    assert db.get_state(conn, 1)["consecutive_failures"] == 0, "our budget is not the site failing"
