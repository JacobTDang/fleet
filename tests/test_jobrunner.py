import pytest

from fleet import db, jobs
from fleet.jobrunner import process_job, run_judged
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


async def _no_sleep(_):
    pass


async def run(conn, job, *, llm=None, notifier=None, **kw):
    notifier = notifier or StubNotifier()
    args = dict(llm=llm or StubLlm(), notifier=notifier, now_fn=lambda: NOW,
                sleep=_no_sleep)
    args.update(kw)
    await process_job(conn, job, **args)
    return notifier


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

    # crash-level failure (stub that raises)
    class Exploding:
        has_fallback = False

        async def complete(self, *a, **k):
            raise RuntimeError("boom")

    out3 = await run_judged(conn, Exploding(), handler_prompt="j", context="c",
                            raw_message="1 -> 2", watcher_id=w["id"])
    assert out3 == "[unjudged] 1 -> 2"


async def test_notify_failure_never_crashes_job(conn):
    j = make_job(conn, notify_policy="always")
    await run(conn, j, notifier=StubNotifier(fail=True))  # must not raise
    assert db.recent_alerts(conn)[0]["kind"] == "job"  # intent audited before send
