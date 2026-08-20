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
    # only reachable after a final failed attempt, so code/err are bound
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
                          message=f"usage limit hit; deferring {int(defer_seconds)}s"
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
