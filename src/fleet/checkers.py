"""The three check kinds. A check never raises — every failure mode becomes a
loud CheckResult error so the worker can escalate it, because a watcher that
dies silently is worse than no watcher."""

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass

import httpx

SCRIPT_TIMEOUT = 60.0
HTTP_TIMEOUT = 30.0
BLOCK_CODES = (401, 403, 407, 429)  # "not you, not now" — back the whole domain off
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class CheckResult:
    ok: bool
    value: str | None = None
    error: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False
    blocked: bool = False           # the site refused us, not the page missing
    body: str | None = None         # raw response, kept only for forensics
    retry_after: float | None = None
    duration_ms: int | None = None
    status_code: int | None = None


def _resolve_headers(raw):
    """Watcher headers may reference environment variables as ${NAME} so API
    keys live in the process environment, never in the database or its backups.
    An unset reference is an error, not an empty header — a silently
    unauthenticated request would look like the site changed."""
    if not raw:
        return {}
    headers = json.loads(raw) if isinstance(raw, str) else dict(raw)
    out = {}
    for key, value in headers.items():
        def sub(m):
            env = os.environ.get(m.group(1))
            if env is None:
                raise KeyError(m.group(1))
            return env
        out[key] = _ENV_REF.sub(sub, str(value))
    return out


def _dot_path(data, path):
    cur = data
    for seg in path.split("."):
        if isinstance(cur, list):
            cur = cur[int(seg)]
        else:
            cur = cur[seg]
    return cur


def _as_text(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _retry_after(resp):
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)          # delta-seconds form; HTTP-date form is rare
    except ValueError:
        return None


async def _http_check(watcher, client):
    try:
        headers = _resolve_headers(watcher.get("headers"))
    except (KeyError, json.JSONDecodeError, TypeError, ValueError) as e:
        return CheckResult(ok=False, error=f"headers unusable: {type(e).__name__}: {e}")
    if watcher.get("etag"):
        headers["If-None-Match"] = watcher["etag"]
    if watcher.get("last_modified"):
        headers["If-Modified-Since"] = watcher["last_modified"]
    timeout = watcher.get("timeout_seconds") or HTTP_TIMEOUT
    started = time.monotonic()
    try:
        resp = await client.get(watcher["target"], headers=headers,
                                follow_redirects=True, timeout=timeout)
    except httpx.HTTPError as e:
        return CheckResult(ok=False, error=f"{type(e).__name__}: {e}",
                           duration_ms=int((time.monotonic() - started) * 1000))
    elapsed = int((time.monotonic() - started) * 1000)

    validators = {
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "duration_ms": elapsed,
        "status_code": resp.status_code,
    }
    if resp.status_code == 304:
        return CheckResult(ok=True, not_modified=True, **validators)
    if resp.status_code in BLOCK_CODES:
        return CheckResult(ok=False, error=f"HTTP {resp.status_code} (refused)",
                           blocked=True, retry_after=_retry_after(resp),
                           body=resp.text[:64_000], **validators)
    if resp.status_code == 503:
        # 503 with Retry-After is a throttle; without one it is just an outage
        after = _retry_after(resp)
        return CheckResult(ok=False, error="HTTP 503", blocked=after is not None,
                           retry_after=after, **validators)
    if not resp.is_success:
        return CheckResult(ok=False, error=f"HTTP {resp.status_code}", **validators)

    expect = watcher.get("expect_pattern")
    if expect and not re.search(expect, resp.text):
        # 200 OK with the wrong page: a bot wall, a soft 404, a login redirect.
        # Reporting this as a change would silently re-point the watcher at it.
        # NOT blocked: this could be a bot wall or an equally likely wrong
        # regex, and the evidence cannot tell them apart. Backing off the whole
        # domain on a guess would pause sibling watchers that are working.
        return CheckResult(ok=False, **validators, body=resp.text[:64_000],
                           error=f"expected pattern not found: {expect}")

    extract = watcher.get("extract")
    if watcher["kind"] == "http_json":
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            return CheckResult(ok=False, error=f"invalid JSON: {e}", **validators)
        if extract is None:
            return CheckResult(ok=True, value=_as_text(data), **validators)
        try:
            return CheckResult(ok=True, value=_as_text(_dot_path(data, extract)), **validators)
        except (KeyError, IndexError, TypeError, ValueError):
            return CheckResult(ok=False, error=f"extract path not found: {extract}", **validators)

    # http_text
    if extract is None:
        return CheckResult(ok=True, value=resp.text, **validators)
    m = re.search(extract, resp.text)
    if m is None:
        return CheckResult(ok=False, error=f"regex matched nothing: {extract}", **validators)
    return CheckResult(ok=True, value=m.group(1) if m.groups() else m.group(0), **validators)


async def _script_check(watcher, timeout):
    proc = await asyncio.create_subprocess_shell(
        watcher["target"],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return CheckResult(ok=False, error=f"script timed out after {timeout}s")
    if proc.returncode != 0:
        tail = stderr.decode(errors="replace").strip()[-500:]
        return CheckResult(ok=False, error=f"exit {proc.returncode}: {tail}")
    return CheckResult(ok=True, value=stdout.decode(errors="replace").strip())


async def run_check(watcher, client, *, script_timeout=SCRIPT_TIMEOUT):
    kind = watcher["kind"]
    if kind in ("http_json", "http_text"):
        return await _http_check(watcher, client)
    if kind == "script":
        return await _script_check(watcher, script_timeout)
    if kind == "webhook":
        pushed = watcher.get("pushed_value")
        if pushed is None:
            return CheckResult(ok=True, not_modified=True)
        return CheckResult(ok=True, value=pushed)
    return CheckResult(ok=False, error=f"unknown kind: {kind}")
