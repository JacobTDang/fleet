"""The three check kinds. A check never raises — every failure mode becomes a
loud CheckResult error so the worker can escalate it, because a watcher that
dies silently is worse than no watcher."""

import asyncio
import json
import re
from dataclasses import dataclass

import httpx

SCRIPT_TIMEOUT = 60.0


@dataclass
class CheckResult:
    ok: bool
    value: str | None = None
    error: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


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


async def _http_check(watcher, client):
    headers = {}
    if watcher.get("etag"):
        headers["If-None-Match"] = watcher["etag"]
    if watcher.get("last_modified"):
        headers["If-Modified-Since"] = watcher["last_modified"]
    try:
        resp = await client.get(watcher["target"], headers=headers, follow_redirects=True)
    except httpx.HTTPError as e:
        return CheckResult(ok=False, error=f"{type(e).__name__}: {e}")

    validators = {
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
    }
    if resp.status_code == 304:
        return CheckResult(ok=True, not_modified=True, **validators)
    if not resp.is_success:
        return CheckResult(ok=False, error=f"HTTP {resp.status_code}", **validators)

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
