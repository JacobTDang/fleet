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
SCRAPE_TIMEOUT = 120.0   # rendering a page is slow; that is the point
BLOCK_CODES = (401, 403, 407, 429)  # "not you, not now" — back the whole domain off
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class ScrapeConfig:
    """Fetch through a rendering / anti-bot service instead of a plain GET.

    Deliberately provider-agnostic: any API that takes JSON containing the URL
    and returns the page is usable, so Firecrawl (self-hosted or cloud),
    browserless, or a paid scraping API are configuration, not code. The
    default body is Firecrawl's v2 shape.

    maxAge=0 is load-bearing: Firecrawl serves cached pages by default, and a
    monitor fed from a cache would look healthy while never seeing a change.
    """

    DEFAULT_BODY = ('{"url": "{{url}}", "formats": ["markdown"],'
                    ' "onlyMainContent": true, "maxAge": 0}')
    DEFAULT_PATH = "data.markdown"

    url: str
    key: str | None = None
    body_template: str = DEFAULT_BODY
    content_path: str = DEFAULT_PATH
    timeout: float = SCRAPE_TIMEOUT

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        url = env.get("FLEET_SCRAPE_URL")
        if not url:
            return None
        return cls(
            url=url,
            key=env.get("FLEET_SCRAPE_KEY") or None,
            body_template=env.get("FLEET_SCRAPE_BODY") or cls.DEFAULT_BODY,
            content_path=env.get("FLEET_SCRAPE_PATH", cls.DEFAULT_PATH),
            timeout=float(env.get("FLEET_SCRAPE_TIMEOUT", SCRAPE_TIMEOUT)),
        )


def _fill_template(obj, url):
    """Substitute into parsed JSON, never into the raw text: a URL with a quote
    or a backslash would otherwise produce an unparseable body."""
    if isinstance(obj, str):
        return obj.replace("{{url}}", url)
    if isinstance(obj, list):
        return [_fill_template(v, url) for v in obj]
    if isinstance(obj, dict):
        return {k: _fill_template(v, url) for k, v in obj.items()}
    return obj


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

    return _extract(watcher, resp.text, validators)


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


async def _scrape_check(watcher, client, cfg):
    """Fetch via the scrape provider, then hand the returned page to exactly the
    same extraction and guards a direct fetch uses."""
    try:
        body = _fill_template(json.loads(cfg.body_template), watcher["target"])
    except (json.JSONDecodeError, TypeError) as e:
        return CheckResult(ok=False, error=f"scrape body template unusable: {e}")
    headers = {"Content-Type": "application/json"}
    if cfg.key:
        headers["Authorization"] = f"Bearer {cfg.key}"
    started = time.monotonic()
    try:
        resp = await client.post(cfg.url, json=body, headers=headers, timeout=cfg.timeout)
    except httpx.HTTPError as e:
        return CheckResult(ok=False, error=f"scrape provider {type(e).__name__}: {e}",
                           duration_ms=int((time.monotonic() - started) * 1000))
    elapsed = int((time.monotonic() - started) * 1000)

    if not resp.is_success:
        # the PROVIDER refused us (quota, auth) — that is not the target site
        # refusing, so it must not back the target domain off
        return CheckResult(ok=False, duration_ms=elapsed,
                           error=f"scrape provider HTTP {resp.status_code}")

    if cfg.content_path:
        try:
            payload = resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            return CheckResult(ok=False, error=f"scrape provider sent non-JSON: {e}",
                               duration_ms=elapsed)
        if payload.get("success") is False:
            return CheckResult(ok=False, duration_ms=elapsed,
                               error=f"scrape failed: {payload.get('error', 'unknown')}")
        site_status = None
        meta = payload.get("data", {}).get("metadata") if isinstance(payload.get("data"), dict) else None
        if isinstance(meta, dict):
            site_status = meta.get("statusCode")
        if site_status in BLOCK_CODES:
            return CheckResult(ok=False, blocked=True, duration_ms=elapsed,
                               status_code=site_status,
                               error=f"HTTP {site_status} (refused) via scrape")
        try:
            content = _as_text(_dot_path(payload, cfg.content_path))
        except (KeyError, IndexError, TypeError, ValueError):
            return CheckResult(ok=False, duration_ms=elapsed,
                               error=f"scrape response has no {cfg.content_path}")
    else:
        content = resp.text

    return _extract(watcher, content, {"duration_ms": elapsed})


def _extract(watcher, text, extras=None):
    """Turn fetched page text into a value: dot-path for JSON, regex for text,
    plus the expect_pattern guard that both kinds share."""
    extras = extras or {}
    expect = watcher.get("expect_pattern")
    if expect and not re.search(expect, text):
        return CheckResult(ok=False, body=text[:64_000], **extras,
                           error=f"expected pattern not found: {expect}")
    extract = watcher.get("extract")
    if watcher["kind"] == "http_json":
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            return CheckResult(ok=False, error=f"invalid JSON: {e}", **extras)
        if extract is None:
            return CheckResult(ok=True, value=_as_text(data), **extras)
        try:
            return CheckResult(ok=True, value=_as_text(_dot_path(data, extract)), **extras)
        except (KeyError, IndexError, TypeError, ValueError):
            return CheckResult(ok=False, error=f"extract path not found: {extract}", **extras)
    if extract is None:
        return CheckResult(ok=True, value=text, **extras)
    m = re.search(extract, text)
    if m is None:
        return CheckResult(ok=False, error=f"regex matched nothing: {extract}", **extras)
    return CheckResult(ok=True, value=m.group(1) if m.groups() else m.group(0), **extras)


async def run_check(watcher, client, *, script_timeout=SCRIPT_TIMEOUT, scrape=None):
    kind = watcher["kind"]
    if kind in ("http_json", "http_text") and watcher.get("fetch_via") == "scrape":
        if scrape is None:
            return CheckResult(ok=False, error="scrape transport not configured"
                                               " (set FLEET_SCRAPE_URL)")
        return await _scrape_check(watcher, client, scrape)
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
