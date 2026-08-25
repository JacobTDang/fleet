"""The intelligence layer: headless Claude CLI first, OpenAI-compatible
fallback second (OpenRouter's `models` array does server-side failover), and
never an exception — the caller must always be able to fall through to the raw
alert (the prime invariant). Tools are OFF by default: handler prompts contain
attacker-influenceable scraped text, and a tools-off session can only produce
odd prose, never actions."""

import asyncio
from dataclasses import dataclass

import httpx

USAGE_LIMIT_MARKERS = ("usage limit", "rate limit", "limit reached", "out of usage")
# OpenRouter rejects a longer 'models' array with HTTP 400 — configure as many
# ids as you like, but only this many reach the wire, or the tier dies outright.
MAX_FAILOVER_MODELS = 3
_LOCKED_TOOLS = "Bash,Edit,Write,NotebookEdit,WebFetch,WebSearch,Task"


@dataclass
class LlmResult:
    ok: bool
    text: str | None = None
    tier: str | None = None  # 'subscription' | 'fallback'
    error: str | None = None
    usage_limited: bool = False


def cli_args(prompt, *, model, allow_fleetctl=False, allow_tools=False):
    args = ["claude", "-p", prompt, "--model", model, "--output-format", "text"]
    if allow_fleetctl:
        args += ["--allowedTools", "Bash(fleetctl *)"]
    elif not allow_tools:
        args += ["--disallowedTools", _LOCKED_TOOLS]
    return args


async def _exec_claude(args, timeout, cwd):
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=cwd)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 1, "", f"claude timed out after {timeout}s"
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


class Llm:
    def __init__(self, *, fallback_url=None, fallback_key=None, fallback_models=(),
                 client=None, exec_fn=None, cwd="/tmp"):
        self._fallback_url = fallback_url.rstrip("/") if fallback_url else None
        self._fallback_key = fallback_key
        self._fallback_models = [m for m in fallback_models if m]
        self._client = client
        self._exec = exec_fn or _exec_claude
        self._cwd = cwd

    @property
    def has_fallback(self):
        return bool(self._fallback_url and self._fallback_models)

    async def claude(self, prompt, *, model="haiku", timeout=600,
                     allow_fleetctl=False, allow_tools=False):
        try:
            code, out, err = await self._exec(
                cli_args(prompt, model=model, allow_fleetctl=allow_fleetctl,
                         allow_tools=allow_tools), timeout, self._cwd)
        except Exception as e:  # noqa: BLE001 — the ladder must never raise
            return LlmResult(ok=False, error=f"{type(e).__name__}: {e}")
        if code == 0:
            return LlmResult(ok=True, text=out.strip(), tier="subscription")
        blob = f"{out} {err}".lower()
        limited = any(m in blob for m in USAGE_LIMIT_MARKERS)
        return LlmResult(ok=False, usage_limited=limited,
                         error=(err or out).strip()[-300:] or f"claude exit {code}")

    async def fallback(self, prompt):
        if not self.has_fallback:
            return LlmResult(ok=False, error="no fallback endpoint configured")
        headers = {}
        if self._fallback_key:
            headers["Authorization"] = f"Bearer {self._fallback_key}"
        body = {"model": self._fallback_models[0],
                "models": self._fallback_models[:MAX_FAILOVER_MODELS],
                "messages": [{"role": "user", "content": prompt}]}
        client = self._client or httpx.AsyncClient(timeout=60.0)
        try:
            resp = await client.post(f"{self._fallback_url}/chat/completions",
                                     json=body, headers=headers)
            if not resp.is_success:
                return LlmResult(ok=False, error=f"fallback HTTP {resp.status_code}")
            text = resp.json()["choices"][0]["message"]["content"]
            return LlmResult(ok=True, text=text.strip(), tier="fallback")
        except Exception as e:  # noqa: BLE001 — the ladder must never raise
            return LlmResult(ok=False, error=f"fallback {type(e).__name__}: {e}")
        finally:
            if self._client is None:
                await client.aclose()

    async def complete(self, prompt, *, model="haiku", timeout=600,
                       allow_fleetctl=False, allow_tools=False, fallback_ok=True):
        r = await self.claude(prompt, model=model, timeout=timeout,
                              allow_fleetctl=allow_fleetctl, allow_tools=allow_tools)
        if r.ok or not (fallback_ok and self.has_fallback):
            return r
        fb = await self.fallback(prompt)
        if fb.ok:
            return fb
        return LlmResult(ok=False, usage_limited=r.usage_limited,
                         error=f"{r.error}; {fb.error}")
