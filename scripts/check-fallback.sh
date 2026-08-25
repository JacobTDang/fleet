#!/bin/bash
# Verify the LLM fallback tier against the real provider — the one rung of the
# degradation ladder unit tests can only mock. Reads the key from .env and never
# prints it.
#
#   ./scripts/check-fallback.sh
#
# Expects FLEET_FALLBACK_LLM_URL / _KEY / _MODELS in .env (see .env.example).
# .env is parsed here the way docker compose parses it, NOT by sourcing it —
# values like `FLEET_USER_AGENT=fleet-watcher/0.1 (personal monitoring)` are
# valid to compose and a syntax error to the shell.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

[ -f .env ] || { echo "no .env — copy .env.example and fill in the fallback keys" >&2; exit 1; }

uv run python - <<'PY'
import asyncio
import os
import pathlib

from fleet.llm import Llm


def read_env(path=".env"):
    out = {}
    for line in pathlib.Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


env = read_env()
key = env.get("FLEET_FALLBACK_LLM_KEY", "")
models = [m.strip() for m in env.get("FLEET_FALLBACK_MODELS", "").split(",") if m.strip()]
url = env.get("FLEET_FALLBACK_LLM_URL") or None

if not key or not models:
    raise SystemExit("set FLEET_FALLBACK_LLM_KEY and FLEET_FALLBACK_MODELS in .env first")

print(f"endpoint: {url}")
print(f"models:   {models}")
print(f"key:      <{len(key)} chars, not shown>\n")

llm = Llm(fallback_url=url, fallback_key=key, fallback_models=models)


async def main():
    print("1. a real judgment through the fallback chain")
    r = await llm.fallback("Reply with exactly one word: ok")
    print(f"   ok={r.ok} tier={r.tier} text={(r.text or '')[:60]!r}")
    if r.error:
        print(f"   error={r.error}")
    if not r.ok:
        raise SystemExit("\n   FAILED — the fallback tier is not usable as configured.")

    print("\n2. a model id that does not exist must fail loudly, not silently")
    bad = Llm(fallback_url=url, fallback_key=key,
              fallback_models=["definitely/not-a-real-model:free"])
    b = await bad.fallback("hi")
    print(f"   ok={b.ok} error={(b.error or '')[:80]!r}")
    if b.ok:
        print("   WARNING: a bogus model id returned success — the provider is"
              " substituting silently, so a retired model would go unnoticed.")

    print("\n3. prime invariant: an unusable ladder still lets the raw alert through")
    # Both rungs must genuinely fail. Emptying PATH makes the claude binary
    # unfindable — a real FileNotFoundError, not a stub — and an Llm() with no
    # endpoint has no fallback. complete() must still return, never raise.
    saved, os.environ["PATH"] = os.environ.get("PATH", ""), ""
    try:
        d = await Llm().complete("judge this")
    finally:
        os.environ["PATH"] = saved
    print(f"   ok={d.ok} error={(d.error or '')[:70]!r}")
    if d.ok:
        raise SystemExit("\n   FAILED — the ladder reported success with both tiers"
                         " unusable; callers can no longer tell judged from unjudged.")
    print("   degrades to '[unjudged] <raw>' as required")
    print("\nFALLBACK TIER OK")


asyncio.run(main())
PY
