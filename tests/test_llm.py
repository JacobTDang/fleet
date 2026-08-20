import httpx

from fleet.llm import Llm, cli_args


def fake_exec(code, out="", err=""):
    async def _exec(args, timeout, cwd):
        return code, out, err
    return _exec


def fallback_client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def ok_fallback(req):
    assert req.url.path.endswith("/chat/completions")
    return httpx.Response(200, json={"choices": [{"message": {"content": "judged"}}]})


def test_cli_args_tools_off_by_default():
    args = cli_args("hi", model="haiku")
    assert args[:3] == ["claude", "-p", "hi"]
    assert "--model" in args and "haiku" in args
    assert "--disallowedTools" in args           # locked down
    assert "--allowedTools" not in args


def test_cli_args_fleetctl_grant_is_narrow():
    args = cli_args("hi", model="haiku", allow_fleetctl=True)
    i = args.index("--allowedTools")
    assert "fleetctl" in args[i + 1]
    assert "--disallowedTools" not in args


def test_cli_args_allow_tools_lifts_lockdown():
    args = cli_args("hi", model="haiku", allow_tools=True)
    assert "--disallowedTools" not in args and "--allowedTools" not in args


async def test_claude_success():
    llm = Llm(exec_fn=fake_exec(0, out="the answer\n"))
    r = await llm.claude("q")
    assert r.ok and r.text == "the answer" and r.tier == "subscription"


async def test_claude_usage_limit_detected():
    llm = Llm(exec_fn=fake_exec(1, err="Claude usage limit reached, resets at 5pm"))
    r = await llm.claude("q")
    assert not r.ok and r.usage_limited


async def test_claude_other_failure_not_usage_limited():
    llm = Llm(exec_fn=fake_exec(1, err="boom"))
    r = await llm.claude("q")
    assert not r.ok and not r.usage_limited and "boom" in r.error


async def test_exec_crash_never_raises():
    async def explode(args, timeout, cwd):
        raise OSError("no such binary")
    r = await Llm(exec_fn=explode).claude("q")
    assert not r.ok and "OSError" in r.error


async def test_complete_falls_back_and_sends_models_array():
    seen = {}

    def handler(req):
        import json
        seen.update(json.loads(req.content))
        assert req.headers["Authorization"] == "Bearer k"
        return ok_fallback(req)

    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_key="k",
              fallback_models=["m1", "m2"], client=fallback_client(handler),
              exec_fn=fake_exec(1, err="usage limit reached"))
    r = await llm.complete("q")
    assert r.ok and r.tier == "fallback" and r.text == "judged"
    assert seen["models"] == ["m1", "m2"] and seen["model"] == "m1"


async def test_complete_respects_fallback_ok_false():
    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_models=["m"],
              client=fallback_client(ok_fallback), exec_fn=fake_exec(1, err="usage limit"))
    r = await llm.complete("q", fallback_ok=False)
    assert not r.ok and r.usage_limited


async def test_fallback_http_error_never_raises():
    llm = Llm(fallback_url="https://openrouter.ai/api/v1", fallback_models=["m"],
              client=fallback_client(lambda req: httpx.Response(429)),
              exec_fn=fake_exec(1, err="usage limit"))
    r = await llm.complete("q")
    assert not r.ok and "429" in r.error and r.usage_limited


async def test_no_fallback_configured():
    llm = Llm(exec_fn=fake_exec(1, err="usage limit"))
    assert not llm.has_fallback
    r = await llm.complete("q")
    assert not r.ok and r.usage_limited


async def test_unauthenticated_cli_is_a_loud_failure_not_a_judgment():
    # Verified against the real CLI in the image: it exits 1 and prints
    # "Not logged in · Please run /login" on stdout. Treating that as an answer
    # would push the login prompt to the phone as if it were a verdict.
    llm = Llm(exec_fn=fake_exec(1, out="Not logged in · Please run /login"))
    r = await llm.claude("q")
    assert not r.ok and not r.usage_limited
    assert "Not logged in" in r.error
