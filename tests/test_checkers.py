import httpx

from fleet.checkers import run_check


def make_client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def watcher(kind, target="https://api.example.com/data", extract=None, **kw):
    return {"kind": kind, "target": target, "extract": extract, **kw}


async def test_http_json_extracts_dot_path_with_list_index():
    async def handler(request):
        return httpx.Response(200, json={"items": [{"price": 42}]})

    async with make_client(handler) as client:
        r = await run_check(watcher("http_json", extract="items.0.price"), client)
    assert r.ok and r.value == "42"


async def test_http_json_whole_doc_is_key_order_stable():
    async def h1(request):
        return httpx.Response(200, content=b'{"a": 1, "b": 2}')

    async def h2(request):
        return httpx.Response(200, content=b'{"b": 2, "a": 1}')

    async with make_client(h1) as c1, make_client(h2) as c2:
        r1 = await run_check(watcher("http_json"), c1)
        r2 = await run_check(watcher("http_json"), c2)
    assert r1.ok and r1.value == r2.value


async def test_http_json_missing_path_is_loud_error():
    async def handler(request):
        return httpx.Response(200, json={"items": []})

    async with make_client(handler) as client:
        r = await run_check(watcher("http_json", extract="items.0.price"), client)
    assert not r.ok and "items.0.price" in r.error


async def test_http_json_invalid_json_is_error():
    async def handler(request):
        return httpx.Response(200, content=b"<html>not json</html>")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_json"), client)
    assert not r.ok


async def test_http_text_regex_group_extraction():
    async def handler(request):
        return httpx.Response(200, text="Price: $84.99 today only")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text", extract=r"Price: \$([\d.]+)"), client)
    assert r.ok and r.value == "84.99"


async def test_http_text_regex_no_match_is_loud_error():
    async def handler(request):
        return httpx.Response(200, text="sold out")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text", extract=r"Price: \$([\d.]+)"), client)
    assert not r.ok and "match" in r.error.lower()


async def test_http_text_without_extract_uses_whole_body():
    async def handler(request):
        return httpx.Response(200, text="whole page")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text"), client)
    assert r.ok and r.value == "whole page"


async def test_http_error_status_is_error():
    async def handler(request):
        return httpx.Response(500, text="boom")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text"), client)
    assert not r.ok and "500" in r.error


async def test_conditional_get_sends_validators_and_handles_304():
    seen = {}

    async def handler(request):
        seen["inm"] = request.headers.get("if-none-match")
        seen["ims"] = request.headers.get("if-modified-since")
        return httpx.Response(304)

    async with make_client(handler) as client:
        r = await run_check(
            watcher("http_text", etag='W/"abc"', last_modified="Mon, 01 Jan 2026 00:00:00 GMT"),
            client,
        )
    assert seen["inm"] == 'W/"abc"' and seen["ims"] == "Mon, 01 Jan 2026 00:00:00 GMT"
    assert r.ok and r.not_modified


async def test_response_validators_are_captured():
    async def handler(request):
        return httpx.Response(200, text="x", headers={
            "ETag": '"v2"', "Last-Modified": "Tue, 02 Jan 2026 00:00:00 GMT"})

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text"), client)
    assert r.etag == '"v2"' and r.last_modified == "Tue, 02 Jan 2026 00:00:00 GMT"


async def test_network_error_is_error_result_not_exception():
    async def handler(request):
        raise httpx.ConnectError("no route to host")

    async with make_client(handler) as client:
        r = await run_check(watcher("http_text"), client)
    assert not r.ok and "no route" in r.error


async def test_script_returns_stripped_stdout():
    r = await run_check(watcher("script", target="echo '  hello  '"), client=None)
    assert r.ok and r.value == "hello"


async def test_script_nonzero_exit_is_error_with_stderr():
    r = await run_check(watcher("script", target="echo oops >&2; exit 3"), client=None)
    assert not r.ok and "oops" in r.error and "3" in r.error


async def test_script_timeout_is_error():
    r = await run_check(watcher("script", target="sleep 5"), client=None, script_timeout=0.2)
    assert not r.ok and "timed out" in r.error.lower()


async def test_unknown_kind_is_error_result():
    r = await run_check(watcher("carrier_pigeon"), client=None)
    assert not r.ok and "kind" in r.error


async def test_webhook_kind_uses_pushed_value():
    w = {"kind": "webhook", "target": "hook", "pushed_value": "42"}
    r = await run_check(w, client=None)
    assert r.ok and r.value == "42"


async def test_webhook_kind_without_push_is_not_modified():
    w = {"kind": "webhook", "target": "hook", "pushed_value": None}
    r = await run_check(w, client=None)
    assert r.ok and r.not_modified
