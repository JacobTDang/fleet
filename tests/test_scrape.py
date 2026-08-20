"""The scrape transport: fetch through a rendering/anti-bot service instead of
a plain GET, so JS-heavy and hostile pages become monitorable. Deliberately
provider-agnostic — Firecrawl is the default shape, but any JSON scrape API
(or a plain-HTML renderer like browserless) is a config change, not a code
change."""

import json

import httpx
import pytest

from fleet.checkers import ScrapeConfig, run_check

FIRECRAWL = ScrapeConfig(
    url="http://firecrawl:3002/v2/scrape",
    key="fc-test",
    body_template='{"url": "{{url}}", "formats": ["markdown"], "onlyMainContent": true,'
                  ' "maxAge": 0}',
    content_path="data.markdown",
)


def make_client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def watcher(**kw):
    base = {"kind": "http_text", "target": "https://spa.example/tickets",
            "extract": None, "fetch_via": "scrape"}
    base.update(kw)
    return base


def ok_response(markdown="# Tickets\n\nSold out", status=200):
    return httpx.Response(200, json={"success": True, "data": {
        "markdown": markdown, "metadata": {"statusCode": status, "title": "Tickets"}}})


async def test_scrape_sends_the_configured_body_and_extracts_the_content():
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        seen["auth"] = req.headers.get("authorization")
        seen["body"] = json.loads(req.content)
        return ok_response("# Tickets\n\nOn sale now")

    r = await run_check(watcher(extract=r"(On sale now|Sold out)"),
                        make_client(handler), scrape=FIRECRAWL)
    assert r.ok and r.value == "On sale now"
    assert seen["url"] == "http://firecrawl:3002/v2/scrape"
    assert seen["auth"] == "Bearer fc-test"
    assert seen["body"]["url"] == "https://spa.example/tickets"
    assert seen["body"]["formats"] == ["markdown"]


async def test_cache_is_disabled_by_default_or_monitoring_is_meaningless():
    # A cached page returns the same bytes forever: the watcher would look
    # healthy and never fire again.
    body = json.loads(ScrapeConfig.DEFAULT_BODY)
    assert body.get("maxAge") == 0


async def test_target_site_refusal_is_reported_as_a_block():
    # The provider call succeeded; the SITE refused. That distinction lives in
    # the provider's metadata, and only the latter should back a domain off.
    client = make_client(lambda req: ok_response("Access denied", status=403))
    r = await run_check(watcher(), client, scrape=FIRECRAWL)
    assert not r.ok and r.blocked and "403" in r.error


async def test_provider_failure_is_an_error_but_not_a_site_block():
    client = make_client(lambda req: httpx.Response(429, text="scrape quota"))
    r = await run_check(watcher(), client, scrape=FIRECRAWL)
    assert not r.ok and not r.blocked, "our provider throttling us is not the site refusing"
    assert "scrape provider" in r.error


async def test_provider_reporting_failure_in_the_payload():
    client = make_client(lambda req: httpx.Response(
        200, json={"success": False, "error": "page took too long"}))
    r = await run_check(watcher(), client, scrape=FIRECRAWL)
    assert not r.ok and "page took too long" in r.error


async def test_missing_content_path_fails_loudly():
    client = make_client(lambda req: httpx.Response(
        200, json={"success": True, "data": {"html": "<p>no markdown here</p>"}}))
    r = await run_check(watcher(), client, scrape=FIRECRAWL)
    assert not r.ok and "data.markdown" in r.error


async def test_a_renderer_returning_raw_html_needs_no_json_path():
    # e.g. browserless /content: empty content_path means "use the whole body"
    plain = ScrapeConfig(url="http://browserless:3000/content", key=None,
                         body_template='{"url": "{{url}}"}', content_path="")
    client = make_client(lambda req: httpx.Response(200, text="<h1>Rendered</h1>"))
    r = await run_check(watcher(extract=r"<h1>(.*?)</h1>"), client, scrape=plain)
    assert r.ok and r.value == "Rendered"


async def test_guards_still_apply_to_scraped_content():
    client = make_client(lambda req: ok_response("Just a cookie banner"))
    r = await run_check(watcher(expect_pattern="Add to cart"), client, scrape=FIRECRAWL)
    assert not r.ok and "expected pattern" in r.error


async def test_scrape_watcher_without_configuration_fails_loudly():
    client = make_client(lambda req: ok_response())
    r = await run_check(watcher(), client, scrape=None)
    assert not r.ok and "not configured" in r.error


async def test_json_watchers_can_scrape_too():
    client = make_client(lambda req: httpx.Response(200, json={"success": True, "data": {
        "markdown": '{"price": 42}', "metadata": {"statusCode": 200}}}))
    r = await run_check(watcher(kind="http_json", extract="price"), client, scrape=FIRECRAWL)
    assert r.ok and r.value == "42"


async def test_provider_timeout_is_reported(monkeypatch):
    def boom(req):
        raise httpx.ReadTimeout("too slow", request=req)
    r = await run_check(watcher(), make_client(boom), scrape=FIRECRAWL)
    assert not r.ok and "ReadTimeout" in r.error


def test_config_from_env_is_absent_until_a_url_is_set(monkeypatch):
    monkeypatch.delenv("FLEET_SCRAPE_URL", raising=False)
    assert ScrapeConfig.from_env() is None
    monkeypatch.setenv("FLEET_SCRAPE_URL", "https://api.firecrawl.dev/v2/scrape")
    monkeypatch.setenv("FLEET_SCRAPE_KEY", "fc-abc")
    cfg = ScrapeConfig.from_env()
    assert cfg.url.endswith("/v2/scrape") and cfg.key == "fc-abc"
    assert json.loads(cfg.body_template)["maxAge"] == 0


@pytest.mark.parametrize("path,expected", [("data.markdown", "hi"), ("", '{"data"')])
async def test_content_path_selects_json_field_or_whole_body(path, expected):
    cfg = ScrapeConfig(url="http://x/scrape", key=None,
                       body_template='{"url": "{{url}}"}', content_path=path)
    client = make_client(lambda req: httpx.Response(200, json={"data": {"markdown": "hi"}}))
    r = await run_check(watcher(), client, scrape=cfg)
    assert r.ok and r.value.startswith(expected)


@pytest.mark.parametrize("status,blocked", [(404, False), (500, False), (403, True), (429, True)])
async def test_any_error_status_from_the_target_is_an_error_not_content(status, blocked):
    # Firecrawl answers success:true even when the page came back 404 or 500 —
    # the real status is only in data.metadata.statusCode. Extracting from a
    # 404 body would report "the page changed" and then monitor the error page.
    client = make_client(lambda req: ok_response("# Not Found\n\nnothing here", status=status))
    r = await run_check(watcher(), client, scrape=FIRECRAWL)
    assert not r.ok, f"HTTP {status} content must never become a value"
    assert str(status) in r.error
    assert r.blocked is blocked, "only a refusal should back the domain off"
    assert r.body, "keep the body: it is the evidence of what the site served"


async def test_a_failed_extract_keeps_the_page_so_you_can_fix_the_selector():
    # The most common authoring mistake is an extract written against HTML when
    # the provider returns markdown. Without the page you are guessing blind.
    client = make_client(lambda req: ok_response("# Tickets\n\nStatus: AVAILABLE"))
    r = await run_check(watcher(extract=r'<span class="status">(\w+)'), client,
                        scrape=FIRECRAWL)
    assert not r.ok and "regex matched nothing" in r.error
    assert r.body and "Status: AVAILABLE" in r.body, "keep the content to debug against"
