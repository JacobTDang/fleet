import httpx
import pytest

from fleet.notify import Notifier, NotifyError


def capture_client(status=200):
    captured = {}

    async def handler(request):
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["body"] = request.content.decode()
        return httpx.Response(status)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), captured


async def test_send_posts_message_with_title_priority_and_auth():
    client, captured = capture_client()
    n = Notifier("http://ntfy", "fleet-alerts", token="tk_secret", client=client)
    await n.send("price drop", "84.99 -> 79.99", priority="high")
    assert captured["url"] == "http://ntfy/fleet-alerts"
    assert captured["headers"]["title"] == "price drop"
    assert captured["headers"]["priority"] == "high"
    assert captured["headers"]["authorization"] == "Bearer tk_secret"
    assert captured["body"] == "84.99 -> 79.99"


async def test_send_without_token_sends_no_auth_header():
    client, captured = capture_client()
    n = Notifier("http://ntfy", "t", client=client)
    await n.send("t", "m")
    assert "authorization" not in captured["headers"]


async def test_send_sets_tags_and_click_headers():
    client, captured = capture_client()
    n = Notifier("http://ntfy", "t", client=client)
    await n.send("t", "m", tags=["warning", "fleet"], click="https://ex.com/w/1")
    assert captured["headers"]["tags"] == "warning,fleet"
    assert captured["headers"]["click"] == "https://ex.com/w/1"


async def test_send_raises_loudly_on_http_failure():
    client, _ = capture_client(status=403)
    n = Notifier("http://ntfy", "t", client=client)
    with pytest.raises(NotifyError):
        await n.send("t", "m")
