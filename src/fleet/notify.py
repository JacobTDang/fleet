"""ntfy client. Failures raise — a lost alert must never look like a sent one."""

import httpx


class NotifyError(Exception):
    pass


class Notifier:
    def __init__(self, base_url, topic, *, token=None, client=None):
        self._url = f"{base_url.rstrip('/')}/{topic}"
        self._token = token
        self._client = client or httpx.AsyncClient(timeout=10.0)

    async def send(self, title, message, *, priority="default", tags=None, click=None):
        headers = {"Title": title, "Priority": priority}
        if tags:
            headers["Tags"] = ",".join(tags)
        if click:
            headers["Click"] = click
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            resp = await self._client.post(self._url, content=message.encode(), headers=headers)
        except httpx.HTTPError as e:
            raise NotifyError(f"ntfy unreachable: {e}") from e
        if not resp.is_success:
            raise NotifyError(f"ntfy returned HTTP {resp.status_code}")

    async def aclose(self):
        await self._client.aclose()
