"""Domain-level backoff: the single failure that can end the whole fleet is the
house IP getting banned. Ten watchers on one site share one IP, so a 429 has to
stop the domain, not just the watcher that happened to catch it."""

import pytest

from fleet import db

NOW = 1_800_000_000.0


@pytest.fixture()
def conn(tmp_path):
    c = db.connect(tmp_path / "b.db")
    yield c
    c.close()


def test_backoff_hides_every_watcher_on_that_domain(conn):
    db.create_watcher(conn, name="a", kind="http_text", target="https://shop.com/1")
    db.create_watcher(conn, name="b", kind="http_text", target="https://shop.com/2")
    db.create_watcher(conn, name="c", kind="http_text", target="https://other.com/1")
    assert len(db.due_watchers(conn, NOW)) == 3
    db.set_domain_backoff(conn, "shop.com", until=NOW + 900, reason="HTTP 429")
    due = db.due_watchers(conn, NOW)
    assert [w["name"] for w in due] == ["c"], "backoff must cover the whole domain"
    assert len(db.due_watchers(conn, NOW + 901)) == 3, "backoff must expire"


def test_backoff_escalates_then_resets(conn):
    # repeat offences back off further, so a site that keeps saying no is left alone
    first = db.bump_domain_backoff(conn, "shop.com", now=NOW, reason="HTTP 429")
    second = db.bump_domain_backoff(conn, "shop.com", now=NOW, reason="HTTP 429")
    assert second - NOW > first - NOW
    capped = db.bump_domain_backoff(conn, "shop.com", now=NOW, reason="HTTP 429")
    for _ in range(10):
        capped = db.bump_domain_backoff(conn, "shop.com", now=NOW, reason="HTTP 429")
    assert capped - NOW <= db.MAX_BACKOFF
    db.clear_domain_backoff(conn, "shop.com")
    assert db.domain_backoff(conn, "shop.com") is None


def test_retry_after_header_is_honoured_over_the_default(conn):
    until = db.set_domain_backoff(conn, "shop.com", until=NOW + 3600, reason="Retry-After")
    assert until == NOW + 3600
    assert db.domain_backoff(conn, "shop.com")["until"] == NOW + 3600
