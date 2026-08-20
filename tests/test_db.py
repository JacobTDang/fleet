import sqlite3

import pytest

from fleet import db


@pytest.fixture()
def conn(tmp_path):
    c = db.connect(tmp_path / "t.db")
    yield c
    c.close()


def test_connect_enables_wal_and_foreign_keys(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_create_watcher_derives_domain_and_seeds_state(conn):
    w = db.create_watcher(
        conn, name="gh-price", kind="http_json",
        target="https://api.github.com/repos/x", extract="items.0.price",
        interval_seconds=300,
    )
    assert w["id"] == 1
    assert w["domain"] == "api.github.com"
    assert w["enabled"] == 1
    s = db.get_state(conn, w["id"])
    assert s["next_run_at"] == 0
    assert s["consecutive_failures"] == 0
    assert s["last_hash"] is None


def test_create_script_watcher_gets_local_domain(conn):
    w = db.create_watcher(conn, name="disk", kind="script", target="df -h /", interval_seconds=600)
    assert w["domain"] == "local"


def test_create_watcher_rejects_bad_kind(conn):
    with pytest.raises(ValueError):
        db.create_watcher(conn, name="x", kind="carrier_pigeon", target="https://a.com", interval_seconds=60)


def test_create_watcher_rejects_relative_url(conn):
    with pytest.raises(ValueError):
        db.create_watcher(conn, name="x", kind="http_text", target="not-a-url", interval_seconds=60)


def test_create_watcher_rejects_sub_30s_interval(conn):
    with pytest.raises(ValueError):
        db.create_watcher(conn, name="x", kind="http_text", target="https://a.com", interval_seconds=5)


def test_watcher_names_are_unique(conn):
    db.create_watcher(conn, name="dup", kind="http_text", target="https://a.com", interval_seconds=60)
    with pytest.raises(sqlite3.IntegrityError):
        db.create_watcher(conn, name="dup", kind="http_text", target="https://b.com", interval_seconds=60)


def test_get_watcher_by_id_and_name(conn):
    w = db.create_watcher(conn, name="byname", kind="http_text", target="https://a.com", interval_seconds=60)
    assert db.get_watcher(conn, w["id"])["name"] == "byname"
    assert db.get_watcher(conn, "byname")["id"] == w["id"]
    assert db.get_watcher(conn, "nope") is None


def test_list_watchers_joins_state_and_filters_enabled(conn):
    a = db.create_watcher(conn, name="a", kind="http_text", target="https://a.com", interval_seconds=60)
    db.create_watcher(conn, name="b", kind="http_text", target="https://b.com", interval_seconds=60)
    db.set_enabled(conn, a["id"], False)
    all_rows = db.list_watchers(conn)
    assert {r["name"] for r in all_rows} == {"a", "b"}
    assert all("next_run_at" in r and "consecutive_failures" in r for r in all_rows)
    enabled = db.list_watchers(conn, enabled=True)
    assert [r["name"] for r in enabled] == ["b"]


def test_update_watcher_rederives_domain_and_rejects_unknown_fields(conn):
    w = db.create_watcher(conn, name="u", kind="http_text", target="https://a.com", interval_seconds=60)
    db.update_watcher(conn, w["id"], target="https://other.org/page", interval_seconds=120)
    got = db.get_watcher(conn, w["id"])
    assert got["domain"] == "other.org"
    assert got["interval_seconds"] == 120
    with pytest.raises(ValueError):
        db.update_watcher(conn, w["id"], enabled=0)  # enabled changes go through set_enabled


def test_delete_watcher_cascades_state_and_checks(conn):
    w = db.create_watcher(conn, name="d", kind="http_text", target="https://a.com", interval_seconds=60)
    db.record_check(conn, w["id"], "ok")
    db.delete_watcher(conn, w["id"])
    assert db.get_watcher(conn, w["id"]) is None
    assert conn.execute("SELECT COUNT(*) FROM state").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0] == 0


def test_due_watchers_respects_next_run_and_enabled(conn):
    a = db.create_watcher(conn, name="due", kind="http_text", target="https://a.com", interval_seconds=60)
    b = db.create_watcher(conn, name="future", kind="http_text", target="https://b.com", interval_seconds=60)
    c = db.create_watcher(conn, name="off", kind="http_text", target="https://c.com", interval_seconds=60)
    db.update_state(conn, b["id"], next_run_at=2_000_000_000.0)
    db.set_enabled(conn, c["id"], False)
    due = db.due_watchers(conn, now=1_000_000_000.0)
    assert [r["name"] for r in due] == ["due"]
    assert due[0]["interval_seconds"] == 60  # joined row carries watcher fields
    assert due[0]["next_run_at"] == 0  # ...and state fields


def test_update_state_rejects_unknown_fields(conn):
    w = db.create_watcher(conn, name="s", kind="http_text", target="https://a.com", interval_seconds=60)
    db.update_state(conn, w["id"], last_hash="abc", consecutive_failures=2, etag='W/"1"')
    s = db.get_state(conn, w["id"])
    assert s["last_hash"] == "abc" and s["consecutive_failures"] == 2 and s["etag"] == 'W/"1"'
    with pytest.raises(ValueError):
        db.update_state(conn, w["id"], hacked="yes")


def test_checks_and_recent_errors(conn):
    w = db.create_watcher(conn, name="c", kind="http_text", target="https://a.com", interval_seconds=60)
    db.record_check(conn, w["id"], "ok")
    db.record_check(conn, w["id"], "error", detail="HTTP 500")
    db.record_check(conn, w["id"], "error", detail="HTTP 502")
    errs = db.recent_errors(conn, limit=10)
    assert len(errs) == 2
    assert errs[0]["detail"] == "HTTP 502"  # newest first
    assert errs[0]["name"] == "c"  # joined watcher name


def test_record_check_rejects_bad_status(conn):
    w = db.create_watcher(conn, name="bad", kind="http_text", target="https://a.com", interval_seconds=60)
    with pytest.raises(sqlite3.IntegrityError):
        db.record_check(conn, w["id"], "meh")


def test_alerts_and_recent_alerts(conn):
    w = db.create_watcher(conn, name="al", kind="http_text", target="https://a.com", interval_seconds=60)
    db.record_alert(conn, w["id"], title="al changed", message="1 -> 2", kind="change")
    db.record_alert(conn, w["id"], title="al failing", message="3 errors", kind="error")
    alerts = db.recent_alerts(conn, limit=1)
    assert len(alerts) == 1
    assert alerts[0]["kind"] == "error"  # newest first


def test_stats_counts(conn):
    a = db.create_watcher(conn, name="s1", kind="http_text", target="https://a.com", interval_seconds=60)
    b = db.create_watcher(conn, name="s2", kind="http_text", target="https://b.com", interval_seconds=60)
    db.set_enabled(conn, b["id"], False)
    db.update_state(conn, a["id"], consecutive_failures=3)
    db.record_check(conn, a["id"], "error", detail="x")
    db.record_alert(conn, a["id"], title="t", message="m", kind="error")
    st = db.stats(conn)
    assert st["watchers"] == 2
    assert st["enabled"] == 1
    assert st["failing"] == 1
    assert st["alerts_24h"] == 1
    assert st["checks_24h"] == 1


def test_prune_checks_removes_only_old_rows(conn):
    w = db.create_watcher(conn, name="p", kind="http_text", target="https://a.com", interval_seconds=60)
    db.record_check(conn, w["id"], "ok")
    conn.execute(
        "INSERT INTO checks (watcher_id, ts, status) VALUES (?, datetime('now', '-40 day'), 'ok')",
        (w["id"],),
    )
    conn.commit()
    deleted = db.prune_checks(conn, keep_days=30)
    assert deleted == 1
    assert conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0] == 1


def test_webhook_watcher_gets_secret_and_domain(conn):
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tradingview")
    assert w["domain"] == "webhook"
    assert isinstance(w["webhook_secret"], str) and len(w["webhook_secret"]) >= 20


def test_create_watcher_validates_cron(conn):
    with pytest.raises(ValueError):
        db.create_watcher(conn, name="bad", kind="script", target="echo hi", cron="nope")
    w = db.create_watcher(conn, name="mkt", kind="script", target="echo hi", cron="0 9 * * 1-5")
    assert w["cron"] == "0 9 * * 1-5"


def test_connect_ro_cannot_write(tmp_path):
    path = tmp_path / "ro.db"
    db.connect(path).close()
    ro = db.connect_ro(path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO audit (source, entity, action) VALUES ('mcp','watcher','x')")
    ro.close()
