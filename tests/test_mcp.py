import pytest

from fleet import db
from fleet import mcp_server as m


@pytest.fixture(autouse=True)
def fleet_db(tmp_path, monkeypatch):
    path = tmp_path / "mcp.db"
    monkeypatch.setenv("FLEET_DB", str(path))
    return path


def test_create_returns_row_and_list_sees_it():
    w = m.watcher_create(name="gh", kind="http_json",
                         target="https://api.github.com/x", extract="a.b",
                         interval_seconds=120)
    assert w["id"] == 1 and w["domain"] == "api.github.com"
    rows = m.watcher_list()
    assert len(rows) == 1 and rows[0]["name"] == "gh"


def test_create_invalid_kind_fails_loudly():
    with pytest.raises(ValueError):
        m.watcher_create(name="x", kind="nope", target="https://a.com")


def test_pause_resume_by_name_and_filtered_list():
    m.watcher_create(name="w", kind="http_text", target="https://a.com")
    assert m.watcher_pause("w")["enabled"] == 0
    assert m.watcher_list(enabled_only=True) == []
    assert m.watcher_resume("w")["enabled"] == 1
    assert len(m.watcher_list(enabled_only=True)) == 1


def test_update_changes_fields():
    m.watcher_create(name="w", kind="http_text", target="https://a.com")
    out = m.watcher_update("w", interval_seconds=900, target="https://b.org/x")
    assert out["interval_seconds"] == 900 and out["domain"] == "b.org"


def test_operations_on_unknown_watcher_raise():
    with pytest.raises(ValueError):
        m.watcher_update("ghost", interval_seconds=900)
    with pytest.raises(ValueError):
        m.watcher_pause("ghost")
    with pytest.raises(ValueError):
        m.watcher_delete("ghost")


def test_delete_removes_watcher():
    m.watcher_create(name="w", kind="http_text", target="https://a.com")
    m.watcher_delete("w")
    assert m.watcher_list() == []


async def test_watcher_test_runs_check_without_recording(fleet_db):
    m.watcher_create(name="echo", kind="script", target="echo hi")
    result = await m.watcher_test("echo")
    assert result["ok"] is True and result["value"] == "hi"
    conn = db.connect(fleet_db)
    assert conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0] == 0
    conn.close()


def test_stats_and_recent_views(fleet_db):
    m.watcher_create(name="w", kind="http_text", target="https://a.com")
    conn = db.connect(fleet_db)
    db.record_check(conn, 1, "error", detail="HTTP 500")
    db.record_alert(conn, 1, title="w failing", message="x", kind="error")
    conn.close()
    st = m.fleet_stats()
    assert st["watchers"] == 1 and st["alerts_24h"] == 1
    assert m.recent_errors()[0]["detail"] == "HTTP 500"
    assert m.recent_alerts()[0]["title"] == "w failing"


async def test_all_tools_are_registered_on_the_mcp_server():
    tools = {t.name for t in await m.mcp.list_tools()}
    assert {
        "watcher_create", "watcher_list", "watcher_update", "watcher_pause",
        "watcher_resume", "watcher_delete", "watcher_test",
        "fleet_stats", "recent_alerts", "recent_errors",
    } <= tools
