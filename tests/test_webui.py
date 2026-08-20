import json
import urllib.error
import urllib.request

import pytest

from fleet import db, jobs, webui
from fleet.worker import Health

NOW = 1_755_000_000.0


@pytest.fixture()
def dbpath(tmp_path):
    p = tmp_path / "ui.db"
    conn = db.connect(p)
    db.create_watcher(conn, name="xrp-price", kind="http_json",
                      target="https://api.x.com/p", extract="price")
    jobs.create_job(conn, now=NOW, name="morning-digest", kind="script",
                    target="echo hi", schedule="0 9 * * *", tz="UTC",
                    notify_policy="always")
    db.record_alert(conn, 1, title="xrp-price changed", message="1 -> 2", kind="change")
    db.record_audit(conn, source="mcp", entity="watcher", entity_id=1, action="create")
    jobs.record_run(conn, job_id=1, status="ok", output="hi", llm_tier="subscription")
    conn.close()
    return p


def test_pages_render_fixture_data(dbpath):
    ro = db.connect_ro(dbpath)
    idx = webui.render_index(ro, NOW)
    assert "xrp-price" in idx and "morning-digest" in idx
    assert "claude runs today" in idx
    assert "xrp-price changed" in webui.render_alerts(ro)
    assert "create" in webui.render_audit(ro)
    assert "morning-digest" in webui.render_runs(ro)
    ro.close()


def test_pages_escape_html(dbpath):
    conn = db.connect(dbpath)
    db.create_watcher(conn, name="x<script>alert(1)</script>", kind="script", target="echo hi")
    conn.close()
    ro = db.connect_ro(dbpath)
    assert "<script>alert" not in webui.render_index(ro, NOW)
    ro.close()


def test_ingest_happy_path_parks_value_and_wakes_watcher(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook", kind="webhook", target="tv")
    code, _ = webui.ingest_webhook(conn, "hook", w["webhook_secret"], "signal-up")
    assert code == 204
    s = db.get_state(conn, w["id"])
    assert s["pushed_value"] == "signal-up" and s["next_run_at"] == 0
    conn.close()


def test_ingest_extract_dot_path(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook2", kind="webhook", target="tv",
                          extract="alert.price")
    code, _ = webui.ingest_webhook(conn, "hook2", w["webhook_secret"],
                                   json.dumps({"alert": {"price": 3.14}}))
    assert code == 204
    assert db.get_state(conn, w["id"])["pushed_value"] == "3.14"
    code, _ = webui.ingest_webhook(conn, "hook2", w["webhook_secret"], "not json")
    assert code == 400
    conn.close()


def test_ingest_rejects_bad_secret_and_unknown(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook3", kind="webhook", target="tv")
    code, _ = webui.ingest_webhook(conn, "hook3", "wrong-secret", "v")
    assert code == 403
    assert db.get_state(conn, w["id"])["pushed_value"] is None  # zero side effects
    assert webui.ingest_webhook(conn, "nope", "s", "v")[0] == 404
    # a non-webhook watcher is not a hook
    assert webui.ingest_webhook(conn, "xrp-price", "s", "v")[0] == 404
    conn.close()


def test_server_serves_pages_health_and_hook(dbpath):
    conn = db.connect(dbpath)
    w = db.create_watcher(conn, name="hook4", kind="webhook", target="tv")
    conn.close()
    h = Health(tick_seconds=5)
    h.tick_done(NOW)
    server = webui.start_web_server(h, dbpath, port=0, now_fn=lambda: NOW + 1)
    try:
        port = server.server_address[1]
        base = f"http://127.0.0.1:{port}"
        with urllib.request.urlopen(f"{base}/health") as r:
            assert r.status == 200 and json.loads(r.read())["status"] == "ok"
        with urllib.request.urlopen(base) as r:
            body = r.read().decode()
            assert r.status == 200 and "xrp-price" in body
        req = urllib.request.Request(f"{base}/hook/hook4/{w['webhook_secret']}",
                                     data=b"pushed", method="POST")
        with urllib.request.urlopen(req) as r:
            assert r.status == 204
        ro = db.connect_ro(dbpath)
        assert ro.execute("SELECT pushed_value FROM state WHERE watcher_id = ?",
                          (w["id"],)).fetchone()[0] == "pushed"
        ro.close()
        bad = urllib.request.Request(f"{base}/hook/hook4/wrong", data=b"x", method="POST")
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(bad)
        assert e.value.code == 403
    finally:
        server.shutdown()
