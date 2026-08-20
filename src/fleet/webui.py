"""Read-only dashboard + webhook ingress on one stdlib HTTP server.

Pages render from a mode=ro connection — a dashboard bug physically cannot
write. The webhook POST is the single writer and does the minimum possible:
store the pushed value, wake the watcher (next_run_at=0). The async engine
does detection/alerts/handlers on its next tick, so events survive restarts."""

import hmac
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fleet import db, jobs
from fleet.checkers import _as_text, _dot_path
from fleet.jobrunner import FAR_FUTURE

MAX_BODY = 64 * 1024

_CSS = ("body{font-family:system-ui,sans-serif;margin:2rem;background:#14161a;color:#d6d8dc}"
        "table{border-collapse:collapse;width:100%;margin:0 0 2rem}"
        "td,th{border-bottom:1px solid #2c2f36;padding:.35rem .6rem;text-align:left;"
        "font-size:.9rem}th{color:#8a8f98}h1,h2{font-weight:600}"
        ".bad{color:#ff6b6b;font-weight:600}.ok{color:#69db7c}"
        "a{color:#74c0fc;text-decoration:none}small{color:#8a8f98}")


def _page(title, body):
    nav = ("<p><a href='/'>fleet</a> · <a href='/runs'>runs</a> · "
           "<a href='/alerts'>alerts</a> · <a href='/audit'>audit</a></p>")
    return ("<!doctype html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='30'>"
            f"<title>{html.escape(title)}</title><style>{_CSS}</style></head>"
            f"<body><h1>{html.escape(title)}</h1>{nav}{body}</body></html>")


def _table(headers, rows):
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def _esc(v):
    return html.escape(str(v if v is not None else ""))


def _eta(ts, now):
    if ts is None:
        return ""
    if ts >= FAR_FUTURE:
        return "on push"
    d = int(ts - now)
    return "due" if d <= 0 else f"in {d}s"


def render_index(conn, now):
    ws = sorted(db.list_watchers(conn),
                key=lambda w: (-w["consecutive_failures"], w["name"]))
    js = sorted(jobs.list_jobs(conn),
                key=lambda j: (0 if j["last_status"] in ("fail", "budget_skipped") else 1,
                               j["name"]))
    s = db.stats(conn)
    fail_cls = "bad" if s["failing"] else "ok"
    head = (f"<p>{s['watchers']} watchers ({s['enabled']} enabled, "
            f"<span class='{fail_cls}'>{s['failing']} failing</span>) · "
            f"{s['alerts_24h']} alerts/24h · "
            f"{jobs.claude_runs_today(conn)} claude runs today</p>")
    wrows = []
    for w in ws:
        st = (f"<span class='bad'>{w['consecutive_failures']} fails</span>"
              if w["consecutive_failures"] else "<span class='ok'>ok</span>")
        sched = w["cron"] or f"{w['interval_seconds']}s"
        wrows.append((_esc(w["name"]), w["kind"], st,
                      "paused" if not w["enabled"] else _eta(w["next_run_at"], now),
                      f"<small>{_esc(sched)}</small>"))
    jrows = []
    for j in js:
        st = (f"<span class='bad'>{_esc(j['last_status'])}</span>"
              if j["last_status"] in ("fail", "budget_skipped")
              else f"<span class='ok'>{_esc(j['last_status'] or 'new')}</span>")
        jrows.append((_esc(j["name"]), j["kind"], st,
                      "paused" if not j["enabled"] else _eta(j["next_run_at"], now),
                      f"<small>{_esc(j['schedule'])} {_esc(j['tz'])}</small>"))
    body = (head + "<h2>watchers</h2>"
            + _table(("name", "kind", "status", "next", "schedule"), wrows)
            + "<h2>jobs</h2>"
            + _table(("name", "kind", "status", "next", "schedule"), jrows))
    return _page("fleet", body)


def render_runs(conn):
    rows = [(_esc(r["name"]), _esc(r["status"]), _esc(r["attempt"]),
             _esc(r["exit_code"]), _esc(r["llm_tier"]),
             f"<small>{_esc((r['output'] or r['error'] or '')[:160])}</small>",
             f"<small>{_esc(r['ts'])}</small>")
            for r in jobs.recent_runs(conn, limit=100)]
    return _page("runs", _table(
        ("name", "status", "attempt", "exit", "tier", "output/error", "at"), rows))


def render_alerts(conn):
    rows = [(_esc(a["name"]), _esc(a["kind"]), _esc(a["title"]),
             f"<small>{_esc(a['message'][:160])}</small>", f"<small>{_esc(a['ts'])}</small>")
            for a in db.recent_alerts(conn, limit=100)]
    return _page("alerts", _table(("watcher", "kind", "title", "message", "at"), rows))


def render_audit(conn):
    rows = [(_esc(a["ts"]), _esc(a["source"]), _esc(a["entity"]), _esc(a["entity_id"]),
             _esc(a["action"]), f"<small>{_esc((a['detail'] or '')[:160])}</small>")
            for a in db.recent_audit(conn, limit=100)]
    return _page("audit", _table(("at", "source", "entity", "id", "action", "detail"), rows))


def ingest_webhook(conn, name, secret, body):
    w = db.get_watcher(conn, name)
    if w is None or w["kind"] != "webhook":
        return 404, "no such hook"
    if not secret or not hmac.compare_digest(secret, w["webhook_secret"] or ""):
        return 403, "bad secret"
    value = body
    if w["extract"]:
        try:
            value = _as_text(_dot_path(json.loads(body), w["extract"]))
        except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
            return 400, f"extract failed: {type(e).__name__}"
    db.update_state(conn, w["id"], pushed_value=value, next_run_at=0)
    return 204, ""


def start_web_server(health, db_path, *, port, now_fn=time.time, host="0.0.0.0"):
    pages = {"/": render_index, "/runs": render_runs,
             "/alerts": render_alerts, "/audit": render_audit}

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype="text/html; charset=utf-8"):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                code, body = health.payload(now_fn())
                self._send(code, json.dumps(body), "application/json")
                return
            fn = pages.get("/" + self.path.strip("/") if self.path != "/" else "/")
            if fn is None:
                self.send_error(404)
                return
            conn = db.connect_ro(db_path)
            try:
                body = fn(conn, now_fn()) if fn is render_index else fn(conn)
            finally:
                conn.close()
            self._send(200, body)

        def do_POST(self):
            parts = self.path.strip("/").split("/")
            if len(parts) != 3 or parts[0] != "hook":
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                self.send_error(413)
                return
            body = self.rfile.read(length).decode(errors="replace")
            conn = db.connect(db_path)
            try:
                code, detail = ingest_webhook(conn, parts[1], parts[2], body)
            finally:
                conn.close()
            if code == 204:
                self.send_response(204)
                self.end_headers()
            else:
                self.send_error(code, detail)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
