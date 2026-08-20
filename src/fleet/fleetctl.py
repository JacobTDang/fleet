"""The third door: same db functions, argparse skin. Used over SSH and by jobs
themselves (the burst pattern: a 9:55 job tightens a watcher, a noon job
relaxes it). Every mutation is audited as source='fleetctl'."""

import argparse
import json
import os
import sys
import time

from fleet import db, jobs


def _emit(rows):
    for r in rows:
        print(json.dumps(r, ensure_ascii=False, default=str))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fleetctl")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("watchers")
    sub.add_parser("jobs")
    p = sub.add_parser("runs")
    p.add_argument("job")
    p.add_argument("-n", type=int, default=20)
    p = sub.add_parser("alerts")
    p.add_argument("-n", type=int, default=20)
    p = sub.add_parser("set-interval")
    p.add_argument("watcher")
    p.add_argument("seconds", type=int)
    for name in ("pause", "resume"):
        p = sub.add_parser(name)
        p.add_argument("entity", choices=["watcher", "job"])
        p.add_argument("name")
    p = sub.add_parser("run-now")
    p.add_argument("job")
    args = ap.parse_args(argv)

    conn = db.connect(os.environ["FLEET_DB"])
    try:
        return _dispatch(conn, args)
    finally:
        conn.close()


def _fail(msg):
    print(msg, file=sys.stderr)
    return 2


def _dispatch(conn, args):
    if args.cmd == "watchers":
        _emit(db.list_watchers(conn))
    elif args.cmd == "jobs":
        _emit(jobs.list_jobs(conn))
    elif args.cmd == "alerts":
        _emit(db.recent_alerts(conn, limit=args.n))
    elif args.cmd == "runs":
        j = jobs.get_job(conn, args.job)
        if j is None:
            return _fail(f"no job {args.job!r}")
        _emit(jobs.recent_runs(conn, limit=args.n, job_id=j["id"]))
    elif args.cmd == "set-interval":
        w = db.get_watcher(conn, args.watcher)
        if w is None:
            return _fail(f"no watcher {args.watcher!r}")
        db.update_watcher(conn, w["id"], interval_seconds=args.seconds)
        db.record_audit(conn, source="fleetctl", entity="watcher", entity_id=w["id"],
                        action="set-interval",
                        detail={"old": w["interval_seconds"], "new": args.seconds})
        _emit([db.get_watcher(conn, w["id"])])
    elif args.cmd in ("pause", "resume"):
        enabled = args.cmd == "resume"
        if args.entity == "watcher":
            w = db.get_watcher(conn, args.name)
            if w is None:
                return _fail(f"no watcher {args.name!r}")
            db.set_enabled(conn, w["id"], enabled)
            db.record_audit(conn, source="fleetctl", entity="watcher",
                            entity_id=w["id"], action=args.cmd)
        else:
            j = jobs.get_job(conn, args.name)
            if j is None:
                return _fail(f"no job {args.name!r}")
            jobs.set_job_enabled(conn, j["id"], enabled)
            db.record_audit(conn, source="fleetctl", entity="job",
                            entity_id=j["id"], action=args.cmd)
    elif args.cmd == "run-now":
        j = jobs.get_job(conn, args.job)
        if j is None:
            return _fail(f"no job {args.job!r}")
        # "now", not 0: epoch 0 would look decades late and trip the grace window
        jobs.update_job_state(conn, j["id"], next_run_at=time.time())
        db.record_audit(conn, source="fleetctl", entity="job", entity_id=j["id"],
                        action="run-now")
    return 0
