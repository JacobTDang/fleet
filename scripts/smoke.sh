#!/bin/bash
# End-to-end smoke test against a live compose stack — the loop the unit tests
# cannot cover: real containers, one SQLite file shared by two processes, real
# ntfy delivery, real HTTP surfaces, and survival across a restart. Both of the
# worst bugs this project has had (WAL over a bind mount, run-now scheduling at
# epoch 0) were invisible to unit tests and obvious here.
#
#   ./scripts/smoke.sh              # build, test, tear down
#   SMOKE_KEEP_STACK=1 ./scripts/smoke.sh   # leave the stack up for poking
set -euo pipefail
cd "$(dirname "$0")/.."

HEALTH_PORT="${FLEET_HEALTH_PORT:-8686}"
MCP_PORT="${FLEET_MCP_PORT:-8765}"
NTFY_PORT="${NTFY_PORT:-8666}"
HEALTH="http://localhost:${HEALTH_PORT}"
MCP="http://localhost:${MCP_PORT}/mcp"
NTFY="http://localhost:${NTFY_PORT}"
JOB="smoke-job"
HOOK="smoke-hook"
HDRS="$(mktemp)"

step() { printf '\n=== %s ===\n' "$*"; }
ok() { printf '  ok: %s\n' "$*"; }

fail() {
    printf '\nSMOKE FAILED: %s\n\n' "$*" >&2
    docker compose logs --tail=60 worker mcp ntfy >&2 || true
    exit 1
}

cleanup() {
    rm -f "$HDRS"
    if [ "${SMOKE_KEEP_STACK:-0}" = "1" ]; then
        printf '\nstack left running (SMOKE_KEEP_STACK=1): %s\n' "$HEALTH"
    else
        docker compose down -v >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# Waits for a command to succeed, or fails loudly with the stack's logs.
wait_for() {
    local desc="$1" timeout="$2"
    shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out after ${timeout}s waiting for: ${desc}"
        sleep 2
    done
    ok "$desc"
}

health_ok() { curl -fsS --max-time 5 "${HEALTH}/health" | grep -q '"status": "ok"'; }
run_succeeded() { docker compose exec -T worker fleetctl runs "$JOB" -n 1 | grep -q '"status": "ok"'; }
alert_seen() { docker compose exec -T worker fleetctl alerts -n 10 | grep -q "$1"; }
ntfy_has() { curl -fsS --max-time 5 "${NTFY}/fleet-alerts/json?poll=1" | grep -q "$1"; }
page_has() { curl -fsS --max-time 5 "${HEALTH}${1}" | grep -q "$2"; }
# A push sets next_run_at=0; the engine parks the watcher at FAR_FUTURE once it
# has consumed the value — so "parked again" means "the push was processed".
hook_parked() { docker compose exec -T worker fleetctl watchers | grep "$HOOK" | grep -q '"next_run_at": 4102444800'; }
job_running() { docker compose exec -T worker fleetctl jobs | grep 'slow-job' | grep -q '"running": 1'; }
ticks_now() { curl -fsS --max-time 5 "${HEALTH}/health" | sed -n 's/.*"ticks": \([0-9]*\).*/\1/p'; }

step "stack up"
[ -f .env ] || cp .env.example .env
grep -q '^NTFY_DEFAULT_ACCESS=' .env || echo 'NTFY_DEFAULT_ACCESS=read-write' >> .env
docker compose up -d --build worker mcp ntfy
wait_for "worker reports healthy" 90 health_ok

step "seed a job and a webhook watcher"
SEED="$(docker compose exec -T worker python - <<'PY'
import time
from fleet import db, jobs
conn = db.connect("/data/fleet.db")
w = db.create_watcher(conn, name="smoke-hook", kind="webhook", target="smoke")
jobs.create_job(conn, now=time.time(), name="smoke-job", kind="script",
                target="echo smoke-output", schedule="* * * * *", tz="UTC",
                notify_policy="always", notify_title="smoke-job ran")
print("SECRET=" + w["webhook_secret"])
PY
)"
SECRET="$(printf '%s' "$SEED" | tr -d '\r' | sed -n 's/^SECRET=//p')"
[ -n "$SECRET" ] || fail "no webhook secret came back from the seed step"
ok "job + webhook watcher created"

step "job runs and notifies"
docker compose exec -T worker fleetctl run-now "$JOB" >/dev/null
wait_for "job recorded a successful run" 60 run_succeeded
docker compose exec -T worker fleetctl runs "$JOB" -n 1 | grep -q 'smoke-output' \
    || fail "run history is missing the job's stdout"
ok "stdout captured in run history"
wait_for "notification delivered to ntfy" 60 ntfy_has "smoke-job ran"

step "webhook ingress drives the change pipeline"
code="$(curl -fsS -o /dev/null -w '%{http_code}' -X POST -d "v1" "${HEALTH}/hook/${HOOK}/${SECRET}")"
[ "$code" = "204" ] || fail "first webhook push returned HTTP ${code}, expected 204"
wait_for "baseline push consumed by the engine" 60 hook_parked
code="$(curl -fsS -o /dev/null -w '%{http_code}' -X POST -d "v2" "${HEALTH}/hook/${HOOK}/${SECRET}")"
[ "$code" = "204" ] || fail "second webhook push returned HTTP ${code}, expected 204"
wait_for "change alert fired" 60 alert_seen "v1 -> v2"

code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -d "x" "${HEALTH}/hook/${HOOK}/wrong-secret")"
[ "$code" = "403" ] || fail "wrong webhook secret returned HTTP ${code}, expected 403"
ok "wrong secret rejected with 403"

step "dashboard renders"
# needles avoid characters the pages HTML-escape (the "v1 -> v2" body renders
# as "v1 -&gt; v2", which is the escaping working, not a bug)
for page_check in "/:${HOOK}" "/runs:${JOB}" "/alerts:${HOOK} changed" "/audit:fleetctl"; do
    path="${page_check%%:*}"
    needle="${page_check#*:}"
    page_has "$path" "$needle" || fail "dashboard page ${path} is missing ${needle}"
    ok "${path} renders"
done

step "mcp server answers over http"
init='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}'
body="$(curl -sS -D "$HDRS" --max-time 10 -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' -d "$init" "$MCP")"
SID="$(tr -d '\r' < "$HDRS" | sed -n 's/^[Mm]cp-[Ss]ession-[Ii]d: //p')"
[ -n "$SID" ] || fail "mcp initialize returned no session id; body: ${body}"
curl -sS --max-time 10 -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' -H "mcp-session-id: ${SID}" \
    -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' "$MCP" >/dev/null
tools="$(curl -sS --max-time 10 -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' -H "mcp-session-id: ${SID}" \
    -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' "$MCP")"
for tool in watcher_create job_create job_history fleet_stats; do
    printf '%s' "$tools" | grep -q "$tool" || fail "mcp tools/list is missing ${tool}"
done
ok "mcp handshake + tools/list served the management tools"

step "state survives a restart"
docker compose restart worker >/dev/null
wait_for "worker healthy again" 90 health_ok
run_succeeded || fail "run history did not survive the restart"
ok "run history intact across restart"

step "recovers from a hard kill mid-run"
# A job killed mid-flight leaves running=1 in the database. Uncleared, every
# later fire is skipped as an overlap and the job silently never runs again —
# the exact shape of failure a laptop with no battery produces on a power cut.
docker compose exec -T worker python - <<'PY' >/dev/null
import time
from fleet import db, jobs
conn = db.connect("/data/fleet.db")
jobs.create_job(conn, now=time.time(), name="slow-job", kind="script",
                target="sleep 120", schedule="* * * * *", tz="UTC",
                timeout_seconds=300, notify_policy="never")
PY
docker compose exec -T worker fleetctl run-now slow-job >/dev/null
wait_for "slow job is in flight" 60 job_running

# A tick that waited on this job would stall every watcher behind it and freeze
# the heartbeat — which the watchdog and the k8s liveness probe both read as
# "the box is down". Ticks must keep advancing while the job runs.
before="$(ticks_now)"
sleep 12
after="$(ticks_now)"
[ "$after" -gt "$before" ] \
    || fail "tick loop stalled while a long job ran (watchers starve, watchdog pages)"
ok "ticks advanced ${before} -> ${after} with the long job still running"

docker compose kill worker >/dev/null
# Pause it through the mcp container (same image, same volume) while the worker
# is down: otherwise the job legitimately relaunches on the next tick and the
# assertion below could not tell recovery from a stale flag.
docker compose exec -T mcp fleetctl pause job slow-job >/dev/null
docker compose start worker >/dev/null
wait_for "worker healthy after the kill" 90 health_ok
docker compose exec -T worker fleetctl jobs | grep 'slow-job' | grep -q '"running": 0' \
    || fail "stale running flag survived the restart — the job would never run again"
docker compose exec -T worker fleetctl runs slow-job -n 5 | grep -q 'interrupted by an engine restart' \
    || fail "the interrupted run was not recorded"
ok "stale run cleared and recorded"

printf '\nSMOKE PASSED\n'
