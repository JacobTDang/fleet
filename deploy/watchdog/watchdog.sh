#!/bin/bash
# Runs on the SECOND box (GCP e2-micro), every 5 minutes via cron.
# Alerts via ntfy.sh — deliberately NOT the fleet box's own ntfy, because the
# watchdog must not depend on the machine it is watching.
# Alert cadence: immediately on failure, then at most once per hour while down,
# plus a recovery message — so a dead box can't spam you into muting it.
set -u
FLEET_HOST="${FLEET_HOST:?set FLEET_HOST to the fleet box tailscale name or IP}"
TOPIC="${WATCHDOG_TOPIC:?set WATCHDOG_TOPIC to your secret ntfy.sh topic}"
STATE_DIR="${HOME}/.fleet-watchdog"
COOLDOWN=3600
mkdir -p "$STATE_DIR"

notify() { # title, priority, message
    curl -fsS --max-time 10 -H "Title: $1" -H "Priority: $2" -d "$3" \
        "https://ntfy.sh/${TOPIC}" >/dev/null || true
}

if curl -fsS --max-time 10 "http://${FLEET_HOST}:8686/health" | grep -q '"status": "ok"'; then
    if [ -f "$STATE_DIR/down_since" ]; then
        notify "fleet watchdog" default "fleet box recovered"
        rm -f "$STATE_DIR/down_since" "$STATE_DIR/last_alert"
    fi
    exit 0
fi

[ -f "$STATE_DIR/down_since" ] || date +%s > "$STATE_DIR/down_since"
now="$(date +%s)"
last="$(cat "$STATE_DIR/last_alert" 2>/dev/null || echo 0)"
if [ $((now - last)) -ge "$COOLDOWN" ]; then
    notify "fleet watchdog" urgent "fleet box unhealthy or unreachable (worker /health failed)"
    echo "$now" > "$STATE_DIR/last_alert"
fi
