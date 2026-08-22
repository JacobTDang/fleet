#!/bin/bash
# Runs on the SECOND box, daily: pull the newest fleet backup over the tailnet,
# so losing the fleet box (or its whole cloud account) never loses the fleet.
#
# It also SAYS SO when it cannot. A backup job that fails into silence is
# indistinguishable from one that works — until the day you need it.
set -uo pipefail
FLEET_HOST="${FLEET_HOST:?set FLEET_HOST to the fleet box tailscale name or IP}"
TOPIC="${WATCHDOG_TOPIC:?set WATCHDOG_TOPIC to your secret ntfy.sh topic}"
# Directory on the fleet box that holds fleet-latest.db:
#   compose deploy : fleet/backups      (relative to the remote $HOME)
#   k3s deploy     : /var/fleet-backups (the backup CronJob's hostPath)
FLEET_BACKUP_DIR="${FLEET_BACKUP_DIR:-fleet/backups}"
MAX_AGE_HOURS="${BACKUP_MAX_AGE_HOURS:-48}"
DEST="${HOME}/fleet-backups"
mkdir -p "$DEST"

notify() { # title, priority, message
    curl -fsS --max-time 10 -H "Title: $1" -H "Priority: $2" -d "$3" \
        "https://ntfy.sh/${TOPIC}" >/dev/null || true
}

target="${DEST}/fleet-$(date +%F).db"
if ! rsync -az "${FLEET_HOST}:${FLEET_BACKUP_DIR}/fleet-latest.db" "$target"; then
    notify "fleet backup FAILED" urgent \
        "could not pull ${FLEET_BACKUP_DIR}/fleet-latest.db from ${FLEET_HOST}"
    exit 1
fi

# rsync -a preserves mtime, so this is the age of the backup itself, not of the
# copy. A successful pull of a stale file is the quieter failure: the transfer
# works while the job that writes it stopped days ago.
mtime="$(stat -c %Y "$target" 2>/dev/null || stat -f %m "$target")"
age_h=$(( ( $(date +%s) - mtime ) / 3600 ))
if [ "$age_h" -gt "$MAX_AGE_HOURS" ]; then
    notify "fleet backup STALE" urgent \
        "newest backup is ${age_h}h old (limit ${MAX_AGE_HOURS}h) — the nightly job on ${FLEET_HOST} may have stopped"
fi

ls -1t "${DEST}"/fleet-*.db | tail -n +15 | xargs -r rm --
echo "pulled: $(basename "$target") (${age_h}h old, $(ls -1 "$DEST" | wc -l) kept)"
