#!/bin/bash
# Runs on the SECOND box, daily: pull the newest fleet backup over the tailnet,
# so losing the fleet box (or its whole cloud account) never loses the fleet.
set -euo pipefail
FLEET_HOST="${FLEET_HOST:?set FLEET_HOST to the fleet box tailscale name or IP}"
FLEET_REPO="${FLEET_REPO:-fleet}"   # repo path on the fleet box, relative to $HOME
DEST="${HOME}/fleet-backups"
mkdir -p "$DEST"

rsync -az "${FLEET_HOST}:${FLEET_REPO}/backups/fleet-latest.db" "${DEST}/fleet-$(date +%F).db"
ls -1t "${DEST}"/fleet-*.db | tail -n +15 | xargs -r rm --
echo "pulled: fleet-$(date +%F).db ($(ls -1 "$DEST" | wc -l) kept)"
