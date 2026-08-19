#!/bin/bash
# Nightly: online-safe SQLite backup into ./backups (bind-mounted into the
# worker), prune old check history, keep the newest 14 backups.
set -euo pipefail
cd "$(dirname "$0")/.."

STAMP="$(date +%F)"
docker compose exec -T worker python - <<PY
import sqlite3
src = sqlite3.connect("/data/fleet.db")
dst = sqlite3.connect("/backups/fleet-${STAMP}.db")
src.backup(dst)   # WAL-safe online backup
dst.close(); src.close()
print("backup written: fleet-${STAMP}.db")
PY

docker compose exec -T worker python - <<PY
from fleet import db
conn = db.connect("/data/fleet.db")
print("pruned old checks:", db.prune_checks(conn, keep_days=30))
PY

cp "backups/fleet-${STAMP}.db" backups/fleet-latest.db
ls -1t backups/fleet-2*.db 2>/dev/null | tail -n +15 | xargs -r rm --
echo "backups on disk: $(ls backups/fleet-2*.db 2>/dev/null | wc -l)"
