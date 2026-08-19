#!/bin/bash
# Idempotent bootstrap for the fleet VM (Ubuntu arm64/amd64 — Oracle A1 or any box).
# Run as your normal user from the repo: ./deploy/setup.sh
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
    echo "run as the normal user, not root (it sudo's where needed)" >&2
    exit 1
fi
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> base packages + automatic security updates"
sudo apt-get update -y
sudo apt-get install -y curl git unattended-upgrades
sudo systemctl enable --now unattended-upgrades

echo "==> docker"
if ! command -v docker >/dev/null 2>&1; then
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
fi

echo "==> tailscale"
if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
fi
if ! tailscale ip -4 >/dev/null 2>&1; then
    echo "==> authenticate tailscale (browser link follows)"
    sudo tailscale up
fi
TS_IP="$(tailscale ip -4 | head -1)"
echo "==> tailscale IP: ${TS_IP}"

echo "==> .env (services bind to the tailscale IP — nothing listens publicly)"
cd "$REPO_DIR"
[ -f .env ] || cp .env.example .env
if grep -q '^BIND_IP=' .env; then
    sed -i "s/^BIND_IP=.*/BIND_IP=${TS_IP}/" .env
else
    echo "BIND_IP=${TS_IP}" >> .env
fi

echo "==> build + start (sudo: docker group membership needs a re-login)"
sudo docker compose up -d --build

echo "==> nightly backup cron (03:17, off-minute on purpose)"
mkdir -p "$REPO_DIR/backups"
(crontab -l 2>/dev/null | grep -v '# fleet-backup' || true
 echo "17 3 * * * ${REPO_DIR}/deploy/backup.sh >> ${REPO_DIR}/backups/backup.log 2>&1 # fleet-backup") | crontab -

cat <<EOF

Done. Remaining manual steps (details in README.md):
  1. ntfy auth:  sudo docker compose exec ntfy ntfy user add --role=admin jacob
                 sudo docker compose exec ntfy ntfy token add jacob
                 -> put the token in .env as NTFY_TOKEN, then: sudo docker compose up -d
  2. Subscribe your phone (ntfy app) to http://${TS_IP}:8666/fleet-alerts (needs tailscale on the phone).
  3. Register the MCP server on your Mac:
     claude mcp add --transport http fleet http://${TS_IP}:8765/mcp
  4. Set up the watchdog on a second box: see deploy/watchdog/README.md
  5. Verify one live fire: curl -s http://${TS_IP}:8686/health
EOF
