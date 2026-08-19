# Watchdog box (GCP e2-micro)

The fleet box monitors the world; this box monitors the fleet box — from a
different vendor, different account, different failure domain. An on-box
monitor can't report its own death.

## Setup

1. Create the free e2-micro (us-west1/us-central1/us-east1), Ubuntu.
2. Install tailscale: `curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up`
3. Copy these two scripts over, `chmod +x` them.
4. Pick a secret random ntfy.sh topic (e.g. `openssl rand -hex 12`), subscribe
   to it in the ntfy phone app. The topic name is the only secret — treat it
   like a password.
5. For `pull-backup.sh`: enable Tailscale SSH on both boxes, or put an SSH key
   on the fleet box, so rsync works non-interactively.
6. Cron (`crontab -e`) — off-minutes on purpose:

```cron
*/5 * * * *  FLEET_HOST=<fleet-tailscale-name> WATCHDOG_TOPIC=<topic> $HOME/watchdog.sh
23 4 * * *   FLEET_HOST=<fleet-tailscale-name> $HOME/pull-backup.sh >> $HOME/fleet-backups/pull.log 2>&1
```

7. **Test the failure path once** (this is the whole point): `sudo docker compose stop worker`
   on the fleet box, wait ≤5 min for the urgent push, start it again, expect
   the recovery push. A watchdog whose failure path was never fired is decoration.
