# fleet

A private 24/7 watcher server: hundreds of monitors/notifier bots as **rows in
a database**, run by one engine, alerting a phone via ntfy, managed from Claude
Code over MCP — reachable only through a Tailscale network.

```
you (Mac/phone) ──tailscale──▶ fleet box (Oracle A1, free)
                               ├─ worker          the engine: schedules + runs every watcher
                               ├─ mcp             management console (Claude Code connects here)
                               ├─ ntfy            self-hosted push notifications
                               └─ changedetection CSS-selector page-diff watches (own UI)
                    watchdog box (GCP e2-micro, free, different vendor)
                               └─ curls /health every 5 min; pulls nightly backups;
                                  alerts via ntfy.sh if the fleet box dies
```

Design rules the code enforces:
- **A watcher is a row, not a process** — 300 watchers is an INSERT, not 300 containers.
- **Politeness is the scale limit** — same-domain checks are serialized with a
  minimum gap, schedules carry ±10% jitter, intervals have a 30s floor.
- **Silence must be unambiguous** — errors escalate to one loud alert at 3
  consecutive failures (flap-free), recoveries announce themselves, and an
  external watchdog watches the watcher.
- **The box is disposable, the fleet is not** — everything rebuilds from this
  repo + the newest backup (see `deploy/oci-notes.md`, restore drill).

## Watcher kinds

| kind | target | extract | fires on |
|---|---|---|---|
| `http_json` | URL | dot path (`items.0.price`) | change of extracted value |
| `http_text` | URL | regex (first group) | change of match |
| `script` | shell command | — | change of stdout |

CSS-selector page diffing is delegated to the bundled changedetection.io.

## Local development

```bash
uv sync && uv run pytest          # 65 tests, all offline
cp .env.example .env              # add NTFY_DEFAULT_ACCESS=read-write for local smoke
docker compose up -d --build      # full stack on 127.0.0.1
curl -s localhost:8686/health     # worker heartbeat
```

## Deploy runbook

1. **Provision** the Oracle A1 (2 OCPU / 12 GB — *not more*, see
   `deploy/oci-notes.md`), Ubuntu, and lock down inbound per those notes.
2. On the box: clone this repo, `./deploy/setup.sh` (installs docker +
   tailscale + unattended-upgrades, binds everything to the tailscale IP,
   starts the stack, installs the backup cron).
3. **ntfy auth** (default access is deny-all):
   `sudo docker compose exec ntfy ntfy user add --role=admin <you>` then
   `... ntfy token add <you>`; put the token in `.env` as `NTFY_TOKEN`,
   `sudo docker compose up -d`. Subscribe the ntfy phone app to
   `http://<tailscale-ip>:8666/fleet-alerts` (phone must be on the tailnet).
4. **Connect Claude Code** (on the Mac):
   `claude mcp add --transport http fleet http://<tailscale-name>:8765/mcp`
   Then manage in plain language: "create a watcher for … every 15 minutes".
5. **Watchdog**: second box per `deploy/watchdog/README.md` — and actually
   fire its failure path once.
6. **Pin images**: after first pull, `sudo docker compose images` and replace
   the `:latest` tags in docker-compose.yml with the pulled versions.

## Operating it

- Add/inspect/pause watchers: MCP tools (`watcher_create`, `watcher_list`,
  `watcher_test`, `fleet_stats`, `recent_alerts`, `recent_errors`, …).
- Backups: nightly cron → `backups/` (14 kept) → pulled to the watchdog box.
- Health: `curl http://<tailscale-ip>:8686/health` — `stale` = engine stopped
  ticking; the watchdog pages you either way.
- Being a polite client is load-bearing: prefer the longest interval that
  still serves the purpose, and respect target sites' terms — an IP ban
  blinds the whole fleet at once.
