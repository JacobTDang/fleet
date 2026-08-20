# fleet

A private 24/7 automation server: hundreds of watchers and cron jobs as
**rows in a database**, run by one engine, alerting a phone via ntfy, judged
by short-lived headless Claude runs, managed from Claude Code over MCP —
reachable only through a Tailscale network.

```
you (Mac/phone) ──tailscale──▶ fleet box (home-lab laptop: Proxmox VM + k3s)
                               ├─ worker          the engine: watchers + cron jobs, dashboard,
                               │                  webhook ingress, /health — one process
                               ├─ mcp             management console (Claude Code connects here)
                               ├─ ntfy            self-hosted push notifications
                               └─ changedetection CSS-selector page-diff watches (own UI)
                    watchdog box (GCP e2-micro, free, different failure domain)
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
A `webhook` kind receives pushed values (`POST /hook/<name>/<secret>`) instead
of polling; a `cron` field on any watcher replaces its interval with
exact-time scheduling ("0 9 * * 1-5").

## Jobs

Cron-scheduled tasks sharing the same engine, DB, and alerting:

| kind | target | runs |
|---|---|---|
| `script` | shell command | with retries, timeout, per-attempt history |
| `claude` | a prompt | headless Claude, budget-gated, tools off by default |

- **notify_policy**: `on_failure` (default) · `always` (output → phone) ·
  `on_output` (cron etiquette: only when stdout is non-empty) · `never`.
- **Semantics**: missed fires run late within a 1h grace window (alert+skip
  beyond it); overlapping fires skip, never stack; DST handled per-job tz.
- **Triage handlers**: any watcher or job can carry a `handler_prompt` — the
  engine wakes Claude on the event, and its one-line judgment becomes the
  notification. Every LLM failure degrades to the raw alert tagged
  `[unjudged]`; the ladder is subscription → defer (`defer_ok`) → OpenRouter
  free tier (`fallback_ok`, `FLEET_FALLBACK_MODELS`) → raw. Never silent.
- **Burst pattern**: a 9:55 job runs `fleetctl set-interval tickets 30`, a
  noon job relaxes it back — camp aggressively only when it matters.
- **Three doors, one engine**: MCP tools (write), the read-only dashboard on
  `:8686` (`/`, `/runs`, `/alerts`, `/audit`), and `fleetctl` over SSH —
  every mutation from any door lands in the audit trail.

## Local development

```bash
uv sync && uv run pytest          # 65 tests, all offline
cp .env.example .env              # add NTFY_DEFAULT_ACCESS=read-write for local smoke
docker compose up -d --build      # full stack on 127.0.0.1
curl -s localhost:8686/health     # worker heartbeat
```

## Deploy runbook

Production is **k3s on the home-lab laptop** — the full runbook (k3s install,
Tailscale operator, ACLs, deploy-day checklist, restore drill) lives in
`deploy/k8s/README.md`. The compose file above is the local dev harness.
The cloud-VM compose path (`deploy/setup.sh`, `deploy/oci-notes.md`) remains
as the fallback deployment target. Either way: the **watchdog** is a second
box in a different failure domain per `deploy/watchdog/README.md` — set it up
and actually fire its failure path once.

## Operating it

- Add/inspect/pause watchers: MCP tools (`watcher_create`, `watcher_list`,
  `watcher_test`, `fleet_stats`, `recent_alerts`, `recent_errors`, …).
- Backups: nightly cron → `backups/` (14 kept) → pulled to the watchdog box.
- Health: `curl http://<tailscale-ip>:8686/health` — `stale` = engine stopped
  ticking; the watchdog pages you either way.
- Being a polite client is load-bearing: prefer the longest interval that
  still serves the purpose, and respect target sites' terms — an IP ban
  blinds the whole fleet at once.
