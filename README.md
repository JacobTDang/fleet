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
                               └─ browserless     OPTIONAL single pod: renders JS for
                                                  fetch_via="scrape" watchers
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
| `webhook` | pushed to `/hook/<name>/<secret>` | dot path | change of pushed value |

A `cron` field on any watcher replaces its interval with exact-time scheduling
(`0 9 * * 1-5`).

### Robustness guards (web monitoring)

The two ways a web watcher lies to you are alerting on noise and reporting a
block page as a change. Each guard is optional and off by default:

| guard | what it does |
|---|---|
| `expect_pattern` | regex the page **must** contain, or the check is an error — a bot wall, soft 404, or login redirect can't masquerade as a change |
| `min_change_pct` | numeric values alert only on a move this large, measured **from the last alerted value** so slow drift still trips it |
| `alert_max_per_hour` | caps change alerts; announces the cap once, then holds — a flapping watcher can't train you to ignore ntfy |
| `headers` | JSON object; write API keys as `"${ENV_VAR}"` so the secret lives in the environment, never the database or its backups (listings hide header values) |
| `timeout_seconds` | per-watcher, for sites that are slow on purpose |

**Domain backoff is automatic and not optional.** A 429/403 (or a failed
`expect_pattern`) pauses *every watcher on that domain* — they all share one
egress IP, and answering a soft rate-limit with more requests is how a home IP
earns a ban. `Retry-After` is honored when sent; otherwise backoff doubles from
15 minutes up to 6 hours, and one alert names the domain. A success clears it.
The raw body from a refused check is kept (`watcher_snapshots`) so you can see
what the site actually served.

### Monitoring pages that a plain GET can't read

Set `fetch_via="scrape"` on an `http_text`/`http_json` watcher and the fetch
goes through a rendering / anti-bot service instead of a direct request, so
JavaScript-rendered pages and sites that refuse plain clients become
monitorable. Everything downstream is unchanged — same extraction, same
guards, same diffing.

The transport is provider-agnostic: any API that accepts JSON containing the
URL and returns the page works, configured entirely in the environment
(`FLEET_SCRAPE_URL`, `_KEY`, `_BODY`, `_PATH`). Defaults match Firecrawl v2;
`FLEET_SCRAPE_PATH=` (empty) suits a renderer that returns raw HTML such as
browserless. Two details worth knowing:

- **`maxAge: 0` is load-bearing.** Firecrawl serves cached pages by default,
  and a monitor fed from a cache looks perfectly healthy while never seeing a
  change again.
- **Markdown diffs better than HTML.** The default asks for markdown, so class
  and markup churn stops producing false changes.

Costs are metered accordingly: a 300s interval floor, `FLEET_SCRAPE_MAX_PER_DAY`
(0 = unlimited, right for self-hosted; set it for a credit-metered cloud) which
announces itself once and then stops spending, and `FLEET_SCRAPE_MAX_CONCURRENT`
(default 3) because a browser service melts long before the watcher pool does.
A provider refusing *us* is an error; the *target* refusing (visible in the
provider's reported status) is what backs the domain off.

**Wiring a self-hosted Firecrawl:** point `FLEET_SCRAPE_URL` at the **raw API**
(`:3002/v2/scrape`), not at a failure-proxy in front of it. Firecrawl answers
`success: true` even when the page came back 404 or 500 — the real status is
only in `data.metadata.statusCode` — and fleet reads that itself, so it can
tell a refusal (403/429 → back the whole domain off) from a plain 404 (error,
no backoff). A proxy that flattens both into `success: false` throws that
distinction away. Note also that extracts run against **markdown**, not HTML;
when one misses, the page is kept in `watcher_snapshots` so you can write the
selector against what actually arrived. Compose binds Firecrawl to
`127.0.0.1`, so reach it as `host.docker.internal` from Docker Desktop, or put
both stacks on one network when deploying to the laptop.

For JavaScript-rendered pages, run **one** renderer and point the scrape
transport at it — `deploy/k8s/optional/browserless.yaml` is a single pod that
idles small and only spends memory while a page is rendering. Anything
speaking the same shape works too (a Firecrawl instance elsewhere, a paid
scraping API); it is one environment variable either way.

The deliberate omission is a second monitoring tool. Page-diffing suites
(changedetection.io and friends) bring their own scheduler, their own
notification path, and their own idea of what a change is — a second brain to
keep in sync, for a job this engine already does with guards, thresholds and
diffs it can explain. What they still do better is CSS-selector extraction; if
that becomes the thing you miss, it belongs *inside* fleet as another extract
kind, not as a parallel system.

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
- **Long jobs block nothing**: jobs run as background tasks under their own
  concurrency cap, so a ten-minute `claude` job never delays a watcher check or
  freezes the health heartbeat. An engine restart mid-run is recovered on
  startup instead of leaving the job permanently "running".
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
uv sync && uv run pytest          # 197 tests, all offline
uvx ruff@0.16.3 check .           # same lint CI runs
./scripts/smoke.sh                # end-to-end against a live stack, then tears down
docker compose up -d --build      # or bring the stack up by hand on 127.0.0.1
curl -s localhost:8686/health     # worker heartbeat
```

`scripts/smoke.sh` is the real safety net: it builds the image, runs a job,
waits for the ntfy notification, drives the webhook change pipeline, renders
every dashboard page, completes an MCP handshake, and restarts the worker to
prove state survives. Both of this project's worst bugs (SQLite WAL over a
bind mount, `run-now` scheduling at epoch 0) were invisible to unit tests and
obvious here. `SMOKE_KEEP_STACK=1 ./scripts/smoke.sh` leaves the stack up.

**CI** (`.github/workflows/ci.yml`) runs the suite on Python 3.12/3.13, ruff,
shellcheck, kubeconform over the k8s manifests, a committed-credential guard,
and the same smoke test — **on pushes to main only**, so it reports on a merge
rather than gating it. Run the two commands above locally before merging.

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
