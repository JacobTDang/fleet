# Fleet v2 — cron jobs, event handlers, k3s (design spec)

Date: 2026-08-20. Status: approved in brainstorming; implementation plan to follow.

## 1. Context and goals

Fleet v1 is a private 24/7 watcher server: watchers are SQLite rows run by one
asyncio engine, alerts go to a phone via self-hosted ntfy, management happens
from Claude Code over a streamable-http MCP server. v2 extends it into a full
automation server and moves production to a home-lab laptop:

- **Scheduled jobs** (cron expressions): do-stuff scripts, scheduled
  notifications, and Claude-powered jobs.
- **Timed watchers**: watchers gain an optional cron schedule.
- **Event handlers**: Claude triages watcher changes / job results server-side.
- **Webhook watchers**: push-based sources, no polling delay.
- **Read-only dashboard** + audit trail + `fleetctl` CLI.
- **Production on k3s** in an Ubuntu VM under Proxmox on the laptop
  (i9-13th-gen, 16 GB). The GCP e2-micro watchdog is unchanged. docker-compose
  remains the local dev/smoke harness on the Mac.

Principle: **jobs share fleet's plumbing, not its identity** — one box, one
DB, one engine process; jobs live in their own module (`jobs.py`) and their
own tables. Second principle: **the thing that watches must be boring and
free** — all monitoring/escalation is deterministic Python; Claude is invoked
only as short-lived headless runs, never a resident process.

## 2. Architecture — five changes to v1

1. **Worker engine**: the tick loop also asks "which jobs are due?". Same
   asyncio machinery; jobs get their own concurrency pool
   (`FLEET_JOBS_MAX_CONCURRENT`, default 5) separate from watchers
   (`FLEET_MAX_CONCURRENT`, default 20) so a slow job can never starve a
   time-sensitive watcher.
2. **Watchers gain optional `cron`**: when set, next-run comes from the cron
   expression (exact time, no jitter) instead of `interval ± 10%`. The
   per-domain politeness gate still applies to every HTTP request.
3. **Dashboard**: the worker's stdlib HTTP server (currently `/health`) grows
   server-rendered pages `/` (overview), `/runs`, `/alerts`, `/audit`, plus
   the webhook ingress `POST /hook/<name>/<secret>`. Plain HTML, meta-refresh,
   no JS, no framework. Mutations are impossible by construction (§9.3).
4. **MCP server** gains `job_create/list/update/pause/resume/delete/run_now/
   history`; `watcher_create`/`watcher_update` gain `cron`, `handler_prompt`,
   `fallback_ok` parameters.
5. **Worker image** gains the Claude Code CLI and `fleetctl`.

## 3. Data model

New tables (SQLite, same WAL/foreign-keys setup as v1):

**`jobs`** — definitions:
- `id`, `name` UNIQUE, `kind` CHECK IN (`script`, `claude`)
- `target` — shell command (script) or prompt (claude)
- `schedule` — 5-field cron expression; `tz` — IANA name, default box tz
- `enabled`, `notify_policy` CHECK IN (`on_failure`, `always`, `on_output`,
  `never`) default `on_failure`; `notify_title`
- `timeout_seconds` (default 60 script / 600 claude), `retries` (default 0;
  forced 0 for claude), `retry_delay_seconds` (default 60)
- `defer_ok` (default 0), `fallback_ok` (default 1)
- `max_runs_per_day` — required (NOT NULL) for claude kind
- `model` — claude jobs only, default `haiku`
- `allow_tools` (default 0), `allow_fleetctl` (default 0)
- `handler_prompt` — optional post-run triage prompt
- `created_at`

**`runs`** — one row per attempt (job runs AND watcher-handler runs):
- `id`, `job_id` NULL FK CASCADE, `watcher_id` NULL FK CASCADE
  (CHECK exactly one non-null)
- `scheduled_for`, `started_at`, `finished_at`
- `status` CHECK IN (`ok`, `fail`, `timeout`, `missed`, `skipped_overlap`,
  `budget_skipped`, `deferred`)
- `exit_code`, `output` (4 KB tail), `error` (tail), `attempt`, `llm_tier`
  (NULL | `subscription` | `fallback`)

**`job_state`** — moving parts: `job_id` PK FK, `next_run_at`, `running`,
`consecutive_failures`, `last_status`.

**`audit`** — unforgeable history: `id`, `ts`, `source` CHECK IN (`mcp`,
`fleetctl`, `engine`), `entity` (`watcher`|`job`), `entity_id`, `action`,
`detail` (JSON old→new). Every mutation through any door writes a row.

**`watchers`** — new nullable columns: `cron`, `handler_prompt`,
`handler_allow_fleetctl` (default 0), `fallback_ok` (default 1),
`webhook_secret`. `kind` gains `webhook`.

## 4. Scheduler semantics

- **Parsing**: `cronsim` (approved dependency — tiny, zero-dep, DST-correct).
- **Timezones**: next fire computed in the job's `tz` via stdlib `zoneinfo`,
  stored as a UTC epoch in `next_run_at`. DST rules: spring-forward (2:30
  doesn't exist) → runs at the first valid instant after the gap; fall-back
  (2:30 exists twice) → runs exactly once. Both are explicit tests.
- **Missed runs** (box was down): on startup, a fire time missed by less than
  `FLEET_GRACE_SECONDS` (default 3600) runs once, late. Older → recorded as
  `missed` **with an alert** — never silent. Multiple missed fires collapse to
  at most one catch-up.
- **Overlap**: if a job is still running at its next fire time, the fire is
  recorded `skipped_overlap` and the schedule advances. Runs never stack.
- **Retries**: up to `retries` attempts, `retry_delay_seconds` apart, each its
  own `runs` row; notification fires per policy only after the final attempt.
- **Escalation**: same flap-free doctrine as watchers — `consecutive_failures`
  across scheduled runs, one urgent alert at `FLEET_FAIL_THRESHOLD` (default
  3), recovery alert on comeback.

## 5. Job kinds and notify policies

- **`script`**: subprocess shell, exit 0 = ok, stdout = output. `on_output`
  policy = classic cron etiquette (notify only when stdout non-empty).
  `always` + a script/prompt = scheduled phone notification (morning digest).
- **`claude`**: runs `claude -p "<target>" --model <model>` headlessly with
  auth from a mounted secret. Tools **disabled by default** (`allow_tools=0`);
  `allow_fleetctl=1` grants only `fleetctl` via CLI permission flags, nothing
  else. Retries forced to 0. Output flows into the same runs/notify pipeline.

## 6. Event handlers (server-side, DB-backed)

When a watcher changes or a job finishes and `handler_prompt` is set, the
engine runs a headless Claude with the event as context (old value, new value
/ output tail, name). The handler's reply becomes the notification body — the
**triage pattern**: Claude decides "boring, log it" vs "wake Jacob with a
one-line judgment". Handler runs are `runs` rows (at-least-once via SQLite —
no webhook plumbing, no second machine, nothing lost if the box reboots).
Handlers count against the claude budget; they run tools-off unless
`handler_allow_fleetctl` (the ticket-burst self-tuning pattern) is set.
**Handler failure of any kind passes the raw alert through, tagged
`[unjudged]`** — see §8.

## 7. Webhook watcher kind

`POST /hook/<watcher-name>/<webhook_secret>` on the worker HTTP server (tailnet
only). Body text (optionally reduced via the watcher's `extract` dot-path when
JSON) becomes the new value; the standard check/change/alert/handler pipeline
runs. Wrong or missing secret → 403, zero side effects. Secrets are generated
(`secrets.token_urlsafe`), shown once at create time, stored in the DB. A
future public sender is fronted by Cloudflare Tunnel for that one path —
documented, not built.

## 8. Intelligence degradation ladder

Tier 0 — **subscription** (Claude CLI, budget-gated, haiku default).
Tier 1 — **defer**: usage-limit errors are detected distinctly (parsed CLI
error, not guessed); `defer_ok` jobs reschedule past the reset window
(status `deferred`), one flap-free "ladder engaged" alert.
Tier 2 — **fallback endpoint** (optional): any OpenAI-compatible URL —
default OpenRouter free tier. `FLEET_FALLBACK_LLM_URL` (default
`https://openrouter.ai/api/v1`), `FLEET_FALLBACK_LLM_KEY`,
`FLEET_FALLBACK_MODELS` (comma list, passed as OpenRouter's `models` array so
failover across models happens server-side in one request). Only for items
with `fallback_ok=1` (privacy flag — free-tier providers may log/train).
One light retry, then fall through. If the configured model list vanishes →
a distinct, named alert (fail loud). Notes: OpenRouter free caps ≈50 req/day
(<$10 lifetime credit) / ≈1000 req/day after a one-time $10 top-up; a paid
API key with a console spend cap is the same mechanism with a different URL.
Tier 3 — **raw passthrough, always**: the underlying notification fires
untouched, tagged `[unjudged]`.

**Prime invariant (tested): no LLM failure of any kind — usage, network,
crash, exhausted fallback chain — may ever block, delay, or suppress the alert
that would have fired without the LLM.**

## 9. Hardening

1. **Claude budget gate**: before any claude/handler run, count today's
   claude `runs` against per-job `max_runs_per_day` and global
   `FLEET_CLAUDE_MAX_RUNS_PER_DAY` (default 24). Over → `budget_skipped` +
   exactly one alert. Dashboard shows today's count.
2. **Credentials**: Claude auth, ntfy token, OpenRouter key are k8s Secrets,
   mounted read-only; never in the image, git, or data-volume backups.
   Revocation runbook: log the session out from claude.ai. Prompt-injection
   blast radius: tools-off default means injected text can only produce weird
   notification prose, never actions.
3. **Dashboard is incapable of harm**: it opens its own SQLite connection
   with `mode=ro` (driver-level read-only — a bug cannot write) and serves
   GET only. Network layer: **Tailscale ACLs** — only tagged personal devices
   reach the dashboard port; **only the Mac** reaches the MCP port. Shipped as
   a documented ACL policy snippet.
4. **Audit everything**: every mutation (MCP, fleetctl, engine self-tuning)
   writes an `audit` row; `/audit` page renders it; rides the backups.
5. **Access has a second door**: tailscaled runs on the VM itself (SSH works
   independent of k8s); a one-command hostPort-pinned fallback exposure is
   documented for operator failure.

## 10. Management surfaces (three doors, one engine)

- **MCP** (from the Mac): all watcher/job tools. The only write surface
  besides fleetctl.
- **Dashboard** (any tailnet device): `/` overview sorted problems-first
  (watchers + jobs, status, next run, failures, today's claude count),
  `/runs`, `/alerts`, `/audit`. Read-only.
- **`fleetctl`** (inside the worker container; used over SSH and by jobs):
  `fleetctl jobs|watchers|runs <name>|watcher set-interval <name> <s>|job
  pause/resume <name>|…` — thin wrappers over the same db functions. The
  ticket-burst pattern: a 9:55 job tightens a watcher's interval, a 12:00 job
  relaxes it.

All three doors call identical db-layer functions — one behavior to test.

## 11. Deploy — k3s on the laptop

`deploy/k8s/` (plain YAML, `kubectl apply -k`):
- k3s on the Ubuntu Server VM (Proxmox), installed with
  `--tls-san <vm-tailscale-name>` for remote kubectl/**k9s** from the Mac.
- **`fleet-core` pod: worker + mcp containers sharing the `fleet-data` PVC**
  (k3s local-path = real ext4 → the v1 WAL/bind-mount lesson is satisfied).
- ntfy and changedetection.io: own Deployments + PVCs + Services.
- **Tailscale operator**: annotated Services get tailnet hostnames
  (`fleet-dash`, `fleet-mcp`, `ntfy`, `changedetection`); nothing listens on
  the LAN; the operator's API-server proxy serves kubectl/k9s.
- **Images without a registry**: built on the VM, imported via
  `docker save | k3s ctr images import`, `imagePullPolicy: IfNotPresent` —
  scripted in `deploy/k8s/build-import.sh`.
- **Backups**: a Kubernetes CronJob runs the WAL-safe sqlite `.backup` to a
  hostPath dir; the GCP watchdog's `pull-backup.sh` needs only the new
  hostname. `watchdog.sh` likewise unchanged (curls `/health` via tailnet).
- **Local dev**: docker-compose.yml stays as the Mac dev/smoke harness;
  engine behavior is identical in both.

## 12. Migration

`PRAGMA user_version` drives guarded startup migrations. v1→v2 adds the new
tables and watcher columns; existing rows untouched. TDD'd against a fixture
copy of a v1 database: migrate → assert schema v2 → assert every old row
intact. Data moves box-to-box via the existing restore drill (backup file →
`kubectl cp` into the PVC → restart).

## 13. Testing strategy

TDD throughout (test first, red before green), extending the v1 suite:
cron next-fire incl. both DST transitions and per-job tz; grace-window
(late-run, missed+alert, collapse); overlap-skip; retry attempts + final-only
notify; failure escalation and recovery for jobs; budget gate (per-job,
global, single alert); ladder (usage-limit detection, defer requeue, fallback
via httpx MockTransport, `fallback_ok=0` skip, **prime invariant**); handler
CLI assembly (tools-off, fleetctl grant); webhook (valid secret → pipeline,
bad secret → 403 no side effects); dashboard read-only connection rejects
writes + pages render fixtures; audit rows from every mutation door;
fleetctl arg→function mapping; migration fixture test. E2E smoke on compose
locally; deploy-day checklist validates the loop on real k3s.

## 14. Config reference (new env vars)

`FLEET_JOBS_MAX_CONCURRENT=5`, `FLEET_GRACE_SECONDS=3600`,
`FLEET_CLAUDE_MAX_RUNS_PER_DAY=24`, `FLEET_FALLBACK_LLM_URL`,
`FLEET_FALLBACK_LLM_KEY`, `FLEET_FALLBACK_MODELS`, `FLEET_TZ` (default box
tz). Existing v1 vars unchanged.

## 15. Explicitly out of scope

- n8n / Huginn (pipeline platforms): skipped — `script`/`claude` jobs + the
  handler pattern cover current needs; n8n is one manifest away if a real
  many-SaaS wiring need appears. No bespoke workflow/DAG engine.
- Local model (Ollama) triage tier: deferred until a RAM upgrade; the
  handler-runner seam (one function) makes it an afternoon later.
- Kubernetes beyond single-node k3s; Helm; public webhook ingress
  (Cloudflare Tunnel documented only).
- VM/Proxmox/Tailscale-ACL provisioning: user actions, guided by README.
