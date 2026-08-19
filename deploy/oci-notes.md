# Oracle Cloud notes (provisioning, lockdown, survival)

## Sizing — do not exceed the Always Free cap

As of 2026-06-15 the Always Free Ampere A1 allowance is **2 OCPU / 12 GB RAM**
(1,500 OCPU-hours + 9,000 GB-hours per month), enforced by termination since
2026-08-18. Create the instance at exactly `VM.Standard.A1.Flex, 2 OCPU, 12 GB`
— an oversized instance is a termination target and, once lost, cannot be
recreated above the cap. Boot volume: Ubuntu (aarch64), default ~47 GB is
within the free 200 GB block-storage allowance.

## "Out of capacity" errors

`Out of capacity for shape VM.Standard.A1.Flex` is common. Tactics, in order:
try a different availability domain; try at off-peak hours; keep clicking
(capacity frees in bursts); as a last resort script the launch API to retry.
Home-region choice is permanent for Always Free — pick one with several ADs.

## Network lockdown (defense layer 1 — the cloud firewall)

The instance's subnet has a Security List / NSG. Make inbound **default-deny**:

1. VCN → Security Lists → remove every inbound rule, including the default
   `0.0.0.0/0 tcp/22` SSH rule — after Tailscale is up you SSH over the
   tailnet, not the public internet.
2. Leave egress open (the box needs outbound HTTPS to poll targets, and
   Tailscale connects outbound).
3. Order of operations: install + authenticate Tailscale FIRST, confirm
   `ssh <user>@<tailscale-ip>` works, THEN delete the public SSH rule.

Layer 2 is in docker-compose.yml: every service binds to the Tailscale IP.
Both layers must fail for anything to be exposed. Do not install ufw — Docker
bypasses it at the iptables level and it only creates false confidence.

## Idle reclamation (documented, and this box qualifies)

Oracle may reclaim Always Free instances when, over 7 days, 95th-percentile
CPU, network, AND (on A1) memory utilization are all under 20%. A light watcher
fleet idles exactly like that. Mitigations: the fleet's own steady polling
helps network; if reclamation warnings arrive anyway, either upgrade the
account to Pay-As-You-Go with $0 spend (widely reported to exempt reclamation,
not verified — see the research report) or accept rebuilds via the restore
drill below.

## Restore drill (practice it once before you need it)

The box is disposable by design; the fleet is not. To rebuild from nothing:

1. New VM (any host) → clone the repo → `./deploy/setup.sh`
2. Copy the newest backup from the watchdog box:
   `scp <watchdog>:~/fleet-backups/fleet-<date>.db /tmp/restore.db`
3. Load it into the named volume, then restart:
   `sudo docker compose cp /tmp/restore.db worker:/data/fleet.db && sudo docker compose restart worker mcp`
4. Re-do the ntfy user/token step, re-point the watchdog's FLEET_HOST if the
   tailscale name changed. Total cost: about an evening — by design.
