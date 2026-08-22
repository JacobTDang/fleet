# Fleet on k3s (home-lab laptop)

Production deploy: k3s in an Ubuntu Server VM under Proxmox on the laptop.
docker-compose stays for local dev on the Mac; these manifests are the real
thing. Access is tailnet-only at every layer.

## 1. Install k3s

On the VM (which already runs tailscaled — SSH over the tailnet works
independently of everything below):

```bash
curl -sfL https://get.k3s.io | sh -s - --tls-san <vm-tailscale-name>
```

Remote `kubectl`/`k9s` from the Mac: copy `/etc/rancher/k3s/k3s.yaml`, change
`server:` to `https://<vm-tailscale-name>:6443`, save as `~/.kube/config`
(or export `KUBECONFIG`). `k9s` now works from anywhere on the tailnet.

## 2. Tailscale operator

In the Tailscale admin console create an **OAuth client** for the operator
(per its docs: tag `tag:k8s-operator`, scopes for devices), then:

```bash
helm repo add tailscale https://pkgs.tailscale.com/helmcharts
helm upgrade --install tailscale-operator tailscale/tailscale-operator \
  --namespace=tailscale --create-namespace \
  --set-string oauth.clientId=<id> --set-string oauth.clientSecret=<secret>
```

Every Service in this kit with `loadBalancerClass: tailscale` then acquires
its own tailnet hostname (`fleet-dash`, `fleet-mcp`, `ntfy`). Nothing listens on the LAN. The operator can also proxy
the Kubernetes API into the tailnet — the polished alternative to step 1's
kubeconfig edit.

## 3. Deploy fleet

```bash
./deploy/k8s/build-import.sh                      # build + import the image
cp deploy/k8s/secrets.example.yaml deploy/k8s/secrets.yaml   # fill it in
kubectl apply -f deploy/k8s/secrets.yaml
kubectl apply -k deploy/k8s
```

Set your timezone and (optionally) `FLEET_FALLBACK_MODELS` in `config.yaml`,
and the ntfy `NTFY_BASE_URL` hostname in `ntfy.yaml`, before applying.

**ntfy auth** (default deny-all):

```bash
kubectl -n fleet exec deploy/ntfy -- ntfy user add --role=admin <you>
kubectl -n fleet exec deploy/ntfy -- ntfy token add <you>
```

Put the token in `secrets.yaml` (`NTFY_TOKEN`), re-apply, restart:
`kubectl -n fleet rollout restart deploy/fleet-core`. Subscribe the phone app
to `http://ntfy.<tailnet>.ts.net/fleet-alerts` (phone on the tailnet).

## 4. Tailscale ACLs — scope the flat tailnet down

Only your devices should reach the fleet, and only the Mac should reach the
write surface (MCP). In the admin console's ACL policy:

```json
"acls": [
  {"action": "accept", "src": ["<your-mac>"],
   "dst": ["fleet-mcp:80", "fleet-dash:80", "ntfy:80",
            "<vm>:22", "<vm>:6443", "<vm>:8686"]},
  {"action": "accept", "src": ["<your-phone>"],
   "dst": ["fleet-dash:80", "ntfy:80"]}
]
```

(Adjust to your device names; add the watchdog box → `<vm>:8686` for /health.)

## 5. Connect Claude Code (on the Mac)

```bash
claude mcp add --transport http fleet http://fleet-mcp/mcp
```

Then manage in plain language: "create a job: every weekday at 9am, …".

## 6. Deploy-day checklist

Run each once; every line has a visible pass/fail:

1. `kubectl -n fleet exec deploy/fleet-core -c worker -- claude --version` → a version.
2. **Verify tool flags**: `kubectl -n fleet exec deploy/fleet-core -c worker -- claude --help | grep -E 'disallowedTools|allowedTools'` — if the flag names differ from `fleet/llm.py cli_args`, reconcile the code (only our arg assembly is unit-tested; the CLI's flag names are verified here).
3. `... -- sh -c 'claude -p "say ok" --model haiku'` → "ok" (proves `CLAUDE_CODE_OAUTH_TOKEN`).
4. Create a `* * * * *` script job with `notify_policy=always` via MCP → phone alert within a minute; `fleetctl runs <job>` shows it.
5. Create a webhook watcher, POST twice with different bodies → change alert; wrong secret → 403.
6. `kubectl -n fleet rollout restart deploy/fleet-core` → `fleetctl runs` history intact (PVC + migration survive restarts).
7. Watchdog (GCP box): point `FLEET_HOST` at `<vm-tailscale-name>`, then **fire the failure path once**: `kubectl -n fleet scale deploy/fleet-core --replicas=0`, wait for the urgent ntfy.sh alert, `--replicas=1`, wait for the recovery message.
8. Pin image tags: `kubectl -n fleet get pods -o jsonpath='{..imageID}'` and replace the `:latest` tag in the ntfy manifest.

## 6b. Optional: JavaScript rendering

`kubectl apply -f deploy/k8s/optional/browserless.yaml` adds one pod that
renders JS for `fetch_via="scrape"` watchers (set the token, then the three
`FLEET_SCRAPE_*` keys in the ConfigMap — the file documents them). Deliberately
not part of `kustomization.yaml`: most targets have a JSON endpoint that needs
no browser at all, and a browser is the heaviest thing on the node.

## 7. Fallback door (operator outage)

SSH to the VM always works (host tailscaled, independent of k8s). To expose
the dashboard without the operator:

```bash
kubectl -n fleet patch deploy fleet-core --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/ports/0/hostPort","value":8686}]'
```

Then browse `http://<vm-tailscale-ip>:8686/`. Revert by deleting the patch
(re-apply `fleet-core.yaml`).

## 8. Restore drill (practice once)

1. New VM → k3s + operator (steps 1–2) → `build-import.sh` → apply manifests.
2. Copy the newest backup from the watchdog box, then:
   `kubectl -n fleet cp fleet-latest.db <fleet-core-pod>:/data/fleet.db -c worker`
3. `kubectl -n fleet rollout restart deploy/fleet-core`
4. Redo the ntfy user/token step; re-point the watchdog. About an evening — by design.

## 9. Public webhooks (later, if ever)

A Cloudflare Tunnel fronting only the `/hook/...` path gives external
services (TradingView, GitHub) a public URL while the box stays dark.
Documented on purpose, not built.
