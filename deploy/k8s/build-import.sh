#!/bin/bash
# Build the fleet image on the VM and import it straight into k3s's
# containerd — no registry, public or otherwise, ever sees it.
set -euo pipefail
cd "$(dirname "$0")/../.."
docker build -t fleet:latest .
docker save fleet:latest | sudo k3s ctr images import -
echo "imported fleet:latest; restart to pick up: kubectl -n fleet rollout restart deploy/fleet-core"
