#!/bin/bash
# Harden the fleet VM (Ubuntu 24.04 LTS). Safe to run again.
#
#   sudo ADMIN_LAN_IP=<lan ip> bash harden.sh   apply every control; ADMIN_LAN_IP is the one LAN address
#                                               that may open SSH (the Proxmox host, as a fallback door)
#   sudo bash harden.sh --check  print the state of every control; exit 1 if one is missing
#
# The controls back the security answers in amazon-retail's docs/security-controls.md: a host firewall,
# CrowdSec (intrusion detection) with its firewall bouncer (intrusion prevention), ClamAV with a nightly
# scan, automatic security updates, SSH by key only, and the password rules.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "harden.sh: run it with sudo" >&2; exit 1; }

K3S_POD_CIDR=10.42.0.0/16
K3S_SERVICE_CIDR=10.43.0.0/16

check() {
    # grep -q stops reading at its first match; with pipefail the writer's broken pipe would fail the check.
    set +o pipefail
    local bad=0
    say() { printf '%-28s %s\n' "$1" "$2"; }
    need() { if eval "$2" >/dev/null 2>&1; then say "$1" ok; else say "$1" MISSING; bad=1; fi; }
    need "firewall active"          "ufw status | grep -q '^Status: active'"
    need "firewall denies inbound"  "ufw status verbose | grep -q 'deny (incoming)'"
    need "crowdsec running"         "systemctl is-active --quiet crowdsec"
    need "crowdsec bouncer running" "systemctl is-active --quiet crowdsec-firewall-bouncer"
    need "bouncer registered"       "cscli bouncers list -o json | jq -e 'length > 0'"
    need "clamav signatures fresh"  "find /var/lib/clamav -name 'daily.c[lv]d' -mtime -2 | grep -q ."
    need "nightly scan scheduled"   "systemctl is-enabled --quiet clamscan-nightly.timer"
    need "last scan clean"          "! systemctl is-failed --quiet clamscan-nightly.service"
    need "security updates on"      "grep -q 'Unattended-Upgrade \"1\"' /etc/apt/apt.conf.d/20auto-upgrades"
    need "ssh keys only"            "sshd -T | grep -qx 'passwordauthentication no'"
    need "ssh no root login"        "sshd -T | grep -qx 'permitrootlogin no'"
    need "password min length 12"   "grep -qx 'minlen = 12' /etc/security/pwquality.conf.d/10-fleet.conf"
    need "password expiry 365 days" "grep -qE '^PASS_MAX_DAYS\s+365$' /etc/login.defs"
    for u in $(awk -F: '$3 >= 1000 && $3 < 65534 {print $1}' /etc/passwd); do
        need "user $u expires in 365d" "chage -l $u | grep -q 'Maximum number of days between password change\s*:\s*365'"
    done
    need "guest agent running"      "systemctl is-active --quiet qemu-guest-agent"
    need "cloud-init disabled"      "[ -f /etc/cloud/cloud-init.disabled ]"
    need "k3s: no traefik/servicelb" "grep -qx '  - traefik' /etc/rancher/k3s/config.yaml && grep -qx '  - servicelb' /etc/rancher/k3s/config.yaml"
    need "k3s: secrets encrypted"   "grep -qx 'secrets-encryption: true' /etc/rancher/k3s/config.yaml"
    if command -v k3s >/dev/null; then
        need "k3s: encryption enabled"  "k3s secrets-encrypt status | grep -q 'Encryption Status: Enabled'"
        # ServiceLB would publish LoadBalancer services on the LAN; the Tailscale operator's
        # loadBalancerClass is allowed — those services are only served on the tailnet.
        need "k3s: no LoadBalancer svc" "k3s kubectl get svc -A -o json | jq -e '[.items[] | select(.spec.type==\"LoadBalancer\" and (.spec.loadBalancerClass // \"\") != \"tailscale\")] | length == 0'"
    fi
    return $bad
}

if [ "${1:-}" = "--check" ]; then
    check
    exit $?
fi

: "${ADMIN_LAN_IP:?set ADMIN_LAN_IP to the one LAN address allowed to open SSH (the Proxmox host)}"

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get -y -q upgrade
apt-get install -y -q qemu-guest-agent ufw unattended-upgrades libpam-pwquality clamav clamav-freshclam \
    curl jq git
systemctl enable --now qemu-guest-agent

# Security updates install themselves every day.
cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF

# SSH: keys only, no root login.
cat > /etc/ssh/sshd_config.d/10-hardening.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
EOF
sshd -t
systemctl reload ssh

# Passwords: at least 12 characters with upper case, lower case, a digit and a special character;
# they expire after 365 days.
mkdir -p /etc/security/pwquality.conf.d
cat > /etc/security/pwquality.conf.d/10-fleet.conf <<'EOF'
minlen = 12
ucredit = -1
lcredit = -1
dcredit = -1
ocredit = -1
EOF
sed -i -E 's/^PASS_MAX_DAYS\s+.*/PASS_MAX_DAYS\t365/' /etc/login.defs
for u in $(awk -F: '$3 >= 1000 && $3 < 65534 {print $1}' /etc/passwd); do chage -M 365 "$u"; done

# Firewall: nothing comes in except over Tailscale, from the k3s pod and service networks, and SSH from
# the Proxmox host.
ufw default deny incoming
ufw default allow outgoing
ufw allow in on tailscale0 comment 'tailnet'
ufw allow from "$K3S_POD_CIDR" comment 'k3s pods'
ufw allow from "$K3S_SERVICE_CIDR" comment 'k3s services'
ufw allow from "$ADMIN_LAN_IP" to any port 22 proto tcp comment 'ssh from the proxmox host'
ufw --force enable

# CrowdSec reads the system logs and detects attacks (IDS); its firewall bouncer blocks the attacking
# addresses (IPS). Packages come from CrowdSec's own apt repository.
if ! command -v cscli >/dev/null; then
    curl -fsSL https://install.crowdsec.net | sh
    apt-get install -y -q crowdsec
fi
apt-get install -y -q crowdsec-firewall-bouncer-iptables
cscli collections install crowdsecurity/linux crowdsecurity/sshd
systemctl enable --now crowdsec crowdsec-firewall-bouncer
systemctl reload crowdsec

# ClamAV: signatures update by themselves; the whole disk is scanned every night at low priority. A
# found file or a scan error fails the unit, which --check and `systemctl --failed` report.
systemctl enable --now clamav-freshclam
mkdir -p /var/log/clamav
cat > /etc/systemd/system/clamscan-nightly.service <<'EOF'
[Unit]
Description=ClamAV scan of the whole disk
After=clamav-freshclam.service

[Service]
Type=oneshot
Nice=19
IOSchedulingClass=idle
ExecStart=/usr/bin/clamscan --recursive --infected --cross-fs=no --exclude-dir=^/proc --exclude-dir=^/sys --exclude-dir=^/dev --exclude-dir=^/run --log=/var/log/clamav/nightly.log /
EOF
cat > /etc/systemd/system/clamscan-nightly.timer <<'EOF'
[Unit]
Description=Nightly ClamAV scan

[Timer]
OnCalendar=*-*-* 03:30
RandomizedDelaySec=30m
Persistent=true

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now clamscan-nightly.timer

# k3s settings, read when k3s is installed or restarted. Its bundled Traefik and ServiceLB publish ports 80
# and 443 on the node through rules that bypass ufw (seen on the LAN on 2026-11-11). fleet reaches its
# services through the Tailscale operator, so both stay off. Secrets are encrypted at rest.
mkdir -p /etc/rancher/k3s
cat > /etc/rancher/k3s/config.yaml <<'EOF'
# Nothing listens on the LAN: services are reached over Tailscale (the Tailscale operator), not Traefik/ServiceLB.
secrets-encryption: true
disable:
  - traefik
  - servicelb
EOF
if systemctl is-active --quiet k3s; then
    systemctl restart k3s
    # k3s installed before this file existed leaves Traefik's LoadBalancer service behind, held by a
    # ServiceLB finalizer that nothing clears once ServiceLB is off.
    for i in $(seq 1 30); do k3s kubectl get nodes 2>/dev/null | grep -q ' Ready' && break; sleep 4; done
    if k3s kubectl -n kube-system get svc traefik >/dev/null 2>&1; then
        k3s kubectl -n kube-system patch svc traefik --type=merge -p '{"metadata":{"finalizers":null}}'
        k3s kubectl -n kube-system delete svc traefik --wait=false
    fi
fi

# Disable cloud-init so a regenerated cloud-init drive (for example after a Proxmox migration) cannot
# re-run first-boot steps on a hardened host.
mkdir -p /etc/cloud
cat > /etc/cloud/cloud-init.disabled <<'EOF'
# Disabled by fleet harden.sh: a regenerated cloud-init drive, for example after a migration,
# must not re-run first-boot steps on a hardened host.
EOF

echo
check
