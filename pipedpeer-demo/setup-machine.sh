#!/usr/bin/env bash
# One script per machine role. Safe to re-run: each step checks before it acts.
#
#   ./setup-machine.sh <role> [--introducer IP] [--token T] [--prep]
#
# role: ubuntu-pc | fedora-laptop | gpu-laptop
#   --introducer IP  join through this machine (the laptop running `pipedpeer rendezvous`
#                    or any machine already in the cluster). Without it, rely on LAN
#                    auto-discovery: the daemon finds peers that share the secret.
#   --token T        cluster secret from `pipedpeer auth show` on the first machine.
#                    Without it, this machine makes a new secret (first machine only).
#   --prep           also run the prep scripts at the lab visit (prep-4-common, and
#                    prep-3-gpu on gpu-laptop). Slow the first time.
# Steps are those of join-machine.sh (install, setup, auth, join) plus the prep scripts;
# nothing is copied from them.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
role="${1:-}"
[[ -n "$role" ]] && shift || true
introducer="" token="" prep=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--introducer) introducer="${2:?--introducer needs an IP}"; shift 2 ;;
	--token) token="${2:?--token needs a value}"; shift 2 ;;
	--prep) prep=1; shift ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
case "$role" in
ubuntu-pc | fedora-laptop | gpu-laptop) ;;
*) echo "usage: $0 ubuntu-pc|fedora-laptop|gpu-laptop [--introducer IP] [--token T] [--prep]" >&2; exit 2 ;;
esac

export PATH="$HOME/.local/bin:$PATH"

echo "==> [$role] install"
if command -v pipedpeer >/dev/null 2>&1; then
	echo "already installed: $(command -v pipedpeer)"
else
	curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh |
		bash -s -- --channel nightly
fi

if [[ "$role" == "gpu-laptop" ]] && ! command -v nvidia-smi >/dev/null 2>&1; then
	echo "warn: nvidia-smi not found; install the NVIDIA driver or this machine will not offer a GPU" >&2
fi

echo "==> [$role] setup (nix, crun, identity, firewall, daemon)"
# Opens the ports itself (sudo): 38080/tcp 38099/udp 38447/udp 38445/udp 29500-29509/tcp (setup.FirewallRules). If the firewall still blocks peers on Ubuntu: sudo ufw disable
pipedpeer setup -y

echo "==> [$role] cluster secret"
if [[ -n "$token" ]]; then
	pipedpeer auth set "$token"
else
	pipedpeer auth set
	echo "Secret made here. Give it to the others:  --token $(pipedpeer auth show | head -1)"
fi

if [[ -n "$introducer" ]]; then
	echo "==> [$role] join through $introducer"
	pipedpeer join "$introducer"
else
	# The daemon reads the secret at start; without a restart it keeps answering
	# without proving the token, and peers refuse it (join restarts it for us).
	echo "==> [$role] restarting the daemon so it uses the secret"
	pipedpeer stop >/dev/null 2>&1 || echo "note: no daemon was running"
	pipedpeer start
	echo "==> [$role] no --introducer: waiting on LAN auto-discovery (see 'pipedpeer nodes' in ~30 s)"
fi

if [[ "$prep" == 1 ]]; then
	"$here/demo/prep-4-common.sh"
	[[ "$role" == "gpu-laptop" ]] && "$here/demo/prep-3-gpu.sh"
fi
echo "Done. Check: pipedpeer status; pipedpeer nodes"
