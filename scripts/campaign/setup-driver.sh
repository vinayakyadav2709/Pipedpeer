#!/usr/bin/env bash
# setup-driver.sh: sets up a Fedora driver machine for the lab campaign.
# Creates the setup record, installs required Fedora packages, builds pipedpeer,
# and verifies readiness via check-driver.sh --no-r2.
set -euo pipefail

# Check OS: Fedora only
os_release="${PIPEDPEER_OS_RELEASE:-/etc/os-release}"
if [[ -f "$os_release" ]]; then
	# shellcheck source=/dev/null
	. "$os_release"
	if [[ "${ID:-}" != "fedora" && "${ID_LIKE:-}" != *"fedora"* ]]; then
		echo "setup-driver.sh: only Fedora is supported (detected: ${ID:-unknown})" >&2
		exit 1
	fi
else
	echo "setup-driver.sh: cannot detect OS ($os_release missing); only Fedora is supported" >&2
	exit 1
fi

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)" || {
	echo "setup-driver.sh: run this from a Pipedpeer checkout" >&2
	exit 1
}
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-lab-setup"
RECORD_FILE="$STATE_DIR/record"

# 1. FIRST write the setup record if it does not already exist
if [[ ! -f "$RECORD_FILE" ]]; then
	nix_dir="${PIPEDPEER_TEST_NIX_DIR:-/nix}"
	nix_preexisted="no"
	if [[ -e "$nix_dir" || -d "$nix_dir" ]]; then
		nix_preexisted="yes"
	fi
	mkdir -p "$STATE_DIR"
	cat >"$RECORD_FILE" <<-EOF
		repo=$REPO
		nix_preexisted=$nix_preexisted
	EOF
	echo "Recorded lab setup: repo=$REPO, nix_preexisted=$nix_preexisted at $RECORD_FILE"
else
	echo "Setup record already exists at $RECORD_FILE; leaving unchanged"
fi

if [[ "${PIPEDPEER_SETUP_RECORD_ONLY:-0}" == "1" || "${1:-}" == "--record-only" ]]; then
	exit 0
fi

# Ensure lab ssh key exists for check-driver.sh and driver operations
if [[ ! -f "$HOME/.ssh/pipedpeer-lab" ]]; then
	mkdir -p "$HOME/.ssh"
	chmod 700 "$HOME/.ssh"
	ssh-keygen -t ed25519 -N '' -f "$HOME/.ssh/pipedpeer-lab"
fi

# 2. Install Fedora dependencies
echo "==> Installing driver packages via dnf..."
sudo dnf install -y \
	pssh parallel rsync nmap iperf3 openssh-clients \
	python3-matplotlib python3-pandas python3-scipy python3-pytest \
	rclone ShellCheck uv golang podman make

# 3. Build pipedpeer
echo "==> Building pipedpeer..."
make -C "$REPO" build

# 4. Check driver readiness
echo "==> Checking driver readiness..."
"$REPO/scripts/campaign/check-driver.sh" --no-r2
