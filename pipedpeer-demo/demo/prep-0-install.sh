#!/usr/bin/env bash
# Run on EVERY machine (box-a, box-b, box-c), before anything else.
#
#   ./demo/prep-0-install.sh                     # install the nightly build
#   ./demo/prep-0-install.sh /path/to/pipedpeer  # install a binary you copied over
#
# Every machine must run the SAME binary. The demo build is the nightly
# channel, built from dev (the demo round was merged into dev on 2026-10-12).
# To use your own build instead, copy it (`make build` -> bin/pipedpeer,
# linux/amd64) to each machine and pass it here.
# The sha256 printed at the end must match on all three.
set -euo pipefail

src="${1:-}"
dest="$HOME/.local/bin/pipedpeer"
mkdir -p "$HOME/.local/bin"

if pgrep -x pipedpeer >/dev/null 2>&1; then
	echo "==> stopping the running daemon so the binary can be replaced"
	"$dest" stop 2>/dev/null || pipedpeer stop 2>/dev/null || true
fi

if [[ -n "$src" ]]; then
	[[ -x "$src" ]] || { echo "not an executable: $src" >&2; exit 2; }
	echo "==> installing $src -> $dest"
	install -m 0755 "$src" "$dest"
else
	echo "==> installing the nightly build"
	curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh |
		bash -s -- --channel nightly
fi

# Same PATH step as setup-machine.sh: LAPTOP-C and PC-3 never run that one.
source "$(dirname "$0")/_checks.sh"
ensure_local_bin_path

echo "==> setup (nix, crun, identity, firewall, daemon)"
"$dest" setup -y

echo
echo "binary: $dest"
echo "sha256: $(sha256sum "$dest" | cut -c1-16)   <- must be identical on every machine"
echo
echo "Next: the next line of your card (cards/<ROLE>.txt). First machine: ./setup-machine.sh fedora-laptop --rendezvous;"
echo "      the others: their line from join-command.txt (PC-3: ./demo/prep-4-common.sh only, no join)."
