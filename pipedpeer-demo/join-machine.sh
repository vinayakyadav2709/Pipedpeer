#!/usr/bin/env bash
# Everything one machine needs, in one line.
#
#   ./join-machine.sh <introducer-ip>            # the first machine
#   ./join-machine.sh <introducer-ip> <token>    # every machine after it
#
# This is exactly Part 1 and Part 2 of DEMO.md with nothing hidden - install,
# setup, share the cluster secret, join. Read those parts if you would rather
# see each step; there is no magic in here.
set -euo pipefail

introducer="${1:-}"
token="${2:-}"
if [[ -z "$introducer" ]]; then
	echo "usage: ./join-machine.sh <introducer-ip> [cluster-token]" >&2
	echo >&2
	echo "Run it without a token on the FIRST machine, then print the token with" >&2
	echo "  pipedpeer auth show" >&2
	echo "and pass that to the others." >&2
	exit 2
fi

if ! command -v pipedpeer >/dev/null 2>&1; then
	echo "==> installing pipedpeer"
	curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh |
		bash -s -- --channel nightly
	export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> setup (nix, crun, identity, firewall, daemon)"
pipedpeer setup -y

echo "==> cluster secret"
if [[ -n "$token" ]]; then
	pipedpeer auth set "$token"
else
	pipedpeer auth set
	echo
	echo "This machine generated the cluster secret. Give it to the others:"
	echo
	echo "    ./join-machine.sh $introducer $(pipedpeer auth show | head -1)"
	echo
fi

echo "==> joining through $introducer"
pipedpeer join "$introducer"

echo
echo "Done. Watch the cluster with:  pipedpeer dashboard"
