#!/usr/bin/env bash
# Run ONCE on the worker machine (not the orchestrator).
# Joins it to the LAN cluster as a compute node.
set -euo pipefail

echo "==> installing the latest nightly pipedpeer"
if ! command -v pipedpeer >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly
  export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> setup: checks prerequisites (nix, crun, unsigned-closure imports), installs what's missing, starts the daemon"
pipedpeer setup -y

echo "==> verify"
pipedpeer status
pipedpeer nodes

echo "==> firewall: allow inbound daemon traffic (one port carries everything, DDP sync included)"
# sudo ufw allow 38080/tcp          # pipedpeer daemon API — the only port needed

echo "==> worker is live. Watch it during the demo with:"
echo "    htop            (CPU demos)"
echo "    nvidia-smi -l 1 (GPU DDP)"
echo "    tail -f /tmp/pipedpeer/daemon.log"
