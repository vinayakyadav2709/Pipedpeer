#!/usr/bin/env bash
# Run ONCE on each worker laptop (not the orchestrator).
# Joins it to the LAN cluster as a compute node.
set -euo pipefail

echo "==> building pipedpeer (or copy bin/pipedpeer onto this machine instead)"
if [ ! -x bin/pipedpeer ]; then
  if ! command -v go >/dev/null 2>&1; then
    echo "Go not found. Copy the built 'pipedpeer' binary here or install Go."; exit 1
  fi
  (cd "$(dirname "$0")/.." && make build)
fi

echo "==> setup: checks prerequisites (nix, tar, bash), installs if missing, starts the daemon"
./bin/pipedpeer setup -y || sudo ./bin/pipedpeer setup -y

echo "==> verify"
./bin/pipedpeer status
./bin/pipedpeer nodes

echo "==> nix: multi-user installs must accept unsigned closures from peers"
if systemctl is-active --quiet nix-daemon 2>/dev/null; then
  if ! nix config show require-sigs 2>/dev/null | grep -q false; then
    echo "   run: echo 'require-sigs = false' | sudo tee -a /etc/nix/nix.custom.conf"
    echo "        sudo systemctl restart nix-daemon"
  fi
fi

echo "==> firewall: allow inbound daemon + peer traffic (adjust to taste)"
# sudo ufw allow 38080/tcp          # pipedpeer daemon API
# sudo ufw allow 29500:29510/tcp    # DDP rendezvous — open once, every future --ddp run just works

echo "==> worker is live. Watch it during the demo with:"
echo "    htop"
echo "    tail -f /tmp/pipedpeer/daemon.log"