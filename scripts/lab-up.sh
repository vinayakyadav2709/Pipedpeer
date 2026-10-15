#!/usr/bin/env bash
# The plain lab: three worker containers on one bridge (lab/docker-compose.yml),
# one cluster by LAN discovery. This machine's own daemon is not part of it;
# drive the lab from inside a worker (docker exec pipedpeer-lab-1 pipedpeer ...).
# The token is saved to ~/.local/state/pipedpeer/lab.token for callers.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
source "$repo_root/scripts/lib/cli.sh"

# PIPEDPEER first, and build only when there is source to build from: the lab
# is brought up on the machine that has the cluster, which is not always the
# machine with the checkout.
pp_resolve_cli "$repo_root" || exit 2
mkdir -p "$repo_root/lab"
install -m 0755 "$PP_CLI" "$repo_root/lab/pipedpeer"

state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$state_dir"
LAB_TOKEN="${LAB_TOKEN:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}"
(umask 077 && printf '%s\n' "$LAB_TOKEN" >"$state_dir/lab.token")
export LAB_TOKEN

pp_pick_runtime || exit 1
rt="$PP_RUNTIME"
echo "Using container runtime: $rt"

cd "$repo_root/lab"
# Tear down first: compose will not recreate a container it already knows.
pp_compose down --remove-orphans 2>/dev/null || true
pp_lab_mem_check "$((3 * $(pp_bytes 1536m)))" lab-up || exit 1
pp_compose up -d --build
