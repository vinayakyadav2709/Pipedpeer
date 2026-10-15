#!/usr/bin/env bash
# The plain lab: three worker containers on one bridge (lab/docker-compose.yml),
# one cluster by LAN discovery. This machine's own daemon is not part of it;
# drive the lab from inside a worker (docker exec pipedpeer-lab-1 pipedpeer ...).
# LAB_NAME (default lab) gives the lab its own containers (pipedpeer-lab-<name>-<i>),
# subnet, compose project and token file (~/.local/state/pipedpeer/lab-<name>.token),
# so several can run at once; lab-down.sh and lab-fail.sh take the same name.
# LAB_MEM is the memory per worker, swap included (default 1g; 3 workers).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
source "$repo_root/scripts/lib/cli.sh"

# PIPEDPEER first, and build only when there is source to build from: the lab
# is brought up on the machine that has the cluster, which is not always the
# machine with the checkout.
pp_plain_lab_vars || exit 2
pp_resolve_cli "$repo_root" || exit 2
# Each lab mounts its own copy, so labs on different builds do not clash.
if [[ "$lab_name" == lab ]]; then
	LAB_BIN="$repo_root/lab/pipedpeer"
else
	LAB_BIN="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/lab-$lab_name/pipedpeer"
fi
export LAB_BIN
mkdir -p "$(dirname "$LAB_BIN")"
# Install beside the old binary and rename: a running lab's containers keep
# the file they mounted instead of failing with "Text file busy".
install -m 0755 "$PP_CLI" "$LAB_BIN.new"
mv -f "$LAB_BIN.new" "$LAB_BIN"

state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$state_dir"
LAB_TOKEN="${LAB_TOKEN:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}"
(umask 077 && printf '%s\n' "$LAB_TOKEN" >"$lab_token_file")
export LAB_TOKEN

pp_pick_runtime || exit 1
rt="$PP_RUNTIME"
echo "Using container runtime: $rt; lab $lab_name ($LAB_CTR-<i>, $LAB_SUBNET)"

cd "$repo_root/lab"
# Tear down first: compose will not recreate a container it already knows.
pp_compose -p "$lab_project" down --remove-orphans 2>/dev/null || true
LAB_MEM="${LAB_MEM:-1g}"
export LAB_MEM
pp_lab_mem_check "$((3 * $(pp_bytes "$LAB_MEM")))" lab-up || exit 1
pp_compose -p "$lab_project" up -d --build
