#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# PIPEDPEER first, and build only when there is source to build from. The lab
# is brought up on the machine that has the cluster, which is not the machine
# with the checkout - assuming otherwise made every harness that calls this
# die on "cd: .../src: No such file or directory".
cli="${PIPEDPEER:-$repo_root/bin/pipedpeer}"
mkdir -p "$repo_root/lab"

if [[ -d "$repo_root/src" ]]; then
	echo "Building pipedpeer binary..."
	(cd "$repo_root/src" && CGO_ENABLED=0 go build -o "$repo_root/lab/pipedpeer" .)
elif [[ -x "$cli" ]]; then
	echo "Using $cli (no source here to build from)"
	cp "$cli" "$repo_root/lab/pipedpeer"
	chmod +x "$repo_root/lab/pipedpeer"
else
	echo "no source to build from and no binary at $cli" >&2
	exit 2
fi

# Lab workers are recreated on every run, so each gets a new certificate and
# the pin from last time is stale. That refusal is correct for real machines
# and pure friction here, where the daemons are disposable by design.
"$cli" auth forget --all >/dev/null 2>&1 || true

echo "Stopping host daemon if running..."
cd "$repo_root"
"$cli" stop 2>/dev/null || true
./lab/pipedpeer stop 2>/dev/null || true
# Free ports if stuck from previous host-networked containers
fuser -k 38081/tcp 2>/dev/null || true
fuser -k 38082/tcp 2>/dev/null || true
fuser -k 38083/tcp 2>/dev/null || true

cd "$repo_root/lab"

source "$repo_root/scripts/lib/runtime.sh"
pp_pick_runtime || exit 1
echo "Using container runtime: $PP_RUNTIME"
compose() { pp_compose "$@"; }

# Tear the stack down before bringing it up. The fuser -k above kills the
# daemon inside an already-running host-networked container, but compose then
# sees the container as already defined and will not recreate it, so the lab
# comes back with dead workers and every caller times out waiting on /health.
compose down --remove-orphans 2>/dev/null || true

compose up -d --build
