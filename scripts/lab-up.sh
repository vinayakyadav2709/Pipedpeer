#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Building pipedpeer binary..."
cd "$repo_root/src"
CGO_ENABLED=0 go build -o "$repo_root/lab/pipedpeer" .
echo "Stopping host daemon if running..."
cd "$repo_root"
./bin/pipedpeer stop 2>/dev/null || true
./lab/pipedpeer stop 2>/dev/null || true
# Free ports if stuck from previous host-networked containers
fuser -k 38081/tcp 2>/dev/null || true
fuser -k 38082/tcp 2>/dev/null || true
fuser -k 38083/tcp 2>/dev/null || true

cd "$repo_root/lab"

# Detect container runtime (podman preferred, fallback to docker)
if command -v podman &>/dev/null; then
    RUNTIME="podman"
elif command -v docker &>/dev/null; then
    RUNTIME="docker"
else
    echo "Error: No container runtime found (podman/docker)"
    exit 1
fi

echo "Using container runtime: $RUNTIME"

compose() {
    if [ "$RUNTIME" = "podman" ]; then
        if command -v podman-compose &>/dev/null; then
            podman-compose "$@"
        else
            podman compose "$@"
        fi
    else
        docker compose "$@"
    fi
}

# Tear the stack down before bringing it up. The fuser -k above kills the
# daemon inside an already-running host-networked container, but compose then
# sees the container as already defined and will not recreate it, so the lab
# comes back with dead workers and every caller times out waiting on /health.
compose down --remove-orphans 2>/dev/null || true

compose up -d --build
