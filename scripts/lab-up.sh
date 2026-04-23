#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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

if [ "$RUNTIME" = "podman" ]; then
    # Use podman-compose directly for better reliability
    if command -v podman-compose &>/dev/null; then
        podman-compose up -d --build
    else
        # Fallback to podman compose (may use docker-compose provider)
        podman compose up -d --build
    fi
else
    docker compose up -d --build
fi
