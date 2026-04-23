#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/cmd/pipedpeer"

go build -o "$repo_root/bin/pipedpeer" .
echo "Built: $repo_root/bin/pipedpeer"
