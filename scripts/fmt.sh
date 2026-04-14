#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/cmd/pipedpeer"

gofmt -w .
echo "Formatted Go files in cmd/pipedpeer"