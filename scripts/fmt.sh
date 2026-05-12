#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Now use src instead of cmd
cd "$repo_root/src"

gofmt -w .
echo "Formatted Go files in src"