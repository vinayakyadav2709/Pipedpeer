#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
cd "$repo_root/lab"

pp_pick_runtime || exit 1
echo "Using container runtime: $PP_RUNTIME"

pp_compose down
