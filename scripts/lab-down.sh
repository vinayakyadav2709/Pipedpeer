#!/usr/bin/env bash
# Tear down the plain lab named LAB_NAME (default lab), and nothing else.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
pp_plain_lab_vars || exit 2
cd "$repo_root/lab"

pp_pick_runtime || exit 1
echo "Using container runtime: $PP_RUNTIME; lab $lab_name"

pp_compose -p "$lab_project" down
