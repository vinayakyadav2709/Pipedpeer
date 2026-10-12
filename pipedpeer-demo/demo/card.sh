#!/usr/bin/env bash
# Print one machine's demo card: only that machine's steps, in order.
#
#   ./demo/card.sh LAPTOP-A      # or LAPTOP-B, LAPTOP-C, PC
set -euo pipefail

dir="$(cd "$(dirname "$0")/../cards" && pwd)"
role="${1:-}"
card="$dir/${role^^}.md"

if [[ -z "$role" || ! -f "$card" ]]; then
	[[ -n "$role" ]] && echo "no card for role: $role" >&2
	echo "usage: $0 <role>" >&2
	echo "roles:" >&2
	for f in "$dir"/*.md; do echo "  $(basename "$f" .md)" >&2; done
	exit 2
fi

if [[ -t 1 ]] && command -v less >/dev/null; then
	less -R "$card"
else
	cat "$card"
fi
