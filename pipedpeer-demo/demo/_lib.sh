#!/usr/bin/env bash
# Shared by every demo step. Sourced, never run.
#
# Puts the shell in pipedpeer-demo/ (where the scripts live), finds the
# pipedpeer binary the same way compare.sh does, and gives each step a banner
# and a presenter note so the screen says what to look at.
set -uo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DEMO_DIR"

PIPE="${PIPEDPEER_BIN:-}"
if [[ -z "$PIPE" ]]; then
	if command -v pipedpeer >/dev/null 2>&1; then
		PIPE="$(command -v pipedpeer)"
	elif [[ -x ../bin/pipedpeer ]]; then
		PIPE="$(cd .. && pwd)/bin/pipedpeer"
	fi
fi
if [[ -z "$PIPE" || ! -x "$PIPE" ]]; then
	echo "pipedpeer not found - run demo/prep-0-install.sh first" >&2
	exit 2
fi
# compare.sh and setup.sh honour this too, so every step uses one binary.
export PIPEDPEER_BIN="$PIPE"

if [[ -t 1 ]]; then
	B=$'\e[1m' DIM=$'\e[2m' Y=$'\e[33m' G=$'\e[32m' R=$'\e[31m' N=$'\e[0m'
else
	B='' DIM='' Y='' G='' R='' N=''
fi

banner() {
	echo
	echo "${B}================================================================${N}"
	echo "${B} $*${N}"
	echo "${B}================================================================${N}"
}

# A line for the presenter, not the audience: what to point at.
note() { echo "${Y}  > $*${N}"; }

# Show a command before running it, so the audience sees what was typed.
show() {
	local shown=("$@")
	[[ "${shown[0]}" == "$PIPE" ]] && shown[0]=pipedpeer
	echo "${DIM}\$ ${shown[*]}${N}"
	"$@"
}

# Only the echo, for commands whose output is filtered through a pipe (a grep
# after show would swallow the "$ ..." line along with the noise).
typed() { echo "${DIM}\$ $*${N}"; }

pause() {
	[[ -t 0 ]] || return 0
	read -r -p "${DIM}  [enter to continue]${N}" _
}

# Machines in the cluster with an NVIDIA card, from the node table.
gpu_nodes() { "$PIPE" nodes 2>/dev/null | tail -n +2 | grep -ci nvidia || true; }

# Healthy peers, not counting this machine.
peer_count() {
	"$PIPE" nodes 2>/dev/null | awk 'NR>1 && $NF != "self" && $3 == "healthy"' | wc -l
}
