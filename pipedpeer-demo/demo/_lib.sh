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

# Counts from the daemon's own /v1/nodes JSON, not the printed table (its
# columns change; its GPU cell drops the word NVIDIA). GPU = the same test the
# scheduler uses (main.go ddpHasGPU). Healthy peers only; this machine always.
#   node_count peers      healthy peers, not this machine
#   node_count gpu-peers  healthy peers with a GPU (what --remote can reach)
#   node_count gpu-all    healthy machines with a GPU, this one included
#   node_count gpu-self   1 if this machine has a GPU
DAEMON_PORT="${PIPEDPEER_DAEMON_PORT:-38080}"
node_count() {
	# The API is token-authenticated once a cluster secret is set.
	PP_TOKEN="$("$PIPE" auth show 2>/dev/null)" python3 - "$DAEMON_PORT" "$1" <<'PY'
import json, os, re, sys, urllib.request
port, what = sys.argv[1], sys.argv[2]
req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/nodes")
tok = os.environ.get("PP_TOKEN", "").strip()
if re.fullmatch(r"[0-9a-fA-F]{16,}", tok):
    req.add_header("X-Pipedpeer-Token", tok)
try:
    with urllib.request.urlopen(req, timeout=3) as r:
        nodes = json.load(r)
except Exception as e:
    print(f"  (counting nodes: daemon on port {port} not answering: {e}; counted 0)", file=sys.stderr)
    print(0)
    sys.exit(0)
def gpu(n):
    c, l = n.get("capabilities") or {}, n.get("load") or {}
    return c.get("gpu", "none") not in ("", "none") or bool(l.get("gpu_model")) or bool(l.get("gpus"))
up = [n for n in nodes or [] if n.get("state") == "healthy" or n.get("source") == "self"]
peers = [n for n in up if n.get("source") != "self"]
pick = {
    "peers": peers,
    "gpu-peers": [n for n in peers if gpu(n)],
    "gpu-all": [n for n in up if gpu(n)],
    "gpu-self": [n for n in up if n.get("source") == "self" and gpu(n)],
}[what]
print(len(pick))
PY
}

gpu_nodes() { node_count gpu-all; }
peer_count() { node_count peers; }
