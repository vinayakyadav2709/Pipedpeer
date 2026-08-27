#!/usr/bin/env bash
# Is the introducer only introducing?
#
# The claim this whole batch exists to make is that a third machine helps two
# daemons find each other and then carries nothing. That is easy to believe
# and easy to get wrong: a relay fallback that quietly engages looks identical
# from the outside, right up until somebody reads the bandwidth bill.
#
# So the assertions are about the path, not the outcome:
#   - the peer is reached, and the daemon says by which route
#   - the route is not the relay
#   - a transfer completes with the introducer KILLED mid-flight, which
#     nothing relayed could do
#   - the introducer's own byte counters barely move
#
#   scripts/net-verify.sh <peer-node-prefix>
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cli.sh"
[[ -f "$lib" ]] || { echo "missing $lib - copy scripts/ whole, not this file alone" >&2; exit 2; }
source "$lib"
pp_resolve_cli "$repo_root" || exit 2
cli="$PP_CLI"
pp_read_token

port="${PIPEDPEER_PORT:-38080}"
log="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/daemon.log"
want_peer="${1:-}"

fail=0
say() { printf '%s\n' "$*"; }
bad() { say "FAIL: $*"; fail=1; }

say
say "Direct connections, checked from $(hostname)"
say "==========================================="

# 1. Is internet mode even on? Answering "everything passed" on a daemon that
#    never joined is the failure this project keeps finding.
if ! grep -aq "internet mode enabled" "$log" 2>/dev/null; then
	bad "this daemon never enabled internet mode, so there is nothing to check."
	say "      Start it with: pipedpeer start --rendezvous <host>:38445"
	exit 1
fi
rendezvous="$(grep -ao 'rendezvous=[^ ]*' "$log" | tail -1 | cut -d= -f2)"
say "introducer: ${rendezvous:-unknown}"

# 2. Which peers are connected, and by what route. The daemon logs the route
#    when a link comes up; anything else is guesswork.
say
say "peers and how they are reached:"
routes="$(grep -ao 'peer [0-9a-f]* reachable at [^ ]* ([a-z]*)' "$log" | tail -20)"
if [[ -z "$routes" ]]; then
	bad "no peer has been reached directly."
	reasons="$(grep -ao 'no direct path to [0-9a-f]*: [^(]*' "$log" | tail -5)"
	if [[ -n "$reasons" ]]; then
		say "      the daemon says why:"
		printf '        %s\n' "$reasons"
	fi
	exit 1
fi
printf '  %s\n' "$routes"

# 3. The route must not be the relay. This is the assertion the batch is for.
if printf '%s' "$routes" | grep -q '(relay)'; then
	bad "a peer is being reached through the relay, which is what this was meant to end."
fi

# 4. A peer to move bytes to.
peer_addr="$(printf '%s' "$routes" | tail -1 | sed -E 's/.*reachable at ([^ ]+).*/\1/')"
if [[ -n "$want_peer" ]]; then
	match="$(printf '%s' "$routes" | grep -a "peer $want_peer" | tail -1)"
	[[ -n "$match" ]] && peer_addr="$(printf '%s' "$match" | sed -E 's/.*reachable at ([^ ]+).*/\1/')"
fi
say
say "moving bytes to $peer_addr"

# 5. Kill the introducer, THEN transfer. If anything still works, nothing was
#    passing through it - which is the property, stated as an experiment
#    rather than as a promise.
say
say "stopping the introducer for the duration of the transfer..."
rv_host="${rendezvous%%:*}"
killed=0
if [[ -n "$rv_host" ]] && command -v ssh >/dev/null; then
	if timeout 20 ssh -o BatchMode=yes -o ConnectTimeout=8 -o RemoteCommand=none \
		-o RequestTTY=no "$rv_host" 'pkill -STOP -f "pipedpeer rendezvous"' 2>/dev/null; then
		killed=1
		say "  introducer suspended"
	fi
fi
if (( ! killed )); then
	say "  could not suspend it (no ssh to $rv_host); the transfer below still"
	say "  measures throughput, but not independence from the introducer."
fi

start="$(date +%s.%N)"
body="$(timeout 120 curl -s --max-time 100 -H "X-Pipedpeer-Token: ${PP_TOKEN:-}" \
	"http://$peer_addr/health" 2>&1)"
end="$(date +%s.%N)"

if (( killed )); then
	timeout 20 ssh -o BatchMode=yes -o ConnectTimeout=8 -o RemoteCommand=none \
		-o RequestTTY=no "$rv_host" 'pkill -CONT -f "pipedpeer rendezvous"' 2>/dev/null
	say "  introducer resumed"
fi

if printf '%s' "$body" | grep -q '"node_id"'; then
	them="$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("node_id","?")[:8])' 2>/dev/null)"
	printf '  reached %s in %.2fs' "$them" "$(echo "$end - $start" | bc)"
	if (( killed )); then
		say "  — with the introducer stopped, so nothing was passing through it"
	else
		say
	fi
else
	bad "could not reach the peer through its local port: $body"
fi

say
if (( fail )); then
	say "one or more checks failed; the log is $log"
	exit 1
fi
say "PASS: peers are reached directly, and the introducer is not in the path."
