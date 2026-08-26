#!/usr/bin/env bash
# What it costs to give a peer an environment it does not have.
#
# Three cases, and the difference between them is the whole point of the
# closure cache:
#
#   cold  the peer has nothing for this closure - the full set of store paths
#   warm  the peer already has it - nothing should cross at all
#   diff  the peer has a related environment - only the paths it lacks
#
# The third is what makes a second job on a cluster cheap, and it is the one
# that silently stopped working once before: the diff never fired because the
# relaying node had not imported the closure itself, and nothing said so.
#
#   scripts/bench-nar.sh              # against whatever peers this daemon has
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="${PIPEDPEER:-$repo_root/bin/pipedpeer}"
port="${PIPEDPEER_PORT:-38080}"

[[ -x "$cli" ]] || { echo "no binary at $cli" >&2; exit 2; }

tok="$("$cli" auth show 2>/dev/null | head -1)"
case "$tok" in *"no token set"*) tok="" ;; esac
pp_curl() {
	if [[ -n "$tok" ]]; then curl -H "X-Pipedpeer-Token: $tok" "$@"; else curl "$@"; fi
}

peers="$(pp_curl -sf --max-time 5 "http://127.0.0.1:$port/v1/nodes" 2>/dev/null |
	python3 -c 'import json,sys
try: nodes = json.load(sys.stdin)
except Exception: nodes = []
print(sum(1 for n in nodes if n.get("state") == "healthy" and n.get("source") != "self"))' 2>/dev/null || echo 0)"
if [[ "${peers:-0}" -lt 1 ]]; then
	echo "FAIL: no healthy peer. This measures what crosses to a peer, so with none"
	echo "      it would measure nothing and report success - which is how the diff"
	echo "      stopped firing unnoticed in the first place."
	exit 1
fi
echo "peers: $peers"

work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench-nar"
rm -rf "$work"; mkdir -p "$work"; : > "$work/.pipedpeerignore"

# Two environments that share most of their closure. The second adds one
# package, so a diff should send a handful of paths rather than the set.
cat > "$work/a.py" <<'PY'
import numpy
print("A-OK", numpy.__version__)
PY
cat > "$work/b.py" <<'PY'
import numpy, pandas
print("B-OK", numpy.__version__, pandas.__version__)
PY

# What crossed is read from the submitting side, which is the only place that
# knows. The daemon that logs the decision is whichever one *receives* the
# closure - with --remote that is the peer, whose log is on another machine.
# Reading the local daemon log instead reported decisions from an hour before,
# which is how a working cache came to look broken here.
decision_from() {
	local log="$1" line
	if line="$(grep -ao "Sent [0-9]* of [0-9]* store paths" "$log" | tail -1)" && [[ -n "$line" ]]; then
		echo "diff $(echo "$line" | awk '{print $2"/"$4}') paths"
	elif grep -aq "Closure already on the node" "$log"; then
		echo "nothing to send"
	elif grep -aq "Sending the whole closure" "$log"; then
		echo "full closure"
	else
		echo "unknown"
	fi
}

# The node this daemon is. Every job has to land somewhere else, or nothing
# crosses the wire and there is nothing here to measure.
self_id="$(pp_curl -sf --max-time 5 "http://127.0.0.1:$port/health" 2>/dev/null |
	python3 -c 'import json,sys; print(json.load(sys.stdin).get("node_id",""))' 2>/dev/null || true)"
[[ -n "$self_id" ]] || { echo "FAIL: daemon on :$port did not report its node id"; exit 1; }

# ran_elsewhere reads the job's own receipt rather than trusting the run to
# have gone remote. Without --remote every one of these ran on this machine
# and the benchmark printed four timings that measured nothing: the peer was
# healthy, so the peer check passed, and the scheduler quite reasonably kept
# 16 local cores rather than shipping a closure to a 4-core container. A
# benchmark that cannot tell those apart is worse than no benchmark.
ran_elsewhere() {
	local log="$1"
	local hist
	hist="$(grep -aoE 'History: [^ ]+' "$log" | tail -1 | cut -d" " -f2)"
	[[ -n "$hist" && -f "$hist/metadata.json" ]] || { echo "no receipt"; return; }
	python3 -c 'import json,sys
d = json.load(open(sys.argv[1]))
target = d.get("target_id") or ""
print("self" if target == sys.argv[2] else (target[:8] or "unknown"))' "$hist/metadata.json" "$self_id"
}

declare -A decision

run() {
	local script="$1" label="$2"
	local log="$work/$label.log"
	local start end where
	start="$(date +%s.%N)"
	(cd "$work" && "$cli" run --remote "$script") > "$log" 2>&1 || { echo "FAIL running $script"; tail -20 "$log"; exit 1; }
	end="$(date +%s.%N)"
	decision[$label]="$(decision_from "$log")"
	where="$(ran_elsewhere "$log")"
	if [[ "$where" == "self" || "$where" == "no receipt" || "$where" == "unknown" ]]; then
		echo
		echo "FAIL: $label ran on this machine ($where), so nothing crossed to a peer."
		echo "      The timing below would be a local build time dressed up as a"
		echo "      transfer measurement."
		exit 1
	fi
	printf '%-28s %8.1fs   %-18s %s\n' "$label" "$(echo "$end - $start" | bc)" \
		"${decision[$label]}" "$where"
}

printf '\n%-28s %9s   %-18s %s\n' "case" "time" "sent" "ran on"
run a.py "cold: numpy, first time"
run a.py "warm: numpy again"
run b.py "diff: numpy + pandas"
run b.py "warm: numpy + pandas again"

# The numbers above are only worth anything if the cache did its job, and
# "it looked plausible" is what let the diff stop firing unnoticed before.
fail=0
check() {
	local label="$1" want="$2" got="${decision[$1]}"
	if [[ "$got" != $want ]]; then
		echo "FAIL: $label sent \"$got\", expected $want"
		fail=1
	fi
}
echo
if [[ "${decision["cold: numpy, first time"]}" != "full closure" ]]; then
	echo "FAIL: the cold case sent \"${decision["cold: numpy, first time"]}\", not the whole closure."
	echo "      The peer already had this environment, so there was no cold"
	echo "      transfer to measure. Give it a peer that has never seen it -"
	echo "      a fresh container, or one whose store has been cleared."
	fail=1
fi
check "warm: numpy again"             "nothing to send"
check "diff: numpy + pandas"          "diff*"
check "warm: numpy + pandas again"    "nothing to send"

# The diff has to be a real saving, not one path short of the whole closure.
d="${decision["diff: numpy + pandas"]}"
if [[ "$d" == diff* ]]; then
	sent="${d#diff }"; sent="${sent%%/*}"
	total="${d#*/}"; total="${total%% *}"
	if (( sent * 2 >= total )); then
		echo "FAIL: the diff sent $sent of $total paths, which is not a cache doing its job"
		fail=1
	fi
fi

if (( fail )); then
	echo
	echo "logs in $work"
	exit 1
fi
echo "cache behaved: warm runs sent nothing, the shared closure was not resent"
echo
echo "logs in $work"
