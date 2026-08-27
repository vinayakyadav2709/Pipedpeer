#!/usr/bin/env bash
# Does a machine's second GPU earn its rank?
#
# Placement gives a node one rank per accelerator, so a two-GPU box trains on
# both instead of leaving one idle for the whole run. Whether that is actually
# faster is not something the placement can tell you - only running it both
# ways can - and neither machine this was built on has a second usable GPU, so
# the claim has never been tested.
#
# This is the test, ready for the first machine that can run it. It needs one
# node with two or more GPUs; it refuses rather than pretending otherwise,
# because a GPU count of one measures contention on a single device and would
# report the feature as useless when it had simply never been exercised.
#
#   scripts/bench-multigpu.sh                 # against this daemon's cluster
#   BENCH_SCRIPT=train.py scripts/bench-multigpu.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
port="${PIPEDPEER_PORT:-38080}"

# Best-effort: an absent token is a normal answer. Reading it inline used to
# be a pipeline under `set -o pipefail`, so a binary that did not know the
# `auth` command killed the script here - exit 1, zero bytes of output, before
# the first echo.
lib="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cli.sh"
[[ -f "$lib" ]] || { echo "missing $lib - copy scripts/ whole, not this file alone" >&2; exit 2; }
source "$lib"
pp_resolve_cli "$repo_root" || exit 2
cli="$PP_CLI"
pp_read_token

nodes_json="$(pp_curl -sf --max-time 5 "http://127.0.0.1:$port/v1/nodes" 2>/dev/null || true)"
if [[ -z "$nodes_json" ]]; then
	echo "FAIL: no answer from the daemon API on :$port"
	exit 1
fi

# The most GPUs any single healthy node has. Two ranks on two nodes with one
# GPU each is the case that already works and is not what this measures.
gpus="$(printf '%s' "$nodes_json" | python3 -c 'import json,sys
best = 0
for n in json.load(sys.stdin):
    if n.get("state") != "healthy":
        continue
    load = n.get("load") or {}
    count = len(load.get("gpus") or [])
    if not count:
        try:
            count = int((n.get("capabilities") or {}).get("gpu_count") or 0)
        except ValueError:
            count = 0
    best = max(best, count)
print(best)')"

echo "most GPUs on one node: $gpus"
if [[ "$gpus" -lt 2 ]]; then
	echo
	echo "NOT VERIFIED: no node here has two GPUs, so there is nothing to measure."
	echo
	echo "  This is the one claim in the placement work that hardware could not"
	echo "  settle: that a machine's second accelerator earns its rank. Running it"
	echo "  on a single GPU would measure two ranks contending for one device and"
	echo "  report the feature as harmful when it had never been tried."
	echo
	echo "  Run this on a node with two or more GPUs and it will answer in a few"
	echo "  minutes. Nothing else needs setting up."
	exit 3
fi

script="${BENCH_SCRIPT:-$repo_root/pipedpeer-demo/04_torch_ddp.py}"
[[ -f "$script" ]] || { echo "no training script at $script" >&2; exit 2; }

work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench-multigpu"
rm -rf "$work"; mkdir -p "$work"; : > "$work/.pipedpeerignore"
cp "$script" "$work/train.py"

# One rank per run, so the only thing changing is how many accelerators the
# work is spread over.
run() {
	local ranks="$1" log="$work/ranks-$ranks.log"
	local start end
	start="$(date +%s.%N)"
	if (( ranks == 1 )); then
		(cd "$work" && "$cli" run --gpu force train.py) > "$log" 2>&1 ||
			{ echo "FAIL running on 1 rank"; tail -20 "$log"; exit 1; }
	else
		(cd "$work" && "$cli" run --ddp "$ranks" --gpu force train.py) > "$log" 2>&1 ||
			{ echo "FAIL running on $ranks ranks"; tail -20 "$log"; exit 1; }
	fi
	end="$(date +%s.%N)"
	echo "$(echo "$end - $start" | bc)"
}

# Each rank must be on a different device, or this measures two ranks queueing
# on one GPU - which is the failure the pinning exists to prevent, and would
# look here like the second GPU simply not helping.
devices_distinct() {
	local log="$1"
	python3 - "$log" <<'PY'
import re, sys
seen = [m.group(1) for m in re.finditer(r"device (\d+)", open(sys.argv[1], errors="replace").read())]
print("yes" if len(seen) == len(set(seen)) and len(seen) >= 1 else "no")
PY
}

echo
printf '%-14s %10s   %s\n' "ranks" "time" "notes"
one="$(run 1)"
printf '%-14s %9.1fs\n' "1 (one GPU)" "$one"

two="$(run 2)"
printf '%-14s %9.1fs   distinct devices: %s\n' "2 (two GPUs)" "$two" \
	"$(devices_distinct "$work/ranks-2.log")"

echo
python3 - "$one" "$two" <<'PY'
import sys
one, two = float(sys.argv[1]), float(sys.argv[2])
speedup = one / two if two > 0 else 0
print("speedup on the second GPU: %.2fx" % speedup)
# Two devices cannot beat two, and gradient sync costs something every step, so
# anything near 2x is the ceiling. Below 1.0 the second rank is making the run
# slower, which is the answer the placement rule needs to hear.
if speedup < 1.0:
    print()
    print("The second GPU made the run SLOWER. Placement gives a node one rank")
    print("per accelerator on the assumption that it would not; that assumption")
    print("is wrong on this hardware and the rule needs revisiting.")
    raise SystemExit(1)
print("The second accelerator earned its rank.")
PY
echo
echo "logs in $work"
