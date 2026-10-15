#!/usr/bin/env bash
# Failure injection: fan a pool.map across the 3-node lab, kill a worker that
# is actively taking chunks, and prove the run still finishes with correct
# results.
#
# This script used to prove nothing. Its kernel was defined in the script's
# own __main__, which the old by-reference dispatch could never ship, so every
# chunk failed and the local pool quietly did all the work; its only
# assertions were "exit 0" and "POOL-OK appeared", both of which local
# fallback satisfies for free; and it deliberately killed a worker that was
# *not* involved in the run. It therefore passed identically whether the
# cluster did all of the work, some of it, or none of it.
#
# So the assertions now cover the thing being demonstrated: the shim's receipt
# must show items that actually executed off this process, and the kill must
# land on a worker that was holding chunks.
#
# Worker 1 submits; workers 2 and 3 take chunks and one of them is killed.
# Everything runs inside the lab's own bridge, so this machine's daemon and
# token play no part.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for sibling in lab-up.sh lab-down.sh; do
	[[ -f "$repo_root/scripts/$sibling" ]] || {
		echo "lab-fail needs $sibling beside it: it brings up the cluster it" >&2
		echo "then breaks, rather than using one that is already running." >&2
		echo "Copy the whole scripts/ directory, not this file alone." >&2
		exit 2
	}
done

source "$repo_root/scripts/lib/runtime.sh"
pp_pick_runtime || exit 1
runtime="$PP_RUNTIME"

w() { "$runtime" exec "pipedpeer-lab-$1" sh -c "$2"; }
api() { w "$1" "curl -sf --max-time 3 -H \"X-Pipedpeer-Token: \$LAB_TOKEN\" http://127.0.0.1:4242$2"; }

# How many chunk requests a worker has served, read inside its own container,
# so there is no doubt which daemon answered.
chunks_received() {
	api "$1" /v1/pool/stats 2>/dev/null |
		python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("chunks_received", 0))
except Exception: print(0)' 2>/dev/null || echo 0
}

"$repo_root/scripts/lab-up.sh"
trap '"$repo_root/scripts/lab-down.sh" || true' EXIT

echo "waiting for lab workers..."
for idx in 1 2 3; do
	ok=0
	for _ in $(seq 1 90); do
		if w "$idx" 'curl -sf --max-time 2 http://127.0.0.1:4242/health' >/dev/null 2>&1; then
			ok=1
			break
		fi
		sleep 1
	done
	if [[ $ok -ne 1 ]]; then
		echo "FAIL: worker $idx never came up"
		"$runtime" logs "pipedpeer-lab-$idx" 2>&1 | tail -20
		exit 1
	fi
done
echo "lab is up"

# Worker 1 must see both others healthy before the run, or "distributed"
# below would be measuring discovery, not failover.
want_ids=()
for idx in 2 3; do
	id="$(w "$idx" 'curl -sf --max-time 3 http://127.0.0.1:4242/health' |
		python3 -c 'import json,sys; print(json.load(sys.stdin)["node_id"])' 2>/dev/null || true)"
	[[ -n "$id" ]] && want_ids+=("$id")
done
if ((${#want_ids[@]} < 2)); then
	echo "FAIL: workers 2 and 3 did not both report a node id"
	exit 1
fi
ready=0
for _ in $(seq 1 60); do
	ready="$(api 1 /v1/nodes 2>/dev/null | python3 -c 'import json,sys
want = set(sys.argv[1:])
try: nodes = json.load(sys.stdin)
except Exception: nodes = []
print(sum(1 for n in nodes if n.get("node_id") in want and n.get("state") == "healthy"))' "${want_ids[@]}" 2>/dev/null || echo 0)"
	[[ "${ready:-0}" -ge 2 ]] && break
	sleep 1
done
if [[ "${ready:-0}" -lt 2 ]]; then
	echo "FAIL: worker 1 sees $ready of 2 lab workers as healthy"
	api 1 /v1/nodes || true
	exit 1
fi
echo "worker 1 sees both lab workers"

state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$state_dir"
workdir="$(mktemp -d "$state_dir/lab-fail.XXXXXX")"
trap 'rm -rf "$workdir"; "$repo_root/scripts/lab-down.sh" || true' EXIT
runlog="$workdir/run.log"
receipt="$workdir/receipt.json"

cat >"$workdir/task.py" <<'PYEOF'
import os
import time
from multiprocessing import Pool

def work(i):
    time.sleep(2 + (i % 3) * 2)
    return i * i

if __name__ == "__main__":
    n = int(os.environ.get("LAB_ITEMS", "80"))
    with Pool(8) as p:
        out = p.map(work, range(n))
    assert out == [i * i for i in range(n)], out
    print("POOL-OK sum=%d" % sum(out))
PYEOF
: >"$workdir/.pipedpeerignore"
w 1 'rm -rf /root/labfail && mkdir -p /root/labfail'
"$runtime" cp "$workdir/task.py" pipedpeer-lab-1:/root/labfail/task.py
"$runtime" cp "$workdir/.pipedpeerignore" pipedpeer-lab-1:/root/labfail/.pipedpeerignore

echo "launching intercept run on worker 1..."
w 1 "cd /root/labfail && pipedpeer run -e PIPEDPEER_RECEIPT=receipt.json -e LAB_ITEMS=${LAB_ITEMS:-80} task.py" >"$runlog" 2>&1 &
runpid=$!

# Wait until a remote worker is demonstrably holding chunks, then kill that
# one: a kill that lands on an idle worker proves nothing about failover.
kill_idx=""
for _ in $(seq 1 90); do
	for idx in 2 3; do
		if [[ "$(chunks_received "$idx")" -gt 0 ]]; then
			kill_idx="$idx"
			break
		fi
	done
	[[ -n "$kill_idx" ]] && break
	sleep 2
done

if [[ -z "$kill_idx" ]]; then
	echo "FAIL: no worker ever received a pool chunk, so there was nothing to lose."
	echo "      The run was never distributed; a dead-worker demo on top of that is meaningless."
	kill $runpid 2>/dev/null || true
	tail -30 "$runlog"
	exit 1
fi

# Let the other worker get work too before the kill.
sleep "${LAB_SETTLE_SECS:-6}"

counts=()
for idx in 2 3; do
	counts+=("$(chunks_received "$idx")")
done

echo "killing worker $kill_idx while it holds chunks"
"$runtime" stop "pipedpeer-lab-$kill_idx" >/dev/null 2>&1 || echo "warn: stopping worker $kill_idx failed" >&2

set +e
wait $runpid
run_status=$?
set -e

if [[ $run_status -ne 0 ]]; then
	echo "FAIL: run exited $run_status"
	tail -30 "$runlog"
	exit 1
fi
if ! grep -q "POOL-OK" "$runlog"; then
	echo "FAIL: correct results never printed"
	tail -30 "$runlog"
	exit 1
fi

if ! w 1 'cat /root/labfail/receipt.json' >"$receipt" 2>/dev/null; then
	echo "FAIL: no receipt came back; cannot tell whether anything was distributed"
	tail -30 "$runlog"
	exit 1
fi

read -r remote_items remote_failures local_items < <(python3 - "$receipt" <<'PYEOF'
import json, sys
r = json.load(open(sys.argv[1]))
print(r.get("remote_items", 0), r.get("remote_failures", 0), r.get("local_items", 0))
PYEOF
)

if [[ "$remote_items" -eq 0 ]]; then
	echo "FAIL: receipt shows no work executed off this process (local=$local_items)."
	echo "      Correct results here mean only that local fallback covered everything."
	cat "$receipt"
	exit 1
fi

workers_used=0
for c in "${counts[@]}"; do
	[[ "$c" -gt 0 ]] && workers_used=$((workers_used + 1))
done
if [[ "$workers_used" -lt 2 ]]; then
	echo "FAIL: only $workers_used remote worker received chunks before the kill."
	echo "      The run distributed, but to one node - a fan-out that is not fanning"
	echo "      out, which reads identically to a healthy cluster in the receipt."
	echo "        worker 2: ${counts[0]} chunk request(s), worker 3: ${counts[1]}"
	exit 1
fi

echo
grep "POOL-OK" "$runlog"
echo "receipt: $remote_items items ran on the cluster, $local_items locally, $remote_failures chunk(s) lost"
for idx in 2 3; do
	note=""
	[[ "$idx" == "$kill_idx" ]] && note=" (killed mid-flight)"
	echo "  worker $idx: ${counts[$((idx - 2))]} chunk request(s) at kill time$note"
done
echo "PASS: pool.map distributed real work and completed correctly despite losing a worker mid-flight"
