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
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="$repo_root/bin/pipedpeer"
ports=(38081 38082 38083)

if [[ ! -x "$cli" ]]; then
	echo "building binary..."
	"$repo_root/scripts/build.sh"
fi

if command -v podman &>/dev/null; then
	runtime=podman
elif command -v docker &>/dev/null; then
	runtime=docker
else
	echo "FAIL: no container runtime (podman/docker)"
	exit 1
fi

# Once a token is configured, an unauthenticated probe gets 401 and `curl -sf`
# reports that as a plain failure - which reads as "the daemon is down" when
# it is answering perfectly well. Anything talking to this host's daemon has
# to carry the token; the lab workers are fresh containers with none.
pp_token="$("$cli" auth show 2>/dev/null | head -1)"
case "$pp_token" in
	*"no token set"*) pp_token="" ;;
esac
pp_curl() {
	if [[ -n "$pp_token" ]]; then
		curl -H "X-Pipedpeer-Token: $pp_token" "$@"
	else
		curl "$@"
	fi
}

# Each worker reports its own tally of pool work. Asking the daemon beats
# grepping its log: the log lives inside the container at a path that depends
# on how the daemon was started, and an assertion that silently finds nothing
# is worse than no assertion at all.
chunks_received() {
	pp_curl -sf --max-time 3 "http://127.0.0.1:$((38080 + $1))/v1/pool/stats" 2>/dev/null |
		python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("chunks_received", 0))
except Exception: print(0)' 2>/dev/null || echo 0
}

"$repo_root/scripts/lab-up.sh"
trap '"$repo_root/scripts/lab-down.sh" || true' EXIT

echo "waiting for lab workers..."
for port in "${ports[@]}"; do
	ok=0
	for _ in $(seq 1 60); do
		if curl -sf "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
			ok=1
			break
		fi
		sleep 1
	done
	if [[ $ok -ne 1 ]]; then
		echo "FAIL: worker on :$port never came up"
		exit 1
	fi
done
echo "lab is up"

# The node IDs of the workers that exist right now. Containers are recreated
# on every lab-up, so yesterday's rows are still in the store under different
# IDs; counting "healthy" lines would happily match those.
want_ids=()
for port in "${ports[@]}"; do
	id="$(curl -sf --max-time 3 "http://127.0.0.1:$port/health" |
		python3 -c 'import json,sys; print(json.load(sys.stdin)["node_id"])' 2>/dev/null || true)"
	[[ -n "$id" ]] && want_ids+=("$id")
done
if (( ${#want_ids[@]} == 0 )); then
	echo "FAIL: no lab worker reported a node id"
	exit 1
fi

# An already-running daemon on this port makes `start` a silent no-op, and the
# run is then served by whatever was there - typically an older build pointing
# at containers that no longer exist. `stop` only finds daemons started with
# the same state dir, so do not rely on it.
"$cli" stop >/dev/null 2>&1 || true
fuser -k 38080/tcp >/dev/null 2>&1 || true
sleep 2
"$cli" start 2>/dev/null || true
up=0
for _ in $(seq 1 30); do
	if pp_curl -sf --max-time 3 "http://127.0.0.1:38080/health" >/dev/null 2>&1; then
		up=1
		break
	fi
	sleep 1
done
if [[ $up -ne 1 ]]; then
	echo "FAIL: no daemon answering on :38080"
	exit 1
fi

for port in "${ports[@]}"; do
	"$cli" nodes add 127.0.0.1 "$port" >/dev/null 2>&1 || true
done

# Wait until the daemon reports these exact workers healthy. /v1/nodes serves
# state persisted from earlier runs, so a count is not evidence: a job started
# against a stale view finds no peers holding the closure and runs entirely
# local, with nothing in the output to say why.
ready=0
for _ in $(seq 1 40); do
	nodes_json="$(pp_curl -sf --max-time 3 "http://127.0.0.1:38080/v1/nodes" || echo '[]')"
	ready="$(printf '%s' "$nodes_json" | python3 -c 'import json,sys
want = set(sys.argv[1:])
try: nodes = json.load(sys.stdin)
except Exception: nodes = []
print(sum(1 for n in nodes if n.get("node_id") in want and n.get("state") == "healthy"))' "${want_ids[@]}" 2>/dev/null || echo 0)"
	[[ "${ready:-0}" -ge "${#want_ids[@]}" ]] && break
	sleep 1
done
if [[ "${ready:-0}" -lt "${#want_ids[@]}" ]]; then
	echo "FAIL: daemon sees $ready of ${#want_ids[@]} lab workers as healthy"
	"$cli" nodes || true
	exit 1
fi
echo "daemon sees all ${#want_ids[@]} lab workers"

# Not $TMPDIR: on a tmpfs /tmp this workspace competes with the job's own
# data for RAM, and the daemon has already had uploads rejected that way.
state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$state_dir"
workdir="$(mktemp -d "$state_dir/lab-fail.XXXXXX")"
# Keep the lab teardown from the earlier trap; a second bare trap replaces it.
trap 'rm -rf "$workdir"; "$repo_root/scripts/lab-down.sh" || true' EXIT
task="$workdir/task.py"
runlog="$workdir/run.log"
receipt="$workdir/receipt.json"
# Anchor the workspace here so the job ships this directory and not all of
# its parent (findProjectRoot walks up looking for .pipedpeerignore or .git).
: > "$workdir/.pipedpeerignore"

cat > "$task" <<'EOF'
import time
from multiprocessing import Pool

def work(i):
    time.sleep(2 + (i % 3) * 2)
    return i * i

if __name__ == "__main__":
    with Pool(8) as p:
        out = p.map(work, range(20))
    assert out == [i * i for i in range(20)], out
    print("POOL-OK sum=%d" % sum(out))
EOF

echo "launching intercept run..."
"$cli" run "$task" -e PIPEDPEER_RECEIPT=receipt.json > "$runlog" 2>&1 &
runpid=$!

# Kill a worker that is actually holding chunks. Killing an idle one, which is
# what this did before, removes nothing from the run and proves nothing about
# recovering from a loss.
kill_idx=""
for _ in $(seq 1 90); do
	for idx in 1 2 3; do
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

# Snapshot the tallies before the kill: a stopped worker cannot answer for
# what it did, and reading them afterwards reports zero for the one node we
# know did the work.
counts=()
for idx in 1 2 3; do
	counts+=("$(chunks_received "$idx")")
done

echo "killing worker $kill_idx (:$((38080 + kill_idx))) while it holds chunks"
"$runtime" stop "pipedpeer-lab-$kill_idx" >/dev/null 2>&1 || true

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

if [[ ! -f "$receipt" ]]; then
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

echo
grep "POOL-OK" "$runlog"
echo "receipt: $remote_items items ran on the cluster, $local_items locally, $remote_failures chunk(s) lost"
for idx in 1 2 3; do
	note=""
	[[ "$idx" == "$kill_idx" ]] && note=" (killed mid-flight)"
	echo "  worker $idx: ${counts[$((idx - 1))]} chunk request(s) at kill time$note"
done
echo "PASS: pool.map distributed real work and completed correctly despite losing a worker mid-flight"
