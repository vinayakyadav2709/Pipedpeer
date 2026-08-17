#!/usr/bin/env bash
# Failure-injection demo: fan a pool.map out across the 3-node lab, kill a
# worker mid-flight, and prove the run still finishes with correct results
# (the shim falls back to local compute for the lost batch).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="$repo_root/bin/pipedpeer"

if [[ ! -x "$cli" ]]; then
	echo "building binary..."
	"$repo_root/scripts/build.sh"
fi

"$repo_root/scripts/lab-up.sh"
trap '"$repo_root/scripts/lab-down.sh" || true' EXIT

echo "waiting for lab workers..."
for port in 38081 38082 38083; do
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

"$cli" start 2>/dev/null || true
for port in 38081 38082 38083; do
	"$cli" nodes add 127.0.0.1 "$port" >/dev/null 2>&1 || true
done

cat > /tmp/pipedpeer-fail-task.py <<'EOF'
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
"$cli" run --script /tmp/pipedpeer-fail-task.py --intercept > /tmp/pipedpeer-fail-run.log 2>&1 &
runpid=$!

# Wait until some worker is hosting the job, then kill a *different* worker
# so the run itself survives and only a fan-out batch is lost.
kill_port=""
for _ in $(seq 1 90); do
	kill_port=$(python3 - <<'PYEOF'
import json, subprocess, sys

ports = [38081, 38082, 38083]
busy = set()
for port in ports:
    try:
        ns = json.load(subprocess.check_output(
            ["curl", "-sf", f"http://127.0.0.1:{port}/v1/nodes"], timeout=3))
    except Exception:
        continue
    for n in ns:
        if n.get("daemon_port") == port and n.get("load", {}).get("active_jobs", 0) > 0:
            busy.add(port)
if busy:
    for port in ports:
        if port not in busy:
            sys.stdout.write(str(port))
            break
PYEOF
)
	[[ -n "$kill_port" ]] && break
	sleep 2
done

if [[ -z "$kill_port" ]]; then
	echo "warn: never saw the job land on a worker; killing worker 2 blindly"
	kill_port=38082
fi
echo "killing worker on :$kill_port mid-flight"
if command -v podman &>/dev/null; then
	podman stop "pipedpeer-lab-$((kill_port - 38080))" >/dev/null 2>&1 || true
elif command -v docker &>/dev/null; then
	docker stop "pipedpeer-lab-$((kill_port - 38080))" >/dev/null 2>&1 || true
fi

set +e
wait $runpid
run_status=$?
set -e

if [[ $run_status -ne 0 ]]; then
	echo "FAIL: run exited $run_status"
	tail -30 /tmp/pipedpeer-fail-run.log
	exit 1
fi
if ! grep -q "POOL-OK" /tmp/pipedpeer-fail-run.log; then
	echo "FAIL: correct results never printed"
	tail -30 /tmp/pipedpeer-fail-run.log
	exit 1
fi
grep "POOL-OK" /tmp/pipedpeer-fail-run.log
echo "PASS: pool.map completed correctly despite a dead worker"
