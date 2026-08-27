#!/usr/bin/env bash
# Does the ring notice a machine that slows down while it is running?
#
# Placement measures once, before the job starts. A laptop throttles, another
# job lands, someone closes the lid - and the shares chosen at the start stop
# matching what the machines can do. Every rank reports its step time to the
# lead daemon anyway, for its own sync tuning, so the ring has what it needs
# to refit itself; this checks that it does.
#
# One rank is throttled part-way through with `docker update --cpus`, which is
# the cheapest honest way to make a machine slower without touching what it is
# doing.
#
#   scripts/test-rebalance.sh              # needs the lab from lab-hetero
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cli="${PIPEDPEER:-$repo_root/bin/pipedpeer}"
victim="${VICTIM:-pp-w8}"
throttle_to="${THROTTLE_TO:-1}"
epochs="${EPOCHS:-8}"

[[ -x "$cli" ]] || { echo "no binary at $cli" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker is needed to throttle a worker" >&2; exit 2; }
docker inspect "$victim" >/dev/null 2>&1 || {
	echo "FAIL: no container named $victim to throttle. This needs the uneven"
	echo "      lab: two workers with different CPU quotas." >&2
	exit 1
}

original="$(docker inspect "$victim" --format '{{.HostConfig.NanoCpus}}')"
restore() {
	docker update --cpus="$(python3 -c "print(max(1,int($original)/1e9))")" "$victim" >/dev/null 2>&1 || true
}
trap restore EXIT

driver="$here/bench-ddp-hetero.py"
[[ -f "$driver" ]] || driver="$repo_root/scripts/bench-ddp-hetero.py"
work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/test-rebalance"
rm -rf "$work"; mkdir -p "$work"; : > "$work/.pipedpeerignore"
cp "$driver" "$work/train.py"

daemon_log="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/daemon.log"
mark=0
[[ -f "$daemon_log" ]] && mark="$(wc -c < "$daemon_log")"

log="$work/run.log"
daemon_since() { tail -c "+$((mark + 1))" "$daemon_log" 2>/dev/null; }
reshares() { daemon_since | grep -c "reshared" || true; }

# Waits on what the daemon says rather than on the clock. Fixed sleeps made
# this test depend on how fast the machine happened to be that minute: the run
# finished before the throttle had been in place long enough to be noticed,
# and the test reported a mechanism that works as one that does not.
await() {
	local what="$1" want="$2" limit="$3" i
	for ((i = 0; i < limit; i++)); do
		[[ "$(reshares)" -ge "$want" ]] && return 0
		kill -0 "$run_pid" 2>/dev/null || return 1
		sleep 2
	done
	return 1
}

echo "starting $epochs epochs across the ring ..."
(cd "$work" && PIPEDPEER_BENCH_EPOCHS="$epochs" "$cli" run --ddp 3 --gpu off train.py) \
	> "$log" 2>&1 &
run_pid=$!

if ! await "the ring to settle on its first shares" 1 180; then
	echo "FAIL: the ring never settled on measured shares before the run ended"
	tail -20 "$log"
	kill "$run_pid" 2>/dev/null
	exit 1
fi
first="$(daemon_since | grep "reshared" | tail -1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("shares",""))' 2>/dev/null)"
echo "settled at: $first"

echo "throttling $victim to $throttle_to cpu ..."
docker update --cpus="$throttle_to" "$victim" >/dev/null
if ! await "the ring to notice the throttle" 2 180; then
	echo "FAIL: $victim was cut to $throttle_to cpu and the ring never refitted."
	echo "      Placement measured once, before the job started, and nothing"
	echo "      revisited it - which is the whole point of collecting step times."
	daemon_since | grep -a "ring measured\|reshared" | tail -3
	kill "$run_pid" 2>/dev/null
	restore
	exit 1
fi
throttled="$(daemon_since | grep "reshared" | tail -1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("shares",""))' 2>/dev/null)"
echo "after the throttle: $throttled"

# Adapting down is half the job. A machine that recovers must be given work
# again, and a cumulative average never would - it stays weighed down by the
# slow period for the rest of the run.
echo "restoring $victim ..."
restore
if ! await "the ring to notice the recovery" 3 180; then
	echo "FAIL: $victim recovered and the ring never gave it work back."
	echo "      Adapting down and not up leaves a machine idle for the rest of"
	echo "      the run over a slowdown that has passed."
	kill "$run_pid" 2>/dev/null
	exit 1
fi
recovered="$(daemon_since | grep "reshared" | tail -1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("shares",""))' 2>/dev/null)"
echo "after recovery:     $recovered"

kill "$run_pid" 2>/dev/null
wait "$run_pid" 2>/dev/null
restore

echo
echo "what the daemon decided while it ran:"
tail -c "+$((mark + 1))" "$daemon_log" 2>/dev/null | python3 -c '
import json, sys
said = []
for line in sys.stdin:
    try:
        rec = json.loads(line)
    except Exception:
        continue
    msg = rec.get("message", "")
    if "ring measured" in msg:
        said.append(("measured", rec.get("ranks", "")))
    elif "reshared" in msg:
        said.append(("reshared", rec.get("shares", "")))
    elif "setting the pace" in msg or "faster without it" in msg:
        said.append(("verdict", msg))

for kind, detail in said:
    print("  %-9s %s" % (kind, detail))

reshares = [d for k, d in said if k == "reshared"]
if len(reshares) >= 2:
    print()
    print("adapted down and back up: %s -> %s" % (reshares[0], reshares[-1]))
if not reshares:
    print()
    print("FAIL: one rank was cut to a fraction of its speed part-way through")
    print("      and the ring never refitted its shares. Placement measured")
    print("      once, before the job started, and nothing revisited it - which")
    print("      is the whole point of collecting step times.")
    raise SystemExit(1)
print()
print("the ring refitted itself %d time(s) while running" % len(reshares))
'
rc=$?
echo
echo "logs in $work"
exit $rc
