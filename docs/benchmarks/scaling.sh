#!/usr/bin/env bash
# Speedup measurement on one host, with each daemon pinned to its own physical
# cores so that distributing work genuinely adds compute.
#
#   node A  cores 0-1    the node the job runs on, deliberately the weakest
#   node B  cores 4-7
#   node C  cores 8-11
#
# Baseline is the same job on node A with interception disabled, so it uses
# A's two cores only. The comparison run lets A spill to B and C, giving ten
# cores. Everything is on loopback: see RESULTS.md for what that does and does
# not affect.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cli="$root/bin/pipedpeer"
raw="$root/docs/benchmarks/raw"
mkdir -p "$raw"

start_node() {  # name port cpus
  local name=$1 port=$2 cpus=$3
  rm -rf "/tmp/pp-$name"; mkdir -p "/tmp/pp-$name"
  XDG_DATA_HOME="/tmp/pp-$name" PIPEDPEER_DAEMON_STATE_DIR="/tmp/pp-$name/state" \
    taskset -c "$cpus" "$cli" __daemon__ --port "$port" >"/tmp/pp-$name/daemon.log" 2>&1 &
  echo "  node $name on :$port pinned to cpus $cpus"
}

stop_all() {
  for n in a b c; do
    [ -f "/tmp/pp-$n/state/daemon.pid" ] && kill "$(cat /tmp/pp-$n/state/daemon.pid)" 2>/dev/null
  done
  pkill -f 'bin/pipedpeer __daemon__' 2>/dev/null
  sleep 1
}
trap stop_all EXIT

echo "starting pinned daemons..."
stop_all
start_node a 38081 0-1
start_node b 38082 4-7
start_node c 38083 8-11

for p in 38081 38082 38083; do
  for _ in $(seq 1 40); do
    curl -sf "http://127.0.0.1:$p/health" >/dev/null 2>&1 && break
    sleep 0.5
  done
done
echo "all three healthy"

# A needs to know B and C, and they need to know each other for the closure
# broadcast to reach them.
for p in 38081 38082 38083; do
  for q in 38081 38082 38083; do
    [ "$p" = "$q" ] || "$cli" nodes add --port "$p" 127.0.0.1 "$q" >/dev/null 2>&1
  done
done
sleep 12
"$cli" nodes list --port 38081 | head -6

ws="$raw/ws-scaling"; rm -rf "$ws"; mkdir -p "$ws"
: > "$ws/.pipedpeerignore"
cp "$root/docs/benchmarks/workloads/scaling_mod.py" "$ws/scaling.py"
  cp "$root/docs/benchmarks/workloads/kernel.py" "$ws/"

echo
echo "warming the closure onto all three nodes..."
( cd "$ws" && "$cli" run --port 38081 --host 127.0.0.1:38081 --isolate=false scaling.py >/dev/null 2>&1 )
sleep 5

run_once() {  # label extra-args...
  local label=$1; shift
  local t0 t1
  t0=$(date +%s.%N)
  # --isolate=false so the job inherits the daemon's CPU affinity; crun
  # resets it, which is the same absence of CPU isolation the report notes.
  ( cd "$ws" && "$cli" run --port 38081 --host 127.0.0.1:38081 --isolate=false "$@" scaling.py \
      > "$raw/S_${label}.log" 2>&1 )
  t1=$(date +%s.%N)
  local inner
  inner=$(grep -o 'seconds=[0-9.]*' "$raw/S_${label}.log" | head -1 | cut -d= -f2)
  echo "$label wall=$(echo "$t1 - $t0" | bc) inner=${inner:-FAILED}"
}

echo
echo "=== baseline: node A only, two cores ==="
for i in 1 2 3; do run_once "local$i" -e PIPEDPEER_SHIM=0; done
echo
echo "=== distributed: A spills to B and C, ten cores ==="
for i in 1 2 3; do run_once "dist$i" --distribute force; done
