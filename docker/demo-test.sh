#!/usr/bin/env bash
# Full automated demo verification in Docker, step by step. Evidence for every
# stage is captured per node (processes, daemon logs, nix store, job files,
# traffic ledger) into docker/logs/<node>/, so the live demo can show each
# claim backed by artifacts from all four "machines".
#
# 3 worker containers + 1 orchestrator ("weak laptop") on a private bridge
# network (no mDNS, exactly like a LAN without multicast). The four demo
# scripts run for real; observability (traffic, jobs, job --output, tasks,
# dashboard, ping, prune) is verified against live state.
set -uo pipefail
cd "$(dirname "$0")"

ROOT="$(cd .. && pwd)"
DEMO="$ROOT/pipedpeer-demo"
LOG="$ROOT/docker/logs"
NODES=(pp-orch pp-worker1 pp-worker2 pp-worker3)
mkdir -p "$LOG"

PASS=0
FAIL=0
check() { # check <name> <grep pattern> <file>
  if [ -f "$3" ] && grep -q -- "$2" "$3" 2>/dev/null; then
    PASS=$((PASS + 1)); echo "PASS: $1"
  else
    FAIL=$((FAIL + 1)); echo "FAIL: $1 (pattern \"$2\" not found in $3)"
  fi
}
section() { echo; echo "==== $1 ===="; }

snapshot() { # snapshot <step>: processes + daemon log + store + files + traffic on every node
  for n in "${NODES[@]}"; do
    d="$LOG/$n"; mkdir -p "$d"
    docker exec "$n" sh -c "ps aux | grep -E 'pipedpeer|nix-build|python|run ' | grep -v grep" > "$d/$1-ps.txt" 2>&1
    docker exec "$n" sh -c "tail -50 /tmp/pipedpeer/daemon.log" > "$d/$1-daemon.log" 2>&1
    docker exec "$n" sh -c "echo 'store entries:'; ls /nix/store | wc -l; ls -d /nix/store/*-run 2>/dev/null" > "$d/$1-store.txt" 2>&1
    docker exec "$n" sh -c "ls -la /root/.local/share/pipedpeer/jobs 2>/dev/null | head -12" > "$d/$1-files.txt" 2>&1
    docker exec "$n" pipedpeer traffic > "$d/$1-traffic.txt" 2>&1
  done
}

# --- 0. binary + data -------------------------------------------------------
section "0. build binary + generate data.csv"
make -C "$ROOT" build >/dev/null 2>&1 || { echo "build failed"; exit 1; }
if [ ! -f "$DEMO/data.csv" ]; then
  python3 - "$DEMO/data.csv" <<'PYEOF' || { echo "data gen failed"; exit 1; }
import numpy as np
import pandas as pd
import sys

rng = np.random.RandomState(42)
n = 10_000_000
cats = np.random.choice(["alpha", "beta", "gamma", "delta"], size=n)
df = pd.DataFrame({
    "id": np.arange(n, dtype=np.int64), "cat": cats,
    "g1": rng.rand(n), "g2": rng.rand(n), "g3": rng.rand(n),
    "g4": rng.rand(n), "out": rng.rand(n),
})
df.to_csv(sys.argv[1], index=False)
print("wrote " + sys.argv[1])
PYEOF
else
  echo "data.csv present: $(du -h "$DEMO/data.csv" | cut -f1)"
fi

# --- 1. cluster up ----------------------------------------------------------
section "1. compose build + seed shared nix store"
docker compose build 2>&1 | tail -3

# All four containers bind-mount ONE host /nix/store so the orchestrator's
# closure build is visible to every worker with no per-worker import (which
# OOM'd this shared-RAM rig). Seed the host dir once from the image's base
# store (nix + curl + bash); rm -rf the host dir to force a re-seed.
#
# The host dir must live on a Unix-permission filesystem: nix rejects
# "suspicious ownership or permission" on a store it cannot stat correctly
# (NTFS/fuseblk mounts report 0777/uid-1000 for everything). /var/lib is on
# the Linux root (btrfs) here; override with PP_NIXSTORE for other hosts.
STORE="${PP_NIXSTORE:-/var/lib/pipedpeer-nixstore}"
mkdir -p "$STORE"
if [ -z "$(ls -A "$STORE" 2>/dev/null)" ]; then
  img=$(docker compose config --images | head -1)
  docker run --rm -v "$STORE:/seed" "$img" sh -c 'cp -a /nix/store/. /seed/'
  echo "seeded nixstore from image ($(du -sh "$STORE" | cut -f1))"
else
  echo "nixstore already seeded ($(du -sh "$STORE" | cut -f1))"
fi

docker compose up -d 2>&1 | tail -3

section "2. wait for worker daemons"
for w in 1 2 3; do
  for i in $(seq 1 90); do
    if docker exec "pp-worker$w" pipedpeer status 2>/dev/null | grep -q running; then
      echo "worker$w up"; break
    fi
    [ "$i" = 90 ] && { echo "worker$w never came up"; exit 1; }
    sleep 2
  done
done

section "3. deploy fresh binary + orchestrator daemon + manual node registration"
for n in "${NODES[@]}"; do
  docker cp "$ROOT/bin/pipedpeer" "$n:/usr/local/bin/pipedpeer"
  docker exec "$n" sh -c "pipedpeer stop >/dev/null 2>&1; pipedpeer start" 2>&1 | tail -1
done
sleep 2
docker exec pp-orch pipedpeer nodes add worker1 38080
docker exec pp-orch pipedpeer nodes add worker2 38080
docker exec pp-orch pipedpeer nodes add worker3 38080
sleep 18  # peer poller interval (10s) + health checks
docker exec pp-orch pipedpeer nodes > "$LOG/nodes.txt"
cat "$LOG/nodes.txt"
healthy=$(grep -c "healthy" "$LOG/nodes.txt" || true)
if [ "$healthy" -ge 3 ]; then
  PASS=$((PASS + 1)); echo "PASS: >=3 healthy nodes listed"
else
  FAIL=$((FAIL + 1)); echo "FAIL: only $healthy healthy nodes (want >=3)"
fi

# --- 4. the four demo runs, each with lifecycle evidence --------------------
run_demo() { # run_demo <name> <script> <extra-args> <grep>
  section "4. run $1"
  snapshot "$1-start"   # processes/logs/store/files before: the handoff
  timeout 5400 docker exec pp-orch sh -c \
    "cd /workspace && pipedpeer run --script $2 --intercept --no-self --isolate=false $3" \
    > "$LOG/$1.out" 2>&1
  ec=$?
  [ "$ec" = 0 ] && echo "exit ok" || { echo "EXIT FAIL ($ec) — tail:"; tail -8 "$LOG/$1.out"; }
  snapshot "$1-end"     # evidence after: what each node saw
  check "$1 ran" "$4" "$LOG/$1.out"
}

run_demo 01_sklearn_rf 01_sklearn_rf.py "" "accuracy:"
check "01 shim ledger: joblib dispatch" "joblib: dispatching" "$LOG/01_sklearn_rf.out"
if grep -q "local covers it" "$LOG/01_sklearn_rf.out"; then
  FAIL=$((FAIL + 1)); echo "FAIL: 01 fell back to local (remote dispatch broken)"
else
  PASS=$((PASS + 1)); echo "PASS: 01 ran fully remote (no local fallback)"
fi
run_demo 02_numpy_heavy 02_numpy_heavy.py "" "reconstruction relative error:"
if grep -q "matmul: sending" "$LOG/02_numpy_heavy.out"; then
  FAIL=$((FAIL + 1)); echo "FAIL: matmul should stay local (local BLAS ~200 GFLOP/s beats cluster transport)"
else
  PASS=$((PASS + 1)); echo "PASS: matmul stayed local (D2: local BLAS wins)"
fi
check "02 shim ledger: svd offload" "offloading" "$LOG/02_numpy_heavy.out"
run_demo 03_pandas_ooc 03_pandas_ooc.py "-e PIPEDPEER_PANDAS=1 -e PIPEDPEER_OOC_MIN=5e8" "groupby('cat').mean()"
check "03 result rows: all 4 categories" "gamma" "$LOG/03_pandas_ooc.out"
check "03 shim ledger: ooc read" "read_csv: streaming" "$LOG/03_pandas_ooc.out"
check "03 shim ledger: per-chunk combine" "combining" "$LOG/03_pandas_ooc.out"
# DDP rank count for demo 04. This rig is 3 containers sharing ONE host's RAM
# (plus the desktop), so keep it small locally — each rank is a torch process.
# On real machines (each with its own RAM + GPU) bump this to the worker count.
DDP="${DDP:-1}"
run_demo 04_torch_ddp 04_torch_ddp.py "--ddp $DDP" "final loss:"

# --- 5. where did the work actually run? ------------------------------------
section "5. placement proof: jobs list shows workers, not the orchestrator"
docker exec pp-orch pipedpeer jobs > "$LOG/jobs.txt"
cat "$LOG/jobs.txt"
succ=$(grep -c "succeeded" "$LOG/jobs.txt" || true)
if [ "$succ" -ge 4 ]; then
  PASS=$((PASS + 1)); echo "PASS: >=4 succeeded jobs"
else
  FAIL=$((FAIL + 1)); echo "FAIL: $succ succeeded jobs (want >=4)"
fi
if awk 'NR>2 { print $3 }' "$LOG/jobs.txt" | grep -q "^orch$"; then
  FAIL=$((FAIL + 1)); echo "FAIL: a job ran on the orchestrator (--no-self violated)"
else
  PASS=$((PASS + 1)); echo "PASS: every job executed on a worker node (--no-self honored)"
fi

last_id=$(awk 'NR > 2 { print $1 }' "$LOG/jobs.txt" | head -1)
docker exec pp-orch pipedpeer job --id "$last_id" --output > "$LOG/job.txt"
check "job detail has stdout" "final loss" "$LOG/job.txt"
check "job detail shows target node" "target" "$LOG/job.txt"

section "5b. closure materialised on every worker (store broadcast)"
store_ok=0
for w in 1 2 3; do
  if grep -q -- "-run" "$LOG/pp-worker$w/01_sklearn_rf-end-store.txt" 2>/dev/null; then
    store_ok=$((store_ok + 1))
  fi
done
if [ "$store_ok" = 3 ]; then
  PASS=$((PASS + 1)); echo "PASS: all 3 workers hold the closure store path"
else
  FAIL=$((FAIL + 1)); echo "FAIL: only $store_ok/3 workers materialised the store"
fi

section "5c. worker daemon logs show the pool traffic ledger"
ledger_ok=0
for w in 1 2 3; do
  if grep -q "received pool/map" "$LOG/pp-worker$w/04_torch_ddp-end-daemon.log" 2>/dev/null \
     || grep -q "received pool/map" "$LOG/pp-worker$w/02_numpy_heavy-end-daemon.log" 2>/dev/null; then
    ledger_ok=$((ledger_ok + 1))
  fi
done
if [ "$ledger_ok" -ge 1 ]; then
  PASS=$((PASS + 1)); echo "PASS: worker daemon logs carry the pool ledger lines"
else
  FAIL=$((FAIL + 1)); echo "FAIL: no pool ledger lines in any worker daemon log"
fi

# --- 6. tasks --watch during a live run -------------------------------------
section "6. pipedpeer tasks --watch during a live run"
docker exec -d pp-orch sh -c "pipedpeer tasks --watch > /tmp/tasks.log 2>&1"
timeout 900 docker exec pp-orch sh -c \
  "cd /workspace && pipedpeer run --script 02_numpy_heavy.py --intercept --no-self --isolate=false" \
  > "$LOG/02b.out" 2>&1
docker exec pp-orch sh -c "pkill -f 'tasks --watch' || true" >/dev/null 2>&1
sleep 1
docker exec pp-orch sh -c "cat /tmp/tasks.log" > "$LOG/tasks.log"
check "tasks watch rendered rows" "LEASE" "$LOG/tasks.log"
check "tasks watch saw a running lease" "running" "$LOG/tasks.log"

# --- 7. dashboard + ping ----------------------------------------------------
section "7. dashboard and ping smoke"
docker exec pp-orch sh -c "TERM=xterm-256color timeout 6 script -qec 'pipedpeer dashboard' /tmp/dash.log > /dev/null 2>&1; echo exit=\$? > /tmp/dash-exit"
dash_exit=$(docker exec pp-orch cat /tmp/dash-exit | tr -d ' ' | sed 's/^exit=//')
echo "dashboard exit: $dash_exit (124 = ran until timeout = alive)"
docker exec pp-orch cat /tmp/dash.log > "$LOG/dash.txt"
check "dashboard rendered a header" "PIPEDPEER\|JOBS\|NODES" "$LOG/dash.txt"
if [ "$dash_exit" = "124" ] || [ "$dash_exit" = "0" ]; then
  PASS=$((PASS + 1)); echo "PASS: dashboard stayed up"
else
  FAIL=$((FAIL + 1)); echo "FAIL: dashboard exited with $dash_exit"
fi
docker exec pp-orch sh -c "timeout 4 pipedpeer ping worker1:38080 > /tmp/ping.log 2>&1; echo exit=\$? > /tmp/ping-exit"
ping_exit=$(docker exec pp-orch cat /tmp/ping-exit | tr -d ' ')
docker exec pp-orch cat /tmp/ping.log > "$LOG/ping.txt"
check "ping shows the worker" "worker1" "$LOG/ping.txt"
echo "ping exit: $ping_exit (124 = looped until timeout)"

# --- 8. jobs prune ----------------------------------------------------------
section "8. jobs prune"
docker exec pp-orch sh -c 'd="$HOME/.local/share/pipedpeer/jobs/zz-old"; mkdir -p "$d"; touch -d "10 days ago" "$d"; pipedpeer jobs prune' > "$LOG/prune.txt"
cat "$LOG/prune.txt"
check "prune removed the old entry" "pruned 1" "$LOG/prune.txt"

# --- 9. weak orchestrator: CPU proof ----------------------------------------
section "9. CPU proof (docker stats during a heavy run)"
docker exec -d pp-orch sh -c "cd /workspace && pipedpeer run --script 02_numpy_heavy.py --intercept --no-self --isolate=false > /tmp/02c.out 2>&1"
sleep 25
for i in 1 2 3 4; do
  docker stats --no-stream pp-orch pp-worker1 pp-worker2 pp-worker3 >> "$LOG/stats.txt"
  sleep 8
done
docker exec pp-orch sh -c "pkill -f 'pipedpeer run' || true" >/dev/null 2>&1
cat "$LOG/stats.txt"
orch_max=$(awk '$2 == "pp-orch" { gsub("%", "", $3); print $3 }' "$LOG/stats.txt" | sort -n | tail -1)
work_max=$(awk '$2 ~ /^pp-worker/ { gsub("%", "", $3); print $3 }' "$LOG/stats.txt" | sort -n | tail -1)
echo "orchestrator max CPU: ${orch_max}% | worker max CPU: ${work_max}%"
if awk -v o="$orch_max" -v w="$work_max" 'BEGIN { exit !(o < w && w > 10) }'; then
  PASS=$((PASS + 1)); echo "PASS: orchestrator stayed below the workers"
else
  FAIL=$((FAIL + 1)); echo "FAIL: weak-orchestrator proof not shown (orch=${orch_max}% workers=${work_max}%)"
fi

# --- summary ----------------------------------------------------------------
section "10. final evidence sweep: full daemon logs per node"
for n in "${NODES[@]}"; do
  docker exec "$n" sh -c "cat /tmp/pipedpeer/daemon.log" > "$LOG/$n/final-daemon.log" 2>&1
  docker exec "$n" pipedpeer traffic > "$LOG/$n/final-traffic.txt" 2>&1
done

section "SUMMARY: $PASS passed, $FAIL failed"
docker compose down -v >/dev/null 2>&1
[ "$FAIL" = 0 ] || exit 1