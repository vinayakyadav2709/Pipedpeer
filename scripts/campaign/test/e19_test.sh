#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034
# Case table for scripts/campaign/e19.sh and cpu_sampler.py on the fake lab.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
WORKLOADS="$REPO/docs/benchmarks/workloads"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
pass=0; failed=0; problems=()

DIRS_FILE="$(mktemp)"
cleanup() {
  local d
  while IFS= read -r d; do
    [[ -n "$d" ]] || continue
    pkill -f -- "$d/" 2>/dev/null || true
    rm -rf "$d"
  done <"$DIRS_FILE"
  rm -f "$DIRS_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

ok() { pass=$((pass + 1)); echo "ok   $1"; }
bad() { failed=$((failed + 1)); echo "FAIL $1 -- $2"; }
pnote() { problems+=("$*"); }
verdict() {
  if ((${#problems[@]} == 0)); then
    ok "$1"
  else
    bad "$1" "${problems[*]}"
  fi
  problems=()
}
newdir() {
  local d
  d="$(mktemp -d "$STATE/e-e19-test.XXXXXX")"
  mkdir -p "$d/home"
  echo "$d" >>"$DIRS_FILE"
  echo "$d"
}

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

setup_lab() {
  local dir="$1" n="$2" v="$1/verif"
  fakelab_init "$dir" "$n" || exit 90
  mkdir -p "$v"
  export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.1 LAB_POLL_S=1
  export CLUSTER_SH="$v/cluster.sh" METRICS_SH="$v/metrics.sh" UPLOAD_SH="$v/upload.sh"

  cat >"$v/cluster.sh" <<'EOF'
#!/usr/bin/env bash
echo "cluster $*" >>"$LAB_RESULTS/cluster.log"
exit 0
EOF
  chmod +x "$v/cluster.sh"

  cat >"$v/metrics.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$v/metrics.sh"

  cat >"$v/upload.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$v/upload.sh"

  cat >"$v/pipedpeer" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; }
me="$(basename "$HOME")"
echo "$me pipedpeer $*" >>"$FAKELAB_DIR/pipedpeer.log"
case "${1:-}" in
  invite)
    [[ -z "${FAKE_INVITE_EMPTY:-}" ]] || exit 0
    echo "pipedpeer connect pp1-faketoken12345"
    ;;
  connect)
    if [[ -n "${FAKE_CONNECT_FAIL:-}" && "$me" == "$FAKE_CONNECT_FAIL" ]]; then
      echo "connection failed on $me" >&2
      exit 1
    fi
    echo "connected" >"$HOME/.connected"
    ;;
  stop)
    echo "stopped" >"$HOME/.stopped"
    ;;
  start)
    rm -f "$HOME/.stopped"
    ;;
  status)
    echo -e "NAME\tSTATE\tJOBS\n$me\thealthy\t0"
    ;;
  run)
    if [[ -n "${FAKE_RUN_SLEEP:-}" ]]; then sleep "$FAKE_RUN_SLEEP"; fi
    echo "SWEEP-OK items=10 seconds=1.0000 checksum=5.000000"
    ;;
  jobs)
    echo '{"steps":[]}'
    ;;
  *)
    exit 0
    ;;
esac
EOF
  chmod +x "$v/pipedpeer"

  cat >"$v/systemctl" <<'EOF'
#!/usr/bin/env bash
me="$(basename "$HOME")"
echo "$me systemctl $*" >>"$FAKELAB_DIR/systemctl.log"
exit 0
EOF
  chmod +x "$v/systemctl"

  printf '%s\n' BENCH_PROCS=1 W1_ITEMS=10 W1_WEAK_PER_PC=1 W2_ITEMS=10 W3_TREES=1 T1_EPOCHS=1 \
    SWEEP_ITERS_PER_MS=100 SWEEP_TOTAL_MS=10 SWEEP_DATA_ITEMS=10 SWEEP_DATA_TASK_MS=1 NOOP_ITEMS=10 LAT_CALLS=10 \
    ELASTIC_TASK_MS=1 ELASTIC_ITEMS=10 FILEIO_FILES=1 FILEIO_FILE_KB=1 >"$dir/sizes.env"
  # the real user journal must not leak into the lab
  printf '#!/usr/bin/env bash\nexit 0\n' >"$v/journalctl"
  chmod +x "$v/journalctl"
  export PATH="$v:$PATH"
}

# ---------------------------------------------------------------------- Case 6: --dry-run
d="$(newdir)"
( setup_lab "$d" 4 ) >/dev/null
run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e19.sh" --dry-run 2>&1)" || pnote "e19 dry exit $?"
echo "$run" | grep -q "E19-DRY-RUN" || pnote "no E19-DRY-RUN marker in: $run"
[[ ! -f "$d/cluster.log" ]] || pnote "cluster.sh touched during dry-run"
verdict "--dry-run: prints the plan, touches nothing"

# ---------------------------------------------------------------------- Case 7: cpu_sampler.py fake stat deltas
d="$(newdir)"
stat_f="$d/stat"
out_f="$d/samples.txt"
echo "cpu  1000 0 500 8500 0 0 0 0" >"$stat_f"
(
  export LAB_PROC_STAT="$stat_f" LAB_SAMPLE_INTERVAL_S=0.05 LAB_CORES=8
  python3 "$WORKLOADS/cpu_sampler.py" "$out_f" 0.25 &
  pid=$!
  sleep 0.02
  echo "cpu  2500 0 1000 16500 0 0 0 0" >"$stat_f"
  wait "$pid" || true
) >"$d/out" 2>&1 || pnote "cpu_sampler run failed: $(cat "$d/out")"
if [[ -f "$out_f" ]]; then
  val="$(awk 'NR==1 {print $2}' "$out_f" 2>/dev/null || echo "")"
  python3 -c "import sys; v=float(sys.argv[1]); assert 1.5 <= v <= 1.7, v" "$val" 2>/dev/null || pnote "busy_cores $val not close to 1.6"
else
  pnote "samples file missing"
fi
verdict "cpu_sampler.py, fake stat file, two reads: busy cores computed from deltas"

# ---------------------------------------------------------------------- Case 1: samples 2,2,6,6,6,6,6 on 8 cores
d="$(newdir)"
out_f="$d/samples.txt"
join_ns=1000000000000
for i in 0 1; do
  echo "$((join_ns + i * 1000000000)) 2.0" >>"$out_f"
done
for i in 2 3 4 5 6; do
  echo "$((join_ns + i * 1000000000)) 6.0" >>"$out_f"
done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || pnote "sampler --streak failed: $res"
[[ "$res" == "2.0" || "$res" == "2" ]] || pnote "first_work_s $res != 2.0"
verdict "samples 2,2,6,6,6,6,6 on 8 cores, join at sample 0: first_work_s = index of the first 6"

# ---------------------------------------------------------------------- Case 2: spike of 3 then idle then 5-streak
d="$(newdir)"
out_f="$d/samples.txt"
join_ns=1000000000000
idx=0
for v in 2.0 2.0; do
  echo "$((join_ns + idx * 1000000000)) $v" >>"$out_f"
  idx=$((idx + 1))
done
for v in 6.0 6.0 6.0; do
  echo "$((join_ns + idx * 1000000000)) $v" >>"$out_f"
  idx=$((idx + 1))
done
for v in 2.0 2.0; do
  echo "$((join_ns + idx * 1000000000)) $v" >>"$out_f"
  idx=$((idx + 1))
done
first_streak_idx=$idx
for v in 6.0 6.0 6.0 6.0 6.0; do
  echo "$((join_ns + idx * 1000000000)) $v" >>"$out_f"
  idx=$((idx + 1))
done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || pnote "sampler --streak failed: $res"
[[ "$res" == "$first_streak_idx.0" || "$res" == "$first_streak_idx" ]] || pnote "expected $first_streak_idx, got $res"
verdict "spike of 3 samples then idle then a 5-sample streak: the streak, the near-miss"

# ---------------------------------------------------------------------- Case 3: no streak
d="$(newdir)"
out_f="$d/samples.txt"
join_ns=1000000000000
for i in $(seq 0 10); do
  echo "$((join_ns + i * 1000000000)) 2.0" >>"$out_f"
done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || pnote "sampler --streak failed: $res"
[[ -z "$res" ]] || pnote "expected empty for no streak, got: $res"
verdict "no streak: empty, logged"

# ---------------------------------------------------------------------- Case 4: checksums a,a,b
d="$(newdir)"
( setup_lab "$d" 2 ) >/dev/null
mkdir -p "$d/analysis-input"
run_data="run1:123.456 run2:123.456 run3:999.999"
res="$(export LAB_RESULTS="$d"; source "$CAMP/e19.sh" && e19_resolve_checksums "$run_data" 2>&1)" || pnote "resolve_checksums exit $?: $res"
echo "$res" | grep -q "run1=1" || pnote "run1 not 1: $res"
echo "$res" | grep -q "run2=1" || pnote "run2 not 1: $res"
echo "$res" | grep -q "run3=0" || pnote "run3 (minority) not 0: $res"
verdict "checksums a,a,b: the b run 0, logged"

# ---------------------------------------------------------------------- Case 5: checksums a,b,c
d="$(newdir)"
( setup_lab "$d" 2 ) >/dev/null
run_data="run1:111.111 run2:222.222 run3:333.333"
res="$(export LAB_RESULTS="$d"; source "$CAMP/e19.sh" && e19_resolve_checksums "$run_data" 2>&1)" || pnote "resolve_checksums exit $?: $res"
echo "$res" | grep -q "run1=" || pnote "run1 not empty: $res"
echo "$res" | grep -q "run2=" || pnote "run2 not empty: $res"
echo "$res" | grep -q "run3=" || pnote "run3 not empty: $res"
if [[ -f "$d/BLOCKERS.md" ]]; then
  grep -qi "checksum" "$d/BLOCKERS.md" || pnote "checksum mismatch not in BLOCKERS.md"
else
  pnote "BLOCKERS.md missing"
fi
verdict "checksums a,b,c: empty, BLOCKERS line"

# ---------------------------------------------------------------------- Case 8: join command fails on one PC
d="$(newdir)"
(
  setup_lab "$d" 3
  export LAB_RESULTS="$d"
  export FAKE_CONNECT_FAIL="pc01"
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 2 --repeats 1 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $? ($(cat "$d/e19.log"))"
csv="$d/analysis-input/e19.csv"
if [[ -f "$csv" ]]; then
  row_pc01="$(grep "pc01" "$csv" 2>/dev/null || echo "")"
  row_pc02="$(grep "pc02" "$csv" 2>/dev/null || echo "")"
  [[ -n "$row_pc01" ]] || pnote "pc01 row missing"
  [[ -n "$row_pc02" ]] || pnote "pc02 row missing"
  join_pc01="$(awk -F, '{print $5}' <<<"$row_pc01")"
  [[ -z "$join_pc01" ]] || pnote "pc01 join_s should be empty, got: $join_pc01"
  [[ -f "$d/pc02/.connected" ]] || pnote "pc02 did not connect"
else
  pnote "e19.csv missing"
fi
verdict "join command fails on one PC: row empty, the others go on"

# ---------------------------------------------------------------------- Case 9: trap on INT restores nodes.db
d="$(newdir)"
db_dir="$d/pc01/.local/share/pipedpeer"
mkdir -p "$db_dir"
echo "original_nodes_db" >"$db_dir/nodes.db"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  FAKE_RUN_SLEEP=10 bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems pipedpeer &
  pid=$!
  deadline=$((SECONDS + 5))
  while ((SECONDS < deadline)); do
    if [[ -f "$db_dir/nodes.db.aside" ]]; then
      touch "$d/moved_aside"
      break
    fi
    sleep 0.05
  done
  kill -INT "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
) >/dev/null 2>&1 || true

if [[ ! -f "$d/moved_aside" ]]; then
  pnote "nodes.db was never moved aside before trap"
fi
if [[ -f "$db_dir/nodes.db" ]]; then
  content="$(cat "$db_dir/nodes.db" 2>/dev/null || echo "")"
  [[ "$content" == "original_nodes_db" ]] || pnote "nodes.db content corrupted: $content"
else
  pnote "nodes.db not restored"
fi
verdict "trap on INT: nodes.db back on every joiner (asserted on fake HOME)"

# stub venv on every PC: python (the join poll) sleeps FAKE_POLL_SLEEP, exits FAKE_POLL_RC
mkvenv() {
  local d="$1" pc
  for pc in "$d"/pc*; do
    mkdir -p "$pc/pipedpeer-lab/venv/bin"
    cat >"$pc/pipedpeer-lab/venv/bin/python" <<'EOF2'
#!/usr/bin/env bash
cat >/dev/null
echo "poll $*" >>"$FAKELAB_DIR/poll.log"
sleep "${FAKE_POLL_SLEEP:-0}"
exit "${FAKE_POLL_RC:-0}"
EOF2
    printf '#!/usr/bin/env bash\nexit 0\n' >"$pc/pipedpeer-lab/venv/bin/ray"
    cp "$pc/pipedpeer-lab/venv/bin/ray" "$pc/pipedpeer-lab/venv/bin/dask"
    chmod +x "$pc"/pipedpeer-lab/venv/bin/*
  done
}

# ---------------------------------------------------------------------- Case 10: no fallback wrappers
d="$(newdir)"
( setup_lab "$d" 2 ) >/dev/null
got="$(export LAB_RESULTS="$d" ELASTIC_ITEMS=19200 ELASTIC_TASK_MS=50 SWEEP_ITERS_PER_MS=100; source "$CAMP/e19.sh" && _run_size_json EL 4 2>&1)" || pnote "size json exit: $got"
[[ "$got" == '{"items":19200,"task_ms":50,"payload_bytes":0}' ]] || pnote "ELASTIC_ITEMS set: $got"
got="$(unset ELASTIC_ITEMS; export LAB_RESULTS="$d"; source "$CAMP/e19.sh" 2>&1 && _run_size_json EL 4 2>&1)" && pnote "ELASTIC_ITEMS unset must fail, got: $got"
[[ "$got" != *BENCH_ITEMS* ]] || pnote "fallback wrapper still present: $got"
a="$(source "$CAMP/run.sh" >/dev/null 2>&1; declare -f _run_size_json | md5sum)"
b="$(export LAB_RESULTS="$d"; source "$CAMP/e19.sh" >/dev/null 2>&1; declare -f _run_size_json | md5sum)"
[[ "$a" == "$b" ]] || pnote "_run_size_json differs from run.sh"
verdict "sourcing e19.sh leaves run.sh's _run_size_json; unset sizes fail, no silent default"

# ---------------------------------------------------------------------- Case 11: ray join waits for the poll
d="$(newdir)"
(
  setup_lab "$d" 2; mkvenv "$d"
  export LAB_RESULTS="$d" FAKE_POLL_SLEEP=2 E19_STORE_PATH=
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems ray
) >"$d/e19.log" 2>&1 || pnote "e19 ray exit $? ($(tail -5 "$d/e19.log"))"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
js="$(awk -F, '{print $5}' <<<"$row")"
python3 -c "import sys; assert float(sys.argv[1]) >= 2.0, sys.argv[1]" "${js:-x}" 2>/dev/null || pnote "ray join_s '$js' < 2 (row: $row, log: $(tail -3 "$d/e19.log"))"
grep -q "poll - ray " "$d/poll.log" 2>/dev/null || pnote "poll not run for ray"
verdict "ray join_s covers the poll (2 s stub), not the start call's 0.05 s"

# ---------------------------------------------------------------------- Case 12: ray never ready
d="$(newdir)"
(
  setup_lab "$d" 2; mkvenv "$d"
  export LAB_RESULTS="$d" FAKE_POLL_RC=1 E19_STORE_PATH=
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems ray
) >"$d/e19.log" 2>&1 || pnote "e19 ray exit $?"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ -n "$row" ]] || pnote "no join row"
[[ -z "$(awk -F, '{print $5}' <<<"$row")" ]] || pnote "join_s not empty when not ready: $row"
grep -q "join command failed" "$d/e19.log" 2>/dev/null || grep -q "failed" "$LAB_RESULTS/lab.log" 2>/dev/null || true
verdict "ray not ready: join_s empty, not 0.05"

# ---------------------------------------------------------------------- Case 13: dask join waits for the poll
d="$(newdir)"
(
  setup_lab "$d" 2; mkvenv "$d"
  export LAB_RESULTS="$d" FAKE_POLL_SLEEP=2 E19_STORE_PATH=
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems dask
) >"$d/e19.log" 2>&1 || pnote "e19 dask exit $? ($(tail -5 "$d/e19.log"))"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
js="$(awk -F, '{print $5}' <<<"$row")"
python3 -c "import sys; assert float(sys.argv[1]) >= 2.0, sys.argv[1]" "${js:-x}" 2>/dev/null || pnote "dask join_s '$js' < 2"
grep -q "poll - dask " "$d/poll.log" 2>/dev/null || pnote "poll not run for dask"
verdict "dask join_s covers the poll"

# ---------------------------------------------------------------------- Case 14: store path derived, closure purged, joiner_warm 0
d="$(newdir)"
sp="/nix/store/abcdefghijklmnopqrstuvwxyz012345-env"
mkdir -p "$d/pc00/.local/state/pipedpeer" "$d/pc01/.local/share/pipedpeer/nix/store/abcdefghijklmnopqrstuvwxyz012345-env"
echo "{\"msg\":\"peer took the closure\",\"store\":\"$sp\",\"secs\":1.5}" >"$d/pc00/.local/state/pipedpeer/daemon.log"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  unset E19_STORE_PATH
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 2 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $? ($(tail -5 "$d/e19.log"))"
[[ ! -e "$d/pc01/.local/share/pipedpeer/nix/store/abcdefghijklmnopqrstuvwxyz012345-env" ]] || pnote "closure not purged on the joiner"
rows="$(grep -c ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ "$rows" == 2 ]] || pnote "expected 2 join rows, got $rows"
bad_warm="$(grep ',join,' "$d/analysis-input/e19.csv" | awk -F, '$10 != "0"' || true)"
[[ -z "$bad_warm" ]] || pnote "joiner_warm not 0 in: $bad_warm"
verdict "closure path from the daemon log: purged before each repeat, joiner_warm 0 in both"

# ---------------------------------------------------------------------- Case 15: no store path -> joiner_warm empty, logged
d="$(newdir)"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  unset E19_STORE_PATH
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $?"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ -n "$row" && -z "$(awk -F, '{print $10}' <<<"$row")" ]] || pnote "joiner_warm should be empty (unknown): $row"
verdict "no closure path found: joiner_warm empty, not a false 0"

# ---------------------------------------------------------------------- Case 16: empty invite code
d="$(newdir)"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d" FAKE_INVITE_EMPTY=1
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $?"
[[ ! -f "$d/pc01/.connected" ]] || pnote "joiner connected with an empty code"
! grep -q ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || pnote "join row written for an aborted repeat"
verdict "empty invite code: repeat aborted, no join, no row"

echo "TOTAL: pass=$pass failed=$failed"
exit "$failed"
