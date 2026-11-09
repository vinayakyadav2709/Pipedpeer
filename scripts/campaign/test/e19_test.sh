#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034
# Case table for scripts/campaign/e19.sh and cpu_sampler.py on the fake lab.
set -euo pipefail

REPO="${REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
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
echo "cluster $*" >>"$LAB_RESULTS/order.log"
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
    if [[ -n "${FAKE_CONNECT_255:-}" ]]; then exit 255; fi
    if [[ -n "${FAKE_CONNECT_RC:-}" ]]; then echo "partly connected: 10.9.9.9 unreachable" >&2; exit "$FAKE_CONNECT_RC"; fi
    echo "connected" >"$HOME/.connected"
    ;;
  stop)
    echo "stopped" >"$HOME/.stopped"
    ;;
  start)
    rm -f "$HOME/.stopped"
    ;;
  status)
    echo -e "NAME\tSTATE\tJOBS"
    for ((i = 0; i < ${FAKE_STATUS_ROWS:-1}; i++)); do echo -e "$me-$i\thealthy\t0"; done
    echo
    ;;
  run)
    if [[ -n "${FAKE_RUN_SLEEP:-}" ]]; then sleep "$FAKE_RUN_SLEEP"; fi
    run_no=$(($(cat "$FAKELAB_DIR/run.count" 2>/dev/null || echo 0) + 1))
    echo "$run_no" >"$FAKELAB_DIR/run.count"
    if [[ -n "${FAKE_RUN_FAIL_FROM:-}" ]] && ((run_no >= FAKE_RUN_FAIL_FROM)); then exit 1; fi
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
[[ "$*" != *"stop pipedpeer.service"* && "$*" != *"stop lab-ray"* ]] || echo "$me stop" >>"$FAKELAB_DIR/order.log"
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

# with_sigint <cmd...>: run cmd with SIGINT at its default, as a Ctrl-C target.
# A background job of a non-interactive shell starts with SIGINT ignored, and
# bash cannot trap a signal that was ignored at entry.
with_sigint() {
  exec python3 -c 'import os,signal,sys; signal.signal(signal.SIGINT, signal.SIG_DFL); os.execvp(sys.argv[1], sys.argv[1:])' "$@"
}

# ---------------------------------------------------------------------- Case 9: trap on INT restores nodes.db
d="$(newdir)"
db_dir="$d/pc01/.local/share/pipedpeer"
mkdir -p "$db_dir"
echo "original_nodes_db" >"$db_dir/nodes.db"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  FAKE_RUN_SLEEP=10 with_sigint bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems pipedpeer &
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

# mkjob <lab> <id> <started_at> <store>: a job record on pc00, as the daemon writes it
mkjob() {
  local jd="$1/pc00/.local/share/pipedpeer/jobs/$2"
  mkdir -p "$jd"
  printf '{"id":"%s","status":"finished","started_at":"%s","script_path":"x","remote":"","target_id":"","store_path":"%s"}\n' "$2" "$3" "$4" >"$jd/metadata.json"
}

# stub venv on every PC: python (the join poll) sleeps FAKE_POLL_SLEEP, exits FAKE_POLL_RC
mkvenv() {
  local d="$1" pc
  mkdir -p "$d/fakepy"
  cat >"$d/fakepy/ray.py" <<'EOF3'
import os, sys, time
time.sleep(float(os.environ.get("FAKE_IMPORT_SLEEP", "0")))
_t = None
def is_initialized():
    return _t is not None
def init(address=None, **kw):
    global _t
    err = os.environ.get("FAKE_POLL_ERROR")
    if err:
        raise RuntimeError(err)
    if _t is None:
        _t = time.time()
def nodes():
    if os.environ.get("FAKE_POLL_RC") == "1" or time.time() - _t < float(os.environ.get("FAKE_POLL_SLEEP", "0")):
        return []
    return [{"Alive": True, "NodeManagerAddress": sys.argv[3], "Resources": {"CPU": 8.0}}]
EOF3
  cat >"$d/fakepy/distributed.py" <<'EOF3'
import os, sys, time
time.sleep(float(os.environ.get("FAKE_IMPORT_SLEEP", "0")))
class _Sched:
    def __init__(self):
        self.workers = {"tcp://%s:%d" % (sys.argv[3], 40000 + i): None for i in range(int(sys.argv[4]))}
        self.workers["tcp://10.1.1.1:1"] = None
class Client:
    def __init__(self, addr, timeout=10):
        self.t = None
    def _ready(self):
        if self.t is None:
            self.t = time.time()
        return os.environ.get("FAKE_POLL_RC") != "1" and time.time() - self.t >= float(os.environ.get("FAKE_POLL_SLEEP", "0"))
    def scheduler_info(self):
        with open(os.environ["FAKELAB_DIR"] + "/poll.log", "a") as f:
            f.write("scheduler_info called\n")
        return {"workers": _Sched().workers if self._ready() else {}}
    def run_on_scheduler(self, fn, **kw):
        if not self._ready():
            return 0
        return fn(dask_scheduler=_Sched(), **kw)
EOF3
  for pc in "$d"/pc*; do
    mkdir -p "$pc/pipedpeer-lab/venv/bin"
    # the real poller runs, on the fake ray and distributed modules below
    cat >"$pc/pipedpeer-lab/venv/bin/python" <<'EOF2'
#!/usr/bin/env bash
if [[ "${1:-}" == "-" ]]; then
  echo "poll $*" >>"$FAKELAB_DIR/poll.log"
  PYTHONPATH="$FAKELAB_DIR/fakepy" exec python3 "$@"
fi
cat >/dev/null
exit 0
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
  export LAB_RESULTS="$d" FAKE_POLL_RC=1 E19_STORE_PATH='' E19_JOIN_WAIT_S=2
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems ray
) >"$d/e19.log" 2>&1 || pnote "e19 ray exit $?"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ -n "$row" ]] || pnote "no join row"
[[ -z "$(awk -F, '{print $5}' <<<"$row")" ]] || pnote "join_s not empty when not ready: $row"
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
echo "{\"msg\":\"peer took the closure\",\"store\":\"/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other\",\"secs\":1.5}" >"$d/pc00/.local/state/pipedpeer/daemon.log"
mkdir -p "$d/pc01/.local/share/pipedpeer/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other"
mkjob "$d" oldjob 2020-01-01T00:00:00.123456789Z /nix/store/yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-old
mkjob "$d" thisjob 2099-01-01T00:00:00.123456789Z "$sp"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  unset E19_STORE_PATH
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 2 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $? ($(tail -5 "$d/e19.log"))"
[[ ! -e "$d/pc01/.local/share/pipedpeer/nix/store/abcdefghijklmnopqrstuvwxyz012345-env" ]] || pnote "closure not purged on the joiner"
[[ -e "$d/pc01/.local/share/pipedpeer/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other" ]] || pnote "another closure, named by the daemon log, was purged"
rows="$(grep -c ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ "$rows" == 2 ]] || pnote "expected 2 join rows, got $rows"
bad_warm="$(grep ',join,' "$d/analysis-input/e19.csv" | awk -F, '$10 != "0"' || true)"
[[ -z "$bad_warm" ]] || pnote "joiner_warm not 0 in: $bad_warm"
verdict "closure path from this run's job record, not the daemon log: purged before each repeat, joiner_warm 0 in both"

# ---------------------------------------------------------------------- Case 15: no store path -> joiner_warm empty, logged
d="$(newdir)"
sp="/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other"
mkdir -p "$d/pc00/.local/state/pipedpeer" "$d/pc01/.local/share/pipedpeer/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other"
echo "{\"msg\":\"peer took the closure\",\"store\":\"$sp\",\"secs\":1.5}" >"$d/pc00/.local/state/pipedpeer/daemon.log"
mkjob "$d" oldjob 2020-01-01T00:00:00Z "$sp"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  unset E19_STORE_PATH
  cd "$REPO"
  bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems pipedpeer
) >"$d/e19.log" 2>&1 || pnote "e19 exit $?"
row="$(grep ',join,' "$d/analysis-input/e19.csv" 2>/dev/null || true)"
[[ -n "$row" && -z "$(awk -F, '{print $10}' <<<"$row")" ]] || pnote "joiner_warm should be empty (unknown): $row"
[[ -e "$d/pc01/.local/share/pipedpeer/nix/store/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-other" ]] || pnote "an old job's closure was purged"
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
grep -q ',join_aborted,1,.*invite_failed' "$d/analysis-input/e19.csv" 2>/dev/null || pnote "aborted repeat not counted: $(cat "$d/analysis-input/e19.csv")"
verdict "L2 empty invite code: repeat aborted, no join row, one counted join_aborted row"


# Helpers for the rows below
csv_of() { printf '%s' "$1/analysis-input/e19.csv"; }
run_e19() { # <d> <n> <e19 args...>; env is set by the caller
  local d="$1" n="$2"; shift 2
  ( setup_lab "$d" "$n"; [[ -z "${WITH_VENV:-}" ]] || mkvenv "$d"
    if [[ -n "${NIX_STUB:-}" ]]; then printf '#!/usr/bin/env bash\n%s\n' "$NIX_STUB" >"$d/bin/nix-store"; chmod +x "$d/bin/nix-store"; fi; export LAB_RESULTS="$d"; cd "$REPO"; bash "$CAMP/e19.sh" "$@" ) >"$d/e19.log" 2>&1
}
col() { awk -F, -v c="$2" -v k="$3" '$2 == k { print $c }' "$1" 2>/dev/null || true; }
SP=/nix/store/abcdefghijklmnopqrstuvwxyz012345-env
RELP=abcdefghijklmnopqrstuvwxyz012345-env

# ---------------------------------------------------------------------- H2 + opposite
d="$(newdir)"
WITH_VENV=1 run_e19 "$d" 2 --n 1 --add 1 --repeats 2 --systems ray || pnote "exit $?: $(tail -3 "$d/e19.log")"
seq_got="$(grep '^cluster up' "$d/cluster.log" 2>/dev/null | awk '{print $3}' | tr '\n' ' ' || true)"
[[ "$seq_got" == "2 1 1 " ]] || pnote "cluster ups, want 2 1 1 (N+ADD once, then N before baseline and repeat 2), got: $seq_got"
stops="$(grep -c '^pc01 stop' "$d/order.log" || true)"
((stops >= 2)) || pnote "joiner stopped $stops times, want one per repeat before its start"
last_stop="$(grep -n '^pc01 stop' "$d/order.log" 2>/dev/null | tail -1 | cut -d: -f1 || true)"
last_up="$(grep -n '^cluster up 1' "$d/order.log" 2>/dev/null | tail -1 | cut -d: -f1 || true)"
[[ -n "$last_stop" && -n "$last_up" ]] && ((last_stop < last_up)) || pnote "joiner stop not before cluster up N: $(tr '\n' ';' <"$d/order.log")"
verdict "H2 repeat 2 stops the joiners again and starts from a fresh N cluster (M10: the head restart clears the old node)"

d="$(newdir)"
WITH_VENV=1 run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
seq_got="$(grep '^cluster up' "$d/cluster.log" 2>/dev/null | awk '{print $3}' | tr '\n' ' ' || true)"
[[ "$seq_got" == "2 1 " ]] || pnote "one repeat: want ups 2 1, got: $seq_got"
verdict "H2 opposite: one repeat, no extra cluster up N"

# ---------------------------------------------------------------------- H3
d="$(newdir)"
store="$d/pc01/.local/share/pipedpeer/nix/store"
mkdir -p "$store/$RELP"
chmod 555 "$store"
E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?: $(tail -3 "$d/e19.log")"
chmod 755 "$store"
[[ "$(col "$(csv_of "$d")" 10 join)" == 1 ]] || pnote "purge failed on disk, want joiner_warm 1: $(cat "$(csv_of "$d")")"
verdict "H3 closure still on the joiner's disk with its daemon stopped: joiner_warm 1, not 0"

d="$(newdir)"
FAKELAB_FAIL=pc01 E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?: $(tail -3 "$d/e19.log")"
row="$(grep ',join,' "$(csv_of "$d")" 2>/dev/null || true)"
[[ -n "$row" ]] || pnote "no row for the unreachable joiner"
[[ -z "$(col "$(csv_of "$d")" 10 join)" ]] || pnote "joiner_warm must be empty when the joiner cannot answer: $row"
verdict "H3 opposite: joiner cannot be asked: joiner_warm empty"

# ---------------------------------------------------------------------- M1, M2
d="$(newdir)"
WITH_VENV=1 FAKE_IMPORT_SLEEP=2 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?: $(tail -3 "$d/e19.log")"
js="$(col "$(csv_of "$d")" 5 join)"
python3 -c "import sys; assert 0 <= float(sys.argv[1]) < 1.5" "${js:-x}" 2>/dev/null || pnote "join_s '$js' holds the 2 s import"
verdict "M1 ray: join_s excludes the poller's own import time"

d="$(newdir)"
WITH_VENV=1 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems dask || pnote "exit $?: $(tail -3 "$d/e19.log")"
if grep -q "scheduler_info called" "$d/poll.log" 2>/dev/null; then pnote "poller serialises the worker list"; fi
[[ "$(col "$(csv_of "$d")" 11 join)" == ok ]] || pnote "dask status: $(cat "$(csv_of "$d")")"
verdict "M2 dask: the poll asks the scheduler for one count, never the full worker list"

# ---------------------------------------------------------------------- M3 + opposite
d="$(newdir)"
WITH_VENV=1 FAKE_POLL_RC=1 E19_JOIN_WAIT_S=2 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
[[ "$(col "$(csv_of "$d")" 11 join)" == timeout ]] || pnote "want join_status timeout: $(cat "$(csv_of "$d")")"
verdict "M3 a join that never gets ready: join_status timeout, so slow joins stay counted"

d="$(newdir)"
WITH_VENV=1 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
[[ "$(col "$(csv_of "$d")" 11 join)" == ok ]] || pnote "want join_status ok: $(cat "$(csv_of "$d")")"
verdict "M3 opposite: a join that gets ready: join_status ok"

# ---------------------------------------------------------------------- M4 + opposite
d="$(newdir)"
WITH_VENV=1 FAKE_POLL_ERROR=ray-version-mismatch E19_JOIN_WAIT_S=2 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
grep -q "ray-version-mismatch" "$d/campaign.log" || pnote "the poll's reason is not in campaign.log"
verdict "M4 the join poll's reason reaches the campaign log"

d="$(newdir)"
WITH_VENV=1 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
if grep -q "join log" "$d/campaign.log"; then pnote "join log printed for a good join"; fi
verdict "M4 opposite: a good join logs no join output"

# ---------------------------------------------------------------------- M5 interrupted keeps rows, M6 INT stops the script
d="$(newdir)"
db_dir="$d/pc01/.local/share/pipedpeer"
mkdir -p "$db_dir"
echo "original_nodes_db" >"$db_dir/nodes.db"
rc=0
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d" FAKE_RUN_SLEEP=2
  with_sigint bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 3 --systems pipedpeer &
  pid=$!
  deadline=$((SECONDS + 40))
  while ((SECONDS < deadline)); do
    grep -q ',join,1,' "$d/analysis-input/e19.csv" 2>/dev/null && break
    sleep 0.1
  done
  grep -q ',join,1,' "$d/analysis-input/e19.csv" 2>/dev/null && touch "$d/saw_row"
  kill -INT "$pid"
  rc=0; wait "$pid" || rc=$?
  echo "$rc" >"$d/rc"
) >"$d/e19.log" 2>&1 || true
[[ -f "$d/saw_row" ]] || pnote "repeat 1's row was not on disk while the script ran"
[[ "$(cat "$d/rc" 2>/dev/null)" == 130 ]] || pnote "exit after INT: $(cat "$d/rc" 2>/dev/null), want 130"
[[ "$(grep -c ',join,3,' "$(csv_of "$d")" 2>/dev/null || true)" == 0 ]] || pnote "script went on after INT"
[[ "$(cat "$db_dir/nodes.db" 2>/dev/null)" == original_nodes_db ]] || pnote "nodes.db not restored"
[[ ! -e "$db_dir/nodes.db.aside" ]] || pnote ".aside left behind"
verdict "M5 M6 INT mid-campaign: exits 130, repeat 1's rows kept, nodes.db restored"

d="$(newdir)"
export WITH_VENV=1
run_e19 "$d" 2 --n 1 --add 1 --repeats 2 --systems ray || pnote "first exit $?"
before="$(wc -l <"$(csv_of "$d")" || true)"
ups_before="$(grep -c '^cluster up' "$d/cluster.log" || true)"
( export FAKE_RUN_SLEEP=0; run_e19 "$d" 2 --n 1 --add 1 --repeats 2 --systems ray ) || pnote "second exit"
after="$(wc -l <"$(csv_of "$d")" || true)"
unset WITH_VENV
[[ "$before" == "$after" && "$before" -ge 4 ]] || pnote "rerun changed the row count, or too few rows: $before -> $after"
verdict "M5 rerun of a finished campaign: no duplicate rows"

# ---------------------------------------------------------------------- M6 existing .aside survives
d="$(newdir)"
db_dir="$d/pc01/.local/share/pipedpeer"
mkdir -p "$db_dir"
echo "from_an_interrupted_run" >"$db_dir/nodes.db"
echo "the_real_original" >"$db_dir/nodes.db.aside"
run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(cat "$db_dir/nodes.db" 2>/dev/null)" == the_real_original ]] || pnote "original lost: $(cat "$db_dir/nodes.db" 2>/dev/null)"
verdict "M6 an .aside left by an earlier run is the original and is not overwritten"

d="$(newdir)"
db_dir="$d/pc01/.local/share/pipedpeer"
mkdir -p "$db_dir"
echo "orig" >"$db_dir/nodes.db"
echo "orig-wal" >"$db_dir/nodes.db-wal"
run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(cat "$db_dir/nodes.db" 2>/dev/null)" == orig ]] || pnote "nodes.db not restored"
[[ "$(cat "$db_dir/nodes.db-wal" 2>/dev/null)" == orig-wal ]] || pnote "WAL file not restored with the db"
verdict "M6 opposite: a plain run restores nodes.db and its WAL"

# ---------------------------------------------------------------------- M7 + opposite
d="$(newdir)"
FAKE_CONNECT_255=1 E19_JOIN_WAIT_S=3 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
got="$(grep -c 'pc01 pipedpeer connect' "$d/pipedpeer.log" || true)"
[[ "$got" == 1 ]] || pnote "connect ran $got times, want 1"
verdict "M7 an ssh drop during the join does not run the join again"

d="$(newdir)"
E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
got="$(grep -c 'pc01 pipedpeer connect' "$d/pipedpeer.log" || true)"
[[ "$got" == 1 ]] || pnote "connect ran $got times, want 1"
[[ "$(col "$(csv_of "$d")" 11 join)" == ok ]] || pnote "status: $(cat "$(csv_of "$d")")"
verdict "M7 opposite: a normal join runs once"

# ---------------------------------------------------------------------- M8
d="$(newdir)"
FAKE_RUN_FAIL_FROM=1 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ ! -f "$d/pc01/.connected" ]] || pnote "joined with no baseline"
grep -q ',join_aborted,1,.*no_baseline' "$(csv_of "$d")" 2>/dev/null || pnote "no counted row: $(cat "$(csv_of "$d")")"
grep -q 'baseline' "$d/campaign.log" || pnote "baseline failure not logged"
verdict "M8 failed baseline: no invented 60 s join, repeat counted as no_baseline, logged"

d="$(newdir)"
E19_START_WAIT_S=0 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ ! -f "$d/pc01/.connected" ]] || pnote "joined a run never seen running"
grep -q ',join_aborted,1,.*run_not_seen' "$(csv_of "$d")" 2>/dev/null || pnote "no counted row: $(cat "$(csv_of "$d")")"
verdict "M8 run never seen running: no join fires, repeat counted"

d="$(newdir)"
FAKE_RUN_FAIL_FROM=2 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
! grep -q ',join,' "$(csv_of "$d")" 2>/dev/null || pnote "join row from a failed run: $(cat "$(csv_of "$d")")"
grep -q ',join_aborted,1,.*run_failed' "$(csv_of "$d")" 2>/dev/null || pnote "no counted row: $(cat "$(csv_of "$d")")"
grep -q 'run_failed\|exit\|failed' "$d/campaign.log" || pnote "failed run not logged"
verdict "M8 join run fails: no join row, counted as run_failed"

d="$(newdir)"
E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
python3 -c "import sys; float(sys.argv[1])" "$(col "$(csv_of "$d")" 7 join)" 2>/dev/null || pnote "no wall_s: $(cat "$(csv_of "$d")")"
verdict "M8 opposite: a good run keeps its join row and wall_s"

# ---------------------------------------------------------------------- M11 / L3 + opposite
d="$(newdir)"
# the joiner's usable-CPU probe runs on this host: fixed at 8 with no quota
PP_NPROC=8 PP_CG_ROOT="$d/no-cgroup" PP_PROC_CGROUP=/dev/null WITH_VENV=1 E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems dask || pnote "exit $?: $(tail -3 "$d/e19.log")"
grep -q -- '--nworkers 8 ' "$d/units.log" 2>/dev/null || pnote "--nworkers not from hosts.tsv: $(cat "$d/units.log" 2>/dev/null)"
grep -qE '^poll - dask 10\.99\.0\.10 10\.99\.0\.11 8 ' "$d/poll.log" 2>/dev/null || pnote "poll want not 8: $(cat "$d/poll.log" 2>/dev/null)"
verdict "M11 dask workers come from hosts.tsv cores (8), the poll wants 8"

d="$(newdir)"
( setup_lab "$d" 2; mkvenv "$d"
  awk -F'\t' -v OFS='\t' '$1 == "pc01" { $5 = "" } { print }' "$d/hosts.tsv" >"$d/hosts.tsv.new" && mv "$d/hosts.tsv.new" "$d/hosts.tsv"
  export LAB_RESULTS="$d" E19_STORE_PATH=
  cd "$REPO"; bash "$CAMP/e19.sh" --n 1 --add 1 --repeats 1 --systems dask ) >"$d/e19.log" 2>&1 || pnote "exit $?: $(tail -3 "$d/e19.log")"
if grep -q -- '--nworkers' "$d/units.log" 2>/dev/null; then pnote "worker started with no core count: $(cat "$d/units.log")"; fi
[[ "$(col "$(csv_of "$d")" 11 join)" == no_cores ]] || pnote "want status no_cores: $(cat "$(csv_of "$d")")"
verdict "M11 L3 joiner with no core count: no broken command, row says no_cores"

# ---------------------------------------------------------------------- L1 + opposite
d="$(newdir)"
mkdir -p "$d/analysis-input"
printf 'system,kind,i,pc,join_s,first_work_s,wall_s,result_ok\nray,baseline,0,,,,1.0,1\n' >"$(csv_of "$d")"
E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
[[ "$(head -1 "$(csv_of "$d")")" == *,join_status,purged_bytes,closure_bytes ]] || pnote "header: $(head -1 "$(csv_of "$d")")"
[[ "$(grep -c 'wall_s' "$(csv_of "$d")")" == 1 ]] || pnote "two headers in one file"
ls "$d/analysis-input/"e19.csv.old-* >/dev/null 2>&1 || pnote "old-format file not kept aside"
verdict "L1 a CSV of an older column order is moved aside, never mixed"

d="$(newdir)"
E19_STORE_PATH='' run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems ray || pnote "exit $?"
if ls "$d/analysis-input/"e19.csv.old-* >/dev/null 2>&1; then pnote "fresh file moved aside"; fi
verdict "L1 opposite: the current column order stays in place"

# ---------------------------------------------------------------------- L4 + opposite
d="$(newdir)"
store="$d/pc01/.local/share/pipedpeer/nix/store"
mkdir -p "$store/keep"
E19_STORE_PATH=/nix/store/ run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ -e "$store/keep" ]] || pnote "a hand-set /nix/store/ wiped the joiner's private store"
[[ -z "$(col "$(csv_of "$d")" 10 join)" ]] || pnote "joiner_warm not empty for a refused path"
grep -q 'E19_STORE_PATH' "$d/campaign.log" || pnote "refused path not logged"
verdict "L4 E19_STORE_PATH=/nix/store/: refused and logged, the private store stays"

d="$(newdir)"
store="$d/pc01/.local/share/pipedpeer/nix/store"
mkdir -p "$store/$RELP" "$store/keep"
E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ ! -e "$store/$RELP" ]] || pnote "valid closure not purged"
[[ -e "$store/keep" ]] || pnote "other paths went too"
[[ "$(col "$(csv_of "$d")" 10 join)" == 0 ]] || pnote "joiner_warm: $(cat "$(csv_of "$d")")"
verdict "L4 opposite: a real store path is purged, the rest stays, joiner_warm 0"

# ---------------------------------------------------------------------- FXR2-E2 defect 1: system-store purge through nix-store
RM_STUB='echo "$*" >>"$FAKELAB_DIR/nixstore.log"; [ "$1" = --delete ] && rm -rf "$2"'
d="$(newdir)"
mkdir -p "$d/sysstore/$RELP"
NIX_STUB="$RM_STUB" E19_SYS_STORE="$d/sysstore" E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ ! -e "$d/sysstore/$RELP" ]] || pnote "system-store closure not deleted"
grep -q -- "--delete $d/sysstore/$RELP" "$d/nixstore.log" 2>/dev/null || pnote "nix-store --delete not called"
[[ "$(col "$(csv_of "$d")" 10 join)" == 0 ]] || pnote "joiner_warm: $(cat "$(csv_of "$d")")"
verdict "E2-1 system-store path deleted through nix-store: absent, joiner_warm 0"

d="$(newdir)"
mkdir -p "$d/sysstore/$RELP"
NIX_STUB='echo "error: cannot delete path: still alive" >&2; exit 1' E19_SYS_STORE="$d/sysstore" E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ -e "$d/sysstore/$RELP" ]] || pnote "refused path vanished"
[[ "$(col "$(csv_of "$d")" 10 join)" == 1 ]] || pnote "refusal: want joiner_warm 1: $(cat "$(csv_of "$d")")"
grep -q 'still alive' "$d/campaign.log" || pnote "refusal reason not logged"
verdict "E2-1 nix-store --delete refused: reason logged, joiner_warm 1"

d="$(newdir)"
mkdir -p "$d/pc01/.local/share/pipedpeer/nix/store/$RELP" "$d/sysstore"
NIX_STUB="$RM_STUB" E19_SYS_STORE="$d/sysstore" E19_STORE_PATH="$SP" run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ ! -e "$d/pc01/.local/share/pipedpeer/nix/store/$RELP" ]] || pnote "private path not removed"
[[ ! -e "$d/nixstore.log" ]] || pnote "nix-store called for a private-store-only closure"
[[ "$(col "$(csv_of "$d")" 10 join)" == 0 ]] || pnote "joiner_warm: $(cat "$(csv_of "$d")")"
verdict "E2-1 private-store path only: removed with rm, nix-store untouched, joiner_warm 0"

# ---------------------------------------------------------------------- FXR2-E2 defect 2: samples before join_ns
d="$(newdir)"; out_f="$d/samples.txt"; join_ns=1000000000000
for i in -3 -2 -1 0 1 2 3 4 5; do echo "$((join_ns + i * 1000000000)) 6.0" >>"$out_f"; done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || pnote "sampler failed: $res"
[[ "$res" == "0.0" || "$res" == "0" ]] || pnote "straddling streak: want 0.0 from the first in-window sample, got $res"
verdict "E2-2 streak straddling t0: measured from the first in-window sample"

d="$(newdir)"; out_f="$d/samples.txt"
for i in -6 -5 -4 -3 -2 -1; do echo "$((join_ns + i * 1000000000)) 6.0" >>"$out_f"; done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || true
[[ "$res" != -* ]] || pnote "all samples before t0: want no value, got $res"
verdict "E2-2 all samples before t0: no value, not negative"

d="$(newdir)"; out_f="$d/samples.txt"
for i in 2 3 4 5 6 7; do echo "$((join_ns + i * 1000000000)) 6.0" >>"$out_f"; done
res="$(python3 "$WORKLOADS/cpu_sampler.py" --streak "$out_f" "$join_ns" 8 2>&1)" || pnote "sampler failed: $res"
[[ "$res" == "2.0" || "$res" == "2" ]] || pnote "streak after t0: want 2.0, got $res"
verdict "E2-2 streak fully after t0: unchanged"

# ---------------------------------------------------------------------- FXR2-E2 defect 3: connect exit 2
st() { col "$(csv_of "$1")" 11 join; }
d="$(newdir)"
FAKE_CONNECT_RC=2 FAKE_STATUS_ROWS=2 E19_CONFIRM_WAIT_S=1 run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(st "$d")" == partial ]] || pnote "exit 2 confirmed: want partial: $(cat "$(csv_of "$d")")"
[[ -n "$(col "$(csv_of "$d")" 5 join)" ]] || pnote "partial row has no join_s"
grep -q 'partly connected' "$d/campaign.log" || pnote "partial join not warned"
verdict "E2-3 exit 2 and the cluster lists the node: partial with join_s, warned"

d="$(newdir)"
FAKE_CONNECT_RC=2 FAKE_STATUS_ROWS=1 E19_CONFIRM_WAIT_S=1 run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(st "$d")" == failed ]] || pnote "exit 2 unconfirmed: want failed: $(cat "$(csv_of "$d")")"
[[ -z "$(col "$(csv_of "$d")" 5 join)" ]] || pnote "failed row has a join_s"
verdict "E2-3 exit 2, cluster does not list the node: failed"

d="$(newdir)"
FAKE_CONNECT_RC=1 FAKE_STATUS_ROWS=2 E19_CONFIRM_WAIT_S=1 run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(st "$d")" == failed ]] || pnote "exit 1: want failed even when listed: $(cat "$(csv_of "$d")")"
verdict "E2-3 exit 1: failed"

d="$(newdir)"
FAKE_STATUS_ROWS=2 run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ "$(st "$d")" == ok ]] || pnote "exit 0: want ok: $(cat "$(csv_of "$d")")"
verdict "E2-3 exit 0: ok, unchanged"

# ---------------------------------------------------------------------- FIX-V5d: purge the closure's requisites
# Fake nix-store: -qR lists dependencies first (the top path last), --size reads
# <path>.size, --delete refuses the paths named in $FAKELAB_DIR/refuse.
CLOSURE_STUB='case "$1" in
  -qR) cat "$2.reqs" 2>/dev/null || echo "$2" ;;
  -q) cat "$3.size" ;;
  --delete) echo "$2" >>"$FAKELAB_DIR/nixstore.log"
    if grep -qx -- "$2" "$FAKELAB_DIR/refuse" 2>/dev/null; then echo "error: cannot delete path $2: still alive" >&2; exit 1; fi
    rm -rf "$2" ;;
esac'
mk_closure() { # <d> <top size> <dep1 size> <dep2 size>
  local s="$1/sysstore" t="$RELP"
  mkdir -p "$s/$t" "$s/dep1-x" "$s/dep2-x"
  echo "$2" >"$s/$t.size"; echo "$3" >"$s/dep1-x.size"; echo "$4" >"$s/dep2-x.size"
  printf '%s\n' "$s/dep2-x" "$s/dep1-x" "$s/$t" >"$s/$t.reqs"
}
run_closure() { NIX_STUB="$CLOSURE_STUB" E19_SYS_STORE="$1/sysstore" E19_STORE_PATH="$SP" run_e19 "$1" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?: $(tail -3 "$1/e19.log")"; }

d="$(newdir)"; mk_closure "$d" 10 20 70
run_closure "$d"
[[ ! -e "$d/sysstore/dep1-x" && ! -e "$d/sysstore/dep2-x" && ! -e "$d/sysstore/$RELP" ]] || pnote "requisites remain"
[[ "$(head -1 "$d/nixstore.log")" == "$d/sysstore/$RELP" ]] || pnote "dependents not first: $(tr '\n' ' ' <"$d/nixstore.log")"
[[ "$(col "$(csv_of "$d")" 10 join)" == 0 && "$(col "$(csv_of "$d")" 12 join)" == 100 && "$(col "$(csv_of "$d")" 13 join)" == 100 ]] || pnote "want warm 0, bytes 100/100: $(cat "$(csv_of "$d")")"
verdict "V5d all requisites deletable: dependents first, warm 0, purged_bytes = closure_bytes"

d="$(newdir)"; mk_closure "$d" 10 80 10
echo "$d/sysstore/dep1-x" >"$d/refuse"
run_closure "$d"
[[ -e "$d/sysstore/dep1-x" && ! -e "$d/sysstore/dep2-x" ]] || pnote "wrong paths removed"
[[ "$(col "$(csv_of "$d")" 10 join)" == 1 && "$(col "$(csv_of "$d")" 12 join)" == 20 && "$(col "$(csv_of "$d")" 13 join)" == 100 ]] || pnote "want warm 1, bytes 20/100: $(cat "$(csv_of "$d")")"
grep -q 'refused.*dep1-x.*still alive' "$d/campaign.log" || pnote "refusal not logged with the path"
verdict "V5d shared dependency refused, under 90% removed: warm 1, refusal logged"

d="$(newdir)"; mk_closure "$d" 50 5 45
echo "$d/sysstore/dep1-x" >"$d/refuse"
run_closure "$d"
[[ "$(col "$(csv_of "$d")" 10 join)" == 0 && "$(col "$(csv_of "$d")" 12 join)" == 95 && "$(col "$(csv_of "$d")" 13 join)" == 100 ]] || pnote "want warm 0, bytes 95/100: $(cat "$(csv_of "$d")")"
grep -q 'refused.*dep1-x' "$d/campaign.log" || pnote "refusal not logged"
verdict "V5d refused dependency but 95% removed: warm 0, bytes 95/100"

d="$(newdir)"
store="$d/pc01/.local/share/pipedpeer/nix/store"; mkdir -p "$store/keep" "$d/sysstore/keep"
NIX_STUB="$CLOSURE_STUB" E19_SYS_STORE="$d/sysstore" E19_STORE_PATH=/nix/store/ run_e19 "$d" 2 --n 1 --add 1 --repeats 1 --systems pipedpeer || pnote "exit $?"
[[ -e "$store/keep" && -e "$d/sysstore/keep" && ! -e "$d/nixstore.log" ]] || pnote "something was deleted for /nix/store/"
[[ -z "$(col "$(csv_of "$d")" 10 join)" && -z "$(col "$(csv_of "$d")" 12 join)" && -z "$(col "$(csv_of "$d")" 13 join)" ]] || pnote "warm and bytes must be empty: $(cat "$(csv_of "$d")")"
verdict "V5d /nix/store/ as the path: nothing deleted, warm and bytes empty"

echo "TOTAL: pass=$pass failed=$failed"
exit "$failed"
