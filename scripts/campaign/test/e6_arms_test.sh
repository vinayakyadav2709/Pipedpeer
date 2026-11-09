#!/usr/bin/env bash
# Case table for E6 arms (unit E6F).
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
# Each fake lab resolves the real rsync from PATH; a PATH still holding an
# earlier fake lab would make its rsync stub exec itself forever.
ORIG_PATH="$PATH"
pass=0; failed=0; problems=()

DIRS_FILE="$(mktemp)"
cleanup() {
  local d
  while IFS= read -r d; do
    [[ -n "$d" ]] || continue
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
  d="$(mktemp -d "$STATE/e-e6arms-test.XXXXXX")"
  echo "$d" >>"$DIRS_FILE"
  echo "$d"
}

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

setup_lab() {
  local dir="$1" n="$2" i pc v="$1/verif"
  PATH="$ORIG_PATH"
  fakelab_init "$dir" "$n" || exit 90
  mkdir -p "$v"
  export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.2 LAB_POLL_S=1
  export CLUSTER_SH="$v/cluster.sh" METRICS_SH="$v/metrics.sh" UPLOAD_SH="$v/upload.sh"
  export E4_LOG_WAIT_S=2 E4_JOIN_S=4
  export LAB_NET_DEV='~/net_dev'
  export PATH="$v:$PATH"
  cat >"$v/pipedpeer" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; }
me="$(basename "$HOME")"
[[ -n "${FAKE_INT_RESET:-}" ]] || printf '%s pipedpeer %s\n' "$me" "$*" >>"$FAKELAB_DIR/pipedpeer.log"
add_rx() { [[ -f "$HOME/net_dev" ]] || return 0; awk -v b="$1" '$1=="eth0:" {$2+=b} {print}' "$HOME/net_dev" >"$HOME/net_dev.new" && mv "$HOME/net_dev.new" "$HOME/net_dev"; }
case "${1:-}" in
  status)
    calls=0; [[ -f "$HOME/.status_calls" ]] && calls="$(cat "$HOME/.status_calls")"
    calls=$((calls + 1)); echo "$calls" >"$HOME/.status_calls"
    jobs=0; ((calls < ${FAKE_JOBS_POLLS:-0})) && jobs=1
    f="$FAKELAB_DIR/status.$me"; [[ -f "$f" ]] || f="$FAKELAB_DIR/status.txt"
    [[ -f "$f" ]] || { echo "pipedpeer stub: no status file" >&2; exit 1; }
    sed "s/@JOBS@/$jobs/" "$f" ;;
  start) echo "${PIPEDPEER_POOL_SPLIT:-}" >"$HOME/.split" ;;
  stop) rm -f "$HOME/.split" ;;
  run)
    if [[ -n "${FAKE_RUN_SLEEP:-}" ]]; then sleep "$FAKE_RUN_SLEEP" & wait $!; fi
    [[ -z "${FAKE_RUN_FAIL:-}" ]] || { echo "boom" >&2; exit 1; }
    echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000"
    jd="$HOME/.local/share/pipedpeer/jobs/$(date +%s%N)"; mkdir -p "$jd"
    if [[ -f "$FAKELAB_DIR/receipt.json" ]]; then cp "$FAKELAB_DIR/receipt.json" "$jd/receipt.json"; fi
    add_rx 7000 ;;
  jobs) echo '{"run_id":"x","names":{"m0":"pc00.lab"},"steps":[]}' ;;
  *) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
EOF
  chmod +x "$v/pipedpeer"
  cat >"$v/cluster.sh" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "cluster stub: no FAKELAB_DIR" >&2; exit 97; }
printf 'cluster %s\n' "$*" >>"$FAKELAB_DIR/cluster.log"
case "${1:-}" in
  up) mkdir -p "$LAB_RESULTS/logs/cluster-$2"; echo "CLUSTER-UP n=$2" ;;
  down) echo CLUSTER-DOWN ;;
esac
EOF
  chmod +x "$v/cluster.sh"
  cat >"$v/systemctl" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; }
printf '%s systemctl %s\n' "$(basename "$HOME")" "$*" >>"$FAKELAB_DIR/systemctl.log"
exit 0
EOF
  chmod +x "$v/systemctl"
  for s in metrics upload; do
    cat >"$v/$s.sh" <<EOF
#!/usr/bin/env bash
printf '$s %s\n' "\$*" >>"\$FAKELAB_DIR/$s.log"
EOF
    chmod +x "$v/$s.sh"
  done
  printf 'W1_ITEMS=10\n' >"$dir/sizes.env"
  printf '%-6s%-9s%-6s%-8s%-10s%-5s%-6s%s\n' NAME STATE PATH CPU 'MEM FREE' GPU JOBS NOTE >"$dir/status.txt"
  for ((i = 0; i < n; i++)); do
    pc="$(printf 'pc%02d' "$i")"
    printf '%-6s%-9s%-6s%-8s%-10s%-5s%-7s%s\n' "$pc" healthy self '8c 1%' '7.2 GiB' none '@JOBS@' - >>"$dir/status.txt"
    mkdir -p "$dir/$pc/pipedpeer-lab/runs" "$dir/$pc/pipedpeer-lab/venv/bin" "$dir/$pc/pipedpeer-lab/work/baselines"
    cp "$REPO/scripts/campaign/stamp.py" "$dir/$pc/pipedpeer-lab/work/stamp.py" 2>/dev/null || true
    cat >"$dir/$pc/pipedpeer-lab/venv/bin/python" <<'PY'
#!/usr/bin/env bash
if [[ -n "${FAKE_RUN_SLEEP:-}" ]]; then sleep "$FAKE_RUN_SLEEP" & wait $!; fi
[[ -z "${FAKE_RUN_FAIL:-}" ]] || { echo "boom" >&2; exit 1; }
echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000"
PY
    chmod +x "$dir/$pc/pipedpeer-lab/venv/bin/python"
  done
  printf 'id,offset_ms,rtt_ms,measured_at\n' >"$dir/clocks.csv"
  for ((i = 0; i < n; i++)); do printf 'pc%02d,0,1,x\n' "$i" >>"$dir/clocks.csv"; done
  : >"$dir/campaign.log"
  printf '{"remote_items":120,"local_items":40}' >"$dir/receipt.json"
}

# ---- Case 1: e6_record result_ok true -> ...,1
d="$(newdir)"
export LAB_RESULTS="$d"
printf '{"run_id":"E6-W1-pipedpeer-n2-r1","wall_s":7.5,"result_ok":true}\n' >"$d/runs.jsonl"
# shellcheck source=/dev/null
source "$CAMP/e6.sh"
rec="$(e6_record E6-W1-pipedpeer-n2-r1)"
[[ "$rec" == "7.5,1" ]] || pnote "true: got [$rec]"
verdict "run with result_ok true -> ...,1"

# ---- Case 2: result_ok false -> ...,0
d="$(newdir)"
export LAB_RESULTS="$d"
printf '{"run_id":"E6-W1-pipedpeer-n2-r1","wall_s":7.5,"result_ok":false}\n' >"$d/runs.jsonl"
source "$CAMP/e6.sh"
rec="$(e6_record E6-W1-pipedpeer-n2-r1)"
[[ "$rec" == "7.5,0" ]] || pnote "false: got [$rec]"
verdict "run with result_ok false -> ...,0"

# ---- Case 3: null -> empty field, not 0
d="$(newdir)"
export LAB_RESULTS="$d"
printf '{"run_id":"E6-W1-pipedpeer-n2-r1","wall_s":7.5,"result_ok":null}\n' >"$d/runs.jsonl"
source "$CAMP/e6.sh"
rec="$(e6_record E6-W1-pipedpeer-n2-r1)"
[[ "$rec" == "7.5," ]] || pnote "null: got [$rec]"
verdict "run with null result_ok -> empty field, not 0"

# ---- Case 4: default --systems
d="$(newdir)"
setup_lab "$d" 2
run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e6.sh" --dry-run --n 2 2>&1)" || pnote "e6 dry exit $?"
echo "$run" | grep -q "systems=pipedpeer,ray,ray-pool,dask" || pnote "default systems: [$run]"
verdict "default --systems -> pipedpeer ray ray-pool dask"

# ---- Case 5: ray-pool E1 median uses ray's
d="$(newdir)"
export LAB_RESULTS="$d"
printf '{"run_id":"E1-W1-ray-n2-r1","experiment":"E1","workload":"W1","system":"ray","n":2,"repeat":1,"warmup":false,"wall_s":7.5,"result_ok":true}\n' >"$d/runs.jsonl"
export N=2
source "$CAMP/e6.sh"
med="$(e6_median_wall ray-pool)"
[[ "$med" == "7.5" ]] || pnote "median: got [$med]"
verdict "ray-pool E1 median asked -> ray's W1 median"

# ---- Case 6: --systems ray-pool kills lab-ray, ray-pool in CSV
d="$(newdir)"
setup_lab "$d" 2
for w in 4 4 4; do
  printf '{"run_id":"E1-W1-ray-n2-r%s","experiment":"E1","workload":"W1","system":"ray","n":2,"repeat":1,"warmup":false,"wall_s":%s,"result_ok":true}\n' "$w" "$w" >>"$d/runs.jsonl"
done
export FAKE_RUN_SLEEP=6
(
  cd "$REPO"
  bash "$CAMP/e6.sh" --n 2 --only kill --systems ray-pool --kills 1
) >"$d/out" 2>&1 || pnote "e6 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
csv="$d/analysis-input/e6.csv"
row="$(sed -n 2p "$csv" 2>/dev/null || true)"
[[ "$row" == ray-pool,kill,* ]] || pnote "csv system not ray-pool: [$row]"
grep -q 'pc01 systemctl --user kill -s SIGKILL lab-ray' "$d/systemctl.log" 2>/dev/null || pnote "no SIGKILL lab-ray: $(cat "$d/systemctl.log" 2>/dev/null)"
verdict "--systems ray-pool kills -> lab-ray killed, ray-pool in CSV"

# ---- Case 7: --systems pipedpeer (ordinary use) no Ray arm starts
d="$(newdir)"
setup_lab "$d" 2
export FAKE_RUN_SLEEP=2
(
  cd "$REPO"
  bash "$CAMP/e6.sh" --n 2 --only kill --systems pipedpeer --kills 1
) >"$d/out" 2>&1 || pnote "e6 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
if grep -q 'lab-ray' "$d/systemctl.log" 2>/dev/null; then pnote "Ray arm started: $(cat "$d/systemctl.log")"; fi
verdict "--systems pipedpeer (ordinary use) -> no Ray arm starts"

echo "TOTAL: pass=$pass failed=$failed"
exit "$failed"