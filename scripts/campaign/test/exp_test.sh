#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034
# S12-S15 case table (e4, e5, e6, e11) on the fake lab. Nothing touches a real
# PC, systemd manager or NIC: pipedpeer, systemctl, journalctl, ip and the
# venv python are stubs first on PATH, the cluster/metrics/upload siblings are
# stubs named through CLUSTER_SH / METRICS_SH / UPLOAD_SH, and the NIC counters
# are a file (LAB_NET_DEV). Every row runs the real script; a row fails if the
# script only prints what it would do.
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
pass=0; failed=0; problems=()
# newdir runs in $(...): an array append there is lost, so dirs go to a file
DIRS_FILE="$(mktemp)"
cleanup() {
  local d e p
  while IFS= read -r d; do
    [[ -n "$d" ]] || continue
    pkill -f -- "$d/" 2>/dev/null || true
    for e in /proc/[0-9]*/environ; do
      p="${e#/proc/}"; p="${p%/environ}"
      [[ "$p" == "$$" || "$p" == "$BASHPID" ]] && continue
      if tr '\0' '\n' 2>/dev/null <"$e" | grep -qE -- "=$d([/:]|\$)"; then kill -KILL "$p" 2>/dev/null || true; fi
    done
    rm -rf "$d"
  done <"$DIRS_FILE"
  rm -f "$DIRS_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
ok() { pass=$((pass + 1)); echo "ok   $1"; }
bad() { failed=$((failed + 1)); echo "FAIL $1 -- $2"; }
pnote() { problems+=("$*"); }
verdict() { if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi; problems=(); }
newdir() { local d; d="$(mktemp -d "$STATE/e-exp-test.XXXXXX")"; mkdir -p "$d/home"; echo "$d" >>"$DIRS_FILE"; echo "$d"; }

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

# setup_lab <dir> <n>: fake lab + stubs. Call inside a subshell.
setup_lab() {
  local dir="$1" n="$2" i pc v="$1/verif"
  fakelab_init "$dir" "$n" || exit 90
  mkdir -p "$v"
  export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.2 LAB_POLL_S=1
  export CLUSTER_SH="$v/cluster.sh" METRICS_SH="$v/metrics.sh" UPLOAD_SH="$v/upload.sh"
  export E4_LOG_WAIT_S=2 E4_JOIN_S=4
  # shellcheck disable=SC2088  # the remote shell expands the tilde
  export LAB_NET_DEV='~/net_dev'
  cat >"$v/pipedpeer" <<'EOF'
#!/usr/bin/env bash
# test stub pipedpeer
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; }
me="$(basename "$HOME")"
[[ -n "${FAKE_INT_RESET:-}" ]] || printf '%s pipedpeer %s\n' "$me" "$*" >>"$FAKELAB_DIR/pipedpeer.log"
add_rx() { # add_rx <bytes>: grow eth0 rx in this PC's net_dev
  [[ -f "$HOME/net_dev" ]] || return 0
  awk -v b="$1" '$1=="eth0:" {$2+=b} {print}' "$HOME/net_dev" >"$HOME/net_dev.new" && mv "$HOME/net_dev.new" "$HOME/net_dev"
}
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
    receipt=""
    for a in "$@"; do case "$a" in PIPEDPEER_RECEIPT=*) receipt="${a#PIPEDPEER_RECEIPT=}" ;; esac; done
    receipt="${receipt/#\~/$HOME}"
    # real daemon: a receipt path the user names replaces its own, and the run
    # record then holds no other machine's steps (only the driver's script step)
    if [[ -n "$receipt" ]]; then : >"$HOME/.user_receipt"; else rm -f "$HOME/.user_receipt"; fi
    # a background start leaves SIGINT ignored; put the default back, as a service has it
    if [[ -z "${FAKE_INT_RESET:-}" ]]; then
      FAKE_INT_RESET=1 exec perl -e '$SIG{INT}="DEFAULT"; exec @ARGV' bash "$0" "$@"
    fi
    if [[ -n "${FAKE_RUN_SLEEP:-}" ]]; then sleep "$FAKE_RUN_SLEEP" & wait $!; fi
    [[ -z "${FAKE_RUN_FAIL:-}" ]] || { echo "boom" >&2; exit 1; }
    echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000"
    if [[ -z "${FAKE_NO_RECEIPT:-}" && -f "$FAKELAB_DIR/receipt.json" ]]; then
      if [[ -n "$receipt" ]]; then
        mkdir -p "$(dirname "$receipt")"; cp "$FAKELAB_DIR/receipt.json" "$receipt"
      else
        # the daemon keeps its own receipt in the run's job dir
        jd="$HOME/.local/share/pipedpeer/jobs/$(date +%s%N)"; mkdir -p "$jd"; cp "$FAKELAB_DIR/receipt.json" "$jd/receipt.json"
      fi
    fi
    if [[ "$(cat "$HOME/.split" 2>/dev/null || true)" == headcount && -z "${FAKE_NO_HEADCOUNT_LOG:-}" ]]; then
      mkdir -p "$HOME/.local/state/pipedpeer"
      echo "2026/11/08 12:00:00 [pool] plan (headcount): local=40" >>"$HOME/.local/state/pipedpeer/daemon.log"
    fi
    add_rx 7000 ;;
  jobs)
    t0="$(cat "$HOME"/pipedpeer-lab/runs/*/t0 2>/dev/null | tail -1)"
    [[ -n "$t0" ]] || t0=0
    # per PC: first compute starts (i+1) s after the run's t0; hostnames as in hosts.tsv
    printf '{"run_id":"x","names":{"m0":"pc00.lab","m1":"pc01.lab"},"steps":['
    if [[ -f "$HOME/.user_receipt" || -n "${FAKE_ONE_PC:-}" ]]; then
      printf '{"machine":"m0","node":"m0","chunk":0,"step":"script","start_ns":%s,"dur_ns":1000}]}\n' "$t0"
      exit 0
    fi
    # real daemon: work the origin runs itself has node "" (seen on the Docker lab)
    printf '{"machine":"m0","node":"","chunk":-1,"step":"compute","start_ns":%s,"dur_ns":1000},' "$((t0 + 1000000000))"
    printf '{"machine":"m0","node":"m1","chunk":1,"step":"env_send","start_ns":%s,"dur_ns":10,"bytes":5000},' "$((t0 + 100))"
    # a long run's timeline is hundreds of KB: one argv/env string is capped at 128 KB
    if [[ -n "${FAKE_BIG_TIMELINE:-}" ]]; then
      for ((i = 0; i < 3000; i++)); do
        printf '{"machine":"m1","node":"m1","chunk":%s,"step":"env_send","start_ns":%s,"dur_ns":10,"bytes":5000},' "$i" "$((t0 + 100))"
      done
    fi
    printf '{"machine":"m1","node":"m1","chunk":1,"step":"compute","start_ns":%s,"dur_ns":1000}]}\n' "$((t0 + 2000000000))" ;;
  *) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
EOF
  cat >"$v/systemctl" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; }
printf '%s systemctl %s\n' "$(basename "$HOME")" "$*" >>"$FAKELAB_DIR/systemctl.log"
exit 0
EOF
  cat >"$v/journalctl" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "journalctl stub: no FAKELAB_DIR" >&2; exit 97; }
exit 0
EOF
  cat >"$v/ip" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "ip stub: no FAKELAB_DIR" >&2; exit 97; }
echo "default via 10.99.0.1 dev eth0 proto dhcp metric 100"
EOF
  cat >"$v/cluster.sh" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "cluster stub: no FAKELAB_DIR" >&2; exit 97; }
printf 'cluster %s\n' "$*" >>"$FAKELAB_DIR/cluster.log"
case "${1:-}" in
  up) mkdir -p "$LAB_RESULTS/logs/cluster-$2"
      printf 'pipedpeer=%s\nray=%s\ndask=%s\n' "${FAKE_CORES:-15}" "${FAKE_CORES:-15}" "${FAKE_CORES:-15}" \
        >"$LAB_RESULTS/logs/cluster-$2/cores.txt"
      echo "CLUSTER-UP n=$2" ;;
  down) echo CLUSTER-DOWN ;;
esac
EOF
  for s in metrics upload; do
    cat >"$v/$s.sh" <<EOF
#!/usr/bin/env bash
printf '$s %s\n' "\$*" >>"\$FAKELAB_DIR/$s.log"
EOF
  done
  chmod +x "$v"/*
  export PATH="$v:$PATH"
  printf 'W1_ITEMS=10\nW1_WEAK_PER_PC=5\nW2_ITEMS=10\nW3_TREES=5\nT1_EPOCHS=1\nBENCH_PROCS=0\n' >"$dir/sizes.env"
  mkdir -p "$dir/reference"
  echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000" >"$dir/reference/W1-strong.txt"
  printf '%s' '{"remote_items":120,"local_items":40,"by_where":{"local":40,"peer:10.99.0.11:4242":40,"peer:10.99.0.12:4242":40,"peer:10.99.0.13:4242":40}}' >"$dir/receipt.json"
  printf '2.1.0\nrequests==2\n' >"$dir/requirements.txt"
  printf '%-6s%-9s%-6s%-8s%-10s%-5s%-6s%s\n' NAME STATE PATH CPU 'MEM FREE' GPU JOBS NOTE >"$dir/status.txt"
  for ((i = 0; i < n; i++)); do
    pc="$(printf 'pc%02d' "$i")"
    printf '%-6s%-9s%-6s%-8s%-10s%-5s%-7s%s\n' "$pc" healthy self '8c 1%' '7.2 GiB' none '@JOBS@' - >>"$dir/status.txt"
    mkdir -p "$dir/$pc/pipedpeer-lab/venv/bin" "$dir/$pc/pipedpeer-lab/work/baselines"
    cat >"$dir/$pc/pipedpeer-lab/venv/bin/python" <<'EOF'
#!/usr/bin/env bash
# test stub venv python
case "$*" in
  *"-m venv"*)
    mkdir -p "$3/bin"
    cat >"$3/bin/pip" <<'PIP'
#!/usr/bin/env bash
add=100; for a in "$@"; do [[ "$a" == -r ]] && add=5000; done
awk -v b="$add" '$1=="eth0:" {$2+=b} {print}' "$HOME/net_dev" >"$HOME/net_dev.new" && mv "$HOME/net_dev.new" "$HOME/net_dev"
PIP
    chmod +x "$3/bin/pip" ;;
  *e5_ray.py*)
    now="$(date +%s%N)"
    echo "E5-RAY node=10.99.0.10 start_ns=$((now + 1000000000))"
    echo "E5-RAY node=10.99.0.11 start_ns=$((now + 3000000000))"
    echo "E5-OK nodes=2" ;;
  *) echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000" ;;
esac
EOF
    chmod +x "$dir/$pc/pipedpeer-lab/venv/bin/python"
    printf 'Inter-|   Receive\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo: 1000 1 0 0 0 0 0 0 1000 1 0 0 0 0 0 0\n  eth0: 5000 1 0 0 0 0 0 0 1000 1 0 0 0 0 0 0\n' >"$dir/$pc/net_dev"
  done
  printf 'id,offset_ms,rtt_ms,measured_at\n' >"$dir/clocks.csv"
  for ((i = 0; i < n; i++)); do printf 'pc%02d,0,1,x\n' "$i" >>"$dir/clocks.csv"; done
  : >"$dir/campaign.log"
}

col() { # col <csv> <row-number-after-header> <col-number>
  awk -F, -v r="$2" -v c="$3" 'NR==r+1 {print $c}' "$1"
}
runs_count() { # runs_count <dir> <system>
  grep -cF "\"system\":\"$2\"" "$1/runs.jsonl" 2>/dev/null || true
}

# ---------------------------------------------------------------- dry runs
d="$(newdir)"; (setup_lab "$d" 3) >/dev/null
run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e4.sh" --dry-run 2>&1)" || pnote "e4 dry exit $?"
echo "$run" | grep -q "CAP pc00=8" || pnote "no pc00 cap"
echo "$run" | grep -q "CAP pc03=1" || pnote "no pc03 cap"
n="$(echo "$run" | grep -c '^RUN ')"
[[ "$n" == "20" ]] || pnote "runs $n != 20"
verdict "e4 --dry-run: 20 runs + caps"

run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e5.sh" --dry-run --n 2 2>&1)" || pnote "e5 dry exit $?"
echo "$run" | grep -q "E5-DRY-RUN" || pnote "no marker"
verdict "e5 --dry-run"

run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e6.sh" --dry-run --n 2 2>&1)" || pnote "e6 dry exit $?"
echo "$run" | grep -q "E6-DRY-RUN n=2" || pnote "no marker"
verdict "e6 --dry-run"

run="$(export LAB_RESULTS="$d"; cd "$REPO" && bash "$CAMP/e11.sh" --dry-run --sizes 2 --idle 5 2>&1)" || pnote "e11 dry exit $?"
echo "$run" | grep -q "E11-DRY-RUN sizes=2 idle=5" || pnote "no marker"
verdict "e11 --dry-run"

# ---------------------------------------------------------------------- e4
# e4: cluster up with caps, then down on exit (normal path); 8 CSV columns
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  bash "$CAMP/e4.sh" --systems pipedpeer,ray --repeats 2
) >"$d/out" 2>&1 || pnote "e4 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
grep -qxF 'cluster up 4 --cap pc00=8 --cap pc01=4 --cap pc02=2 --cap pc03=1' "$d/cluster.log" 2>/dev/null || pnote "cluster up args wrong: $(tr '\n' '|' <"$d/cluster.log" 2>/dev/null)"
grep -qxF 'cluster down' "$d/cluster.log" 2>/dev/null || pnote "no cluster down after a normal finish"
csv="$d/analysis-input/e4.csv"
if [[ -f "$csv" ]]; then
  bad_cols="$(awk -F, 'NF != 8' "$csv" | wc -l)"
  [[ "$bad_cols" == 0 ]] || pnote "$bad_cols CSV lines without 8 columns"
  [[ "$(col "$csv" 1 5)" == "0.25" && "$(col "$csv" 1 8)" == "0.25" ]] || pnote "pipedpeer shares not 0.25: $(sed -n 2p "$csv")"
  [[ "$(col "$csv" 1 4)" == "1" ]] || pnote "result_ok not 1 for an ok run: $(sed -n 2p "$csv")"
  ray_row="$(grep '^ray,1,' "$csv" || true)"
  [[ "$ray_row" =~ ,,,,$ ]] || pnote "ray row shares not empty: $ray_row"
else
  pnote "no e4.csv"
fi
verdict "e4: cap args, 8 CSV columns filled from receipt by_where, down after the run"

# e4/e6: a record over 128 KB (a real run's timeline) still gives its row
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  FAKE_BIG_TIMELINE=1 bash "$CAMP/e4.sh" --systems pipedpeer --repeats 1
) >"$d/out" 2>&1 || pnote "e4 exit $?"
[[ "$(wc -c <"$d/runs.jsonl")" -gt 131072 ]] || pnote "record not over 128 KB: $(wc -c <"$d/runs.jsonl")"
row="$(sed -n 2p "$d/analysis-input/e4.csv" 2>/dev/null || true)"
[[ "$row" == pipedpeer,1,* && "$row" != *,,* ]] || pnote "e4 row from a big record: [$row] ($(grep -c 'Argument list' "$d/out") arg-list errors)"
d="$(newdir)"
(
  setup_lab "$d" 2
  python3 -c 'import json;print(json.dumps({"run_id":"E6-W1-pipedpeer-n2-r1","wall_s":7.5,"result_ok":True,"timeline":{"steps":["x"*100]*3000}},separators=(",",":")))' >"$d/runs.jsonl"
  # shellcheck source=/dev/null
  source "$CAMP/e6.sh"
  printf 'rec=%s\n' "$(e6_record E6-W1-pipedpeer-n2-r1)"
) >"$d/out" 2>&1 || pnote "e6_record exit $?"
grep -qx 'rec=7.5,1' "$d/out" || pnote "e6_record from a big record: $(tail -n 2 "$d/out" | tr '\n' ' ')"
verdict "e4 and e6 read a record over 128 KB"

# e4: receipt missing -> shares empty, reason logged
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  FAKE_NO_RECEIPT=1 bash "$CAMP/e4.sh" --systems pipedpeer --repeats 1
) >"$d/out" 2>&1 || pnote "e4 exit $?"
row="$(sed -n 2p "$d/analysis-input/e4.csv" 2>/dev/null || true)"
[[ "$row" =~ ^pipedpeer,1,[0-9.]+,[01],,,,$ ]] || pnote "shares not empty: [$row]"
grep -qi "no receipt" "$d/campaign.log" || pnote "no 'no receipt' reason in campaign.log"
verdict "e4: receipt missing -> shares empty, reason logged"

# e4: headcount arm, log line present -> 5 runs with shares, daemon restarted with the env, measured after
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  bash "$CAMP/e4.sh" --systems pipedpeer-headcount
) >"$d/out" 2>&1 || pnote "e4 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
[[ "$(runs_count "$d" pipedpeer-headcount)" == 5 ]] || pnote "headcount runs: $(runs_count "$d" pipedpeer-headcount)"
[[ "$(grep -c '^pipedpeer-headcount,' "$d/analysis-input/e4.csv" 2>/dev/null || true)" == 5 ]] || pnote "5 headcount csv rows missing"
grep -q '^pipedpeer-headcount,1,[0-9.]*,1,0.25,' "$d/analysis-input/e4.csv" 2>/dev/null || pnote "headcount shares not recorded"
[[ "$(cat "$d/pc00/.split" 2>/dev/null || echo MISSING)" == "" ]] || pnote "pc00 daemon not restored to measured (split=$(cat "$d/pc00/.split" 2>/dev/null))"
grep -q 'pc00 systemctl --user set-environment PIPEDPEER_POOL_SPLIT=headcount' "$d/systemctl.log" 2>/dev/null || pnote "service environment not set for headcount"
grep -q 'unset-environment PIPEDPEER_POOL_SPLIT' "$d/systemctl.log" 2>/dev/null || pnote "service environment not restored"
verdict "e4 headcount arm, log line present -> 5 runs with shares; measured restored"

# e4: headcount arm, log line absent -> arm unavailable, the others still run
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  FAKE_NO_HEADCOUNT_LOG=1 bash "$CAMP/e4.sh" --repeats 1
) >"$d/out" 2>&1 || pnote "e4 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
for s in pipedpeer ray dask; do grep -q "^$s,1," "$d/analysis-input/e4.csv" 2>/dev/null || pnote "no $s row"; done
grep -q '^pipedpeer-headcount,' "$d/analysis-input/e4.csv" 2>/dev/null && pnote "unavailable arm has csv rows"
grep -qi "pipedpeer-headcount unavailable" "$d/BLOCKERS.md" 2>/dev/null || pnote "no unavailable note in BLOCKERS.md"
[[ "$(cat "$d/pc00/.split" 2>/dev/null || echo MISSING)" == "" ]] || pnote "pc00 daemon not restored"
verdict "e4 headcount arm, log line absent -> marked unavailable, others still run"

# e4: a PC with too few CPUs -> E4-NEEDS, nothing started
d="$(newdir)"
(
  setup_lab "$d" 4
  sed -i 's/^\(pc00\t[^\t]*\t[^\t]*\t[^\t]*\t\)8/\14/' "$d/hosts.tsv"
  cd "$REPO"
  bash "$CAMP/e4.sh" --repeats 1
) >"$d/out" 2>&1 && pnote "e4 exited 0 with a 4-CPU pc00"
grep -q '^E4-NEEDS' "$d/out" || pnote "no E4-NEEDS: $(tail -2 "$d/out" | tr '\n' ' ')"
[[ ! -f "$d/cluster.log" ]] || ! grep -q '^cluster up' "$d/cluster.log" || pnote "cluster was brought up"
verdict "e4: nproc below a cap -> E4-NEEDS, no cluster up"

# e4: cores that do not match the caps -> exit 1, caps reset (down)
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  FAKE_CORES=32 bash "$CAMP/e4.sh" --repeats 1
) >"$d/out" 2>&1 && pnote "e4 exited 0 with uncapped cores"
grep -qxF 'cluster down' "$d/cluster.log" 2>/dev/null || pnote "no cluster down on the cores-mismatch exit"
grep -q 'caps not followed' "$d/BLOCKERS.md" 2>/dev/null || pnote "no BLOCKERS line"
verdict "e4: advertised cores != caps -> exit 1, down called"

# e4: killed mid-run -> caps reset
d="$(newdir)"
(
  setup_lab "$d" 4
  cd "$REPO"
  export FAKE_RUN_SLEEP=6
  bash "$CAMP/e4.sh" --systems pipedpeer --repeats 1 &
  pid=$!
  for _ in $(seq 1 40); do grep -q 'pipedpeer run' "$d/pipedpeer.log" 2>/dev/null && break; sleep 0.25; done
  kill -TERM "$pid"
  wait "$pid" || true
) >"$d/out" 2>&1
grep -qxF 'cluster down' "$d/cluster.log" 2>/dev/null || pnote "no cluster down after SIGTERM"
verdict "e4 killed mid-run -> caps reset"

# --------------------------------------------------------------------- e11
# join_s from the real status parse; baseline delta removed; lo traffic ignored
d="$(newdir)"
(
  setup_lab "$d" 2
  # idle sleeps: the stub grows the eth0 rx counters; sequence = baseline, then the window
  cat >"$d/verif/idle-sleep" <<'EOF'
#!/usr/bin/env bash
seq_file="$FAKELAB_DIR/idle-seq"
inc="$(head -1 "$seq_file")"; sed -i 1d "$seq_file"
lo="${inc#*,}"; inc="${inc%%,*}"
for h in "$FAKELAB_DIR"/pc*/net_dev; do
  awk -v b="$inc" -v l="$lo" '$1=="eth0:" {$2+=b} $1=="lo:" {$2+=l} {print}' "$h" >"$h.new" && mv "$h.new" "$h"
done
EOF
  chmod +x "$d/verif/idle-sleep"
  printf '1000,0\n4000,99999\n' >"$d/idle-seq"
  export E11_IDLE_SLEEP="$d/verif/idle-sleep"
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 2 --idle 60
) >"$d/out" 2>&1 || pnote "e11 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
csv="$d/analysis-input/e11.csv"
js="$(col "$csv" 1 3)"
if [[ ! "$js" =~ ^[0-9.]+$ ]] || ! awk -v j="$js" 'BEGIN{exit !(j < 20)}'; then pnote "join_s [$js] not a quick real parse"; fi
[[ "$(col "$csv" 1 4)" == "3000" ]] || pnote "pc00 bytes_per_min [$(col "$csv" 1 4)] != 3000"
[[ "$(col "$csv" 2 4)" == "3000" ]] || pnote "pc01 bytes_per_min [$(col "$csv" 2 4)] != 3000"
[[ "$(col "$csv" 1 5)" == "1" && "$(col "$csv" 2 5)" == "0" ]] || pnote "introducer flags wrong"
verdict "e11: join_s parsed, base 1000 end 5000 baseline 1000 idle 60 -> 3000 bytes/min"

d="$(newdir)"
(
  setup_lab "$d" 2
  cp "$CAMP/test/exp_test.sh" /dev/null
  cat >"$d/verif/idle-sleep" <<'EOF'
#!/usr/bin/env bash
for h in "$FAKELAB_DIR"/pc*/net_dev; do
  awk '$1=="lo:" {$2+=99999} {print}' "$h" >"$h.new" && mv "$h.new" "$h"
done
EOF
  chmod +x "$d/verif/idle-sleep"
  export E11_IDLE_SLEEP="$d/verif/idle-sleep"
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 2 --idle 60
) >"$d/out" 2>&1 || pnote "e11 exit $?"
csv="$d/analysis-input/e11.csv"
[[ "$(col "$csv" 1 4)" == "0" && "$(col "$csv" 2 4)" == "0" ]] || pnote "lo traffic counted: $(sed -n 2,3p "$csv" | tr '\n' ' ')"
verdict "e11: loopback traffic only -> counted 0"

# never healthy -> join_s empty and logged, not the limit
d="$(newdir)"
(
  setup_lab "$d" 2
  sed -i '3d' "$d/status.txt"   # only pc00 listed -> 1 healthy of 2
  export E11_IDLE_SLEEP=true E11_JOIN_S=3
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 2 --idle 1
) >"$d/out" 2>&1 || pnote "e11 exit $?"
csv="$d/analysis-input/e11.csv"
[[ "$(col "$csv" 1 3)" == "" ]] || pnote "join_s [$(col "$csv" 1 3)] should be empty"
grep -qi "did not reach" "$d/campaign.log" || pnote "no 'did not reach' in campaign.log"
verdict "e11: status never healthy -> join_s empty, logged"

# all: one W1 run
d="$(newdir)"
(
  setup_lab "$d" 2
  export E11_IDLE_SLEEP=true
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes all --idle 1
) >"$d/out" 2>&1 || pnote "e11 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
[[ "$(runs_count "$d" pipedpeer)" == 1 ]] || pnote "W1 runs at all: $(runs_count "$d" pipedpeer)"
grep -qF '"run_id":"E11-W1-pipedpeer-n2-r1"' "$d/runs.jsonl" 2>/dev/null || pnote "run id wrong"
verdict "e11: size all -> one W1 run in runs.jsonl"

# --------------------------------------------------------------------- e5
d="$(newdir)"
(
  setup_lab "$d" 2
  cd "$REPO"
  bash "$CAMP/e5.sh" --n 2 --updates 3
) >"$d/out" 2>&1 || pnote "e5 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
csv="$d/analysis-input/e5.csv"
if [[ -f "$csv" ]]; then
  [[ "$(awk -F, 'NR>1' "$csv" | wc -l)" == 24 ]] || pnote "rows: $(awk 'END{print NR-1}' "$csv") != 24 (3 arms x 4 phases x 2 PCs)"
  for s in pipedpeer pip ray; do
    for ph in cold update1 update2 update3; do
      [[ "$(grep -c "^$s,$ph," "$csv")" == 2 ]] || pnote "$s/$ph rows != 2"
    done
  done
  a="$(grep '^pipedpeer,cold,pc00,' "$csv" | cut -d, -f4)"; b="$(grep '^pipedpeer,cold,pc01,' "$csv" | cut -d, -f4)"
  awk -v a="${a:-x}" -v b="${b:-x}" 'BEGIN{d=b-a; exit !(a>=1 && a<60 && d>0.99 && d<1.01)}' || pnote "pipedpeer ready_s a=[$a] b=[$b] (want relative, 1.0 s apart)"
  rx="$(grep '^pip,cold,pc00,' "$csv" | cut -d, -f5)"; [[ "$rx" == 5000 ]] || pnote "pip cold rx [$rx] != 5000"
  rx="$(grep '^pip,update1,pc00,' "$csv" | cut -d, -f5)"; [[ "$rx" == 100 ]] || pnote "pip update rx [$rx] != 100"
  [[ "$(grep '^pip,update1,pc00,' "$csv" | cut -d, -f6)" == 1 ]] || pnote "pip update not marked reused"
  [[ "$(grep '^pip,cold,pc00,' "$csv" | cut -d, -f6)" == 0 ]] || pnote "pip cold marked reused"
  r0="$(grep '^pip,cold,pc00,' "$csv" | cut -d, -f4)"
  awk -v a="${r0:-x}" 'BEGIN{exit !(a>=0 && a<60)}' || pnote "pip ready_s [$r0] not relative"
  r0="$(grep '^ray,cold,pc00,' "$csv" | cut -d, -f4)"; r1="$(grep '^ray,cold,pc01,' "$csv" | cut -d, -f4)"
  awk -v a="${r0:-x}" -v b="${r1:-x}" 'BEGIN{d=b-a; exit !(a>0 && a<60 && d>1.9 && d<2.1)}' || pnote "ray ready_s a=[$r0] b=[$r1] (want 2 s apart)"
else
  pnote "no e5.csv"
fi
[[ -f "$d/hosts/pc00/runs/E5-pipedpeer-cold-n2/timeline.json" ]] || pnote "pipedpeer cold timeline not fetched"
[[ "$(grep -c '^pipedpeer,[^,]*,pc0[01],[^,]*,[^,]*,[^,]*,1$' "$csv")" == 8 ]] || pnote "pipedpeer rows not all spread=1: $(grep '^pipedpeer' "$csv" | tr '\n' ' ')"
[[ "$(grep -c '^\(pip\|ray\),.*,$' "$csv")" == 16 ]] || pnote "pip/ray rows should leave spread empty"
! grep -rq 'PIPEDPEER_RECEIPT' "$d/hosts/pc00/runs"/E5-pipedpeer-*/cmd.sh 2>/dev/null || pnote "e5 pipedpeer run still names PIPEDPEER_RECEIPT (daemon drops the other machines' steps)"
verdict "e5: cold + 3 updates per arm, per-PC ready_s relative to the arm start, rx_bytes, reused"

# F2: e5 Pool did not reach a second PC -> spread 0 and logged, never taken as good
d="$(newdir)"
(
  setup_lab "$d" 2
  export FAKE_ONE_PC=1
  cd "$REPO"
  bash "$CAMP/e5.sh" --n 2 --updates 1
) >"$d/out" 2>&1 || pnote "e5 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
csv="$d/analysis-input/e5.csv"
[[ "$(grep -c '^pipedpeer,[^,]*,pc0[01],[^,]*,[^,]*,[^,]*,0$' "$csv" 2>/dev/null)" == 4 ]] || pnote "one-PC run not spread=0: $(grep '^pipedpeer' "$csv" 2>/dev/null | tr '\n' ' ')"
grep -q 'NOT SPREAD' "$d/campaign.log" || pnote "no NOT SPREAD reason logged"
verdict "e5: Pool stayed on one PC -> spread 0 recorded and logged"

# --------------------------------------------------------------------- e6
# kill run: wall_s and result_ok from runs.jsonl; kill_at_s measured from t0
d="$(newdir)"
(
  setup_lab "$d" 2
  for w in 4 4 4; do
    printf '{"run_id":"E1-W1-pipedpeer-n2-r%s","experiment":"E1","workload":"W1","system":"pipedpeer","n":2,"repeat":1,"warmup":false,"wall_s":%s,"result_ok":true}\n' "$w" "$w" >>"$d/runs.jsonl"
  done
  export FAKE_RUN_SLEEP=6
  cd "$REPO"
  bash "$CAMP/e6.sh" --n 2 --only kill --systems pipedpeer --kills 1
) >"$d/out" 2>&1 || pnote "e6 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
csv="$d/analysis-input/e6.csv"
row="$(sed -n 2p "$csv" 2>/dev/null || true)"
IFS=, read -r s k i kat wall okv stop <<<"$row"
[[ "$s,$k,$i" == "pipedpeer,kill,1" ]] || pnote "row head [$row]"
awk -v a="${kat:-x}" 'BEGIN{exit !(a>=1.2 && a<3.5 && a != 1.2)}' || pnote "kill_at_s [$kat] not a measured value near 1.2"
awk -v a="${wall:-x}" 'BEGIN{exit !(a>=5.5 && a<12)}' || pnote "wall_s [$wall] not from the record"
grep -qF '"run_id":"E6-W1-pipedpeer-n2-r1"' "$d/runs.jsonl" 2>/dev/null || pnote "kill run not in runs.jsonl"
grep -q 'pc01 systemctl --user kill -s SIGKILL pipedpeer.service' "$d/systemctl.log" 2>/dev/null || pnote "no SIGKILL on the unit"
[[ "$(grep -c '^cluster up' "$d/cluster.log")" -ge 2 ]] || pnote "agent not restarted after the kill"
verdict "e6 kill run finishes -> wall_s from runs.jsonl, measured kill_at_s, cluster up again"

# a crashing run: result_ok false, not 1
d="$(newdir)"
(
  setup_lab "$d" 2
  printf '{"run_id":"E1-W1-pipedpeer-n2-r1","experiment":"E1","workload":"W1","system":"pipedpeer","n":2,"repeat":1,"warmup":false,"wall_s":1,"result_ok":true}\n' >"$d/runs.jsonl"
  export FAKE_RUN_FAIL=1 FAKE_RUN_SLEEP=3
  cd "$REPO"
  bash "$CAMP/e6.sh" --n 2 --only kill --systems pipedpeer --kills 1
) >"$d/out" 2>&1 || pnote "e6 exit $?"
row="$(sed -n 2p "$d/analysis-input/e6.csv" 2>/dev/null || true)"
IFS=, read -r s k i kat wall okv stop <<<"$row"
[[ "$okv" == 0 ]] || pnote "result_ok [$okv] for a crashed run (want 0): $row"
verdict "e6 run crashes -> result_ok 0"

# Ctrl-C: JOBS reaches 0 on the third poll -> latency measured to that poll
d="$(newdir)"
(
  setup_lab "$d" 2
  export FAKE_RUN_SLEEP=30 FAKE_JOBS_POLLS=3
  cd "$REPO"
  bash "$CAMP/e6.sh" --n 2 --only ctrlc --ctrlc 1 --ctrlc-after 2
) >"$d/out" 2>&1 || pnote "e6 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
row="$(sed -n 2p "$d/analysis-input/e6.csv" 2>/dev/null || true)"
IFS=, read -r s k i kat wall okv stop <<<"$row"
[[ "$s,$k,$i" == "pipedpeer,ctrlc,1" ]] || pnote "row head [$row]"
awk -v a="${stop:-x}" 'BEGIN{exit !(a>=2 && a<9)}' || pnote "stop_latency_s [$stop] not ~2 polls"
[[ "$(cat "$d/pc00/.status_calls" 2>/dev/null || echo 0)" == 3 ]] || pnote "pc00 status polls: $(cat "$d/pc00/.status_calls" 2>/dev/null) != 3"
[[ "$okv" == 0 ]] || pnote "interrupted run result_ok [$okv]"
awk -v a="${wall:-x}" 'BEGIN{exit !(a>0)}' || pnote "wall_s [$wall] not from the record"
verdict "e6 Ctrl-C, JOBS 0 after 3 polls -> stop_latency to that poll, wall/result from the record"

# F1: a run with no record -> wall_s and result_ok empty (never 0), reason logged
d="$(newdir)"
(
  setup_lab "$d" 2
  : >"$d/runs.jsonl"
  export LAB_RESULTS="$d"
  cd "$REPO"
  # shellcheck source=/dev/null
  source "$CAMP/e6.sh"
  rec="$(e6_record E6-W1-pipedpeer-n2-r9 2>"$d/err")" && echo "rc0 [$rec]"
  grep -q 'no record' "$d/err" "$d/campaign.log" && echo logged
  printf '{"run_id":"E6-W1-x-n2-r1","wall_s":5,"result_ok":false}\n' >"$d/runs.jsonl"
  e6_record E6-W1-x-n2-r1
) >"$d/out" 2>&1
grep -qx 'rc0 \[,\]' "$d/out" || pnote "no-record output not empty: $(tr '\n' ' ' <"$d/out")"
grep -q '^logged$' "$d/out" || pnote "no-record reason not logged: $(tr '\n' ' ' <"$d/out")"
grep -qx '5,0' "$d/out" || pnote "recorded failure not 5,0: $(tr '\n' ' ' <"$d/out")"
verdict "e6 no record -> empty wall_s/result_ok + logged; recorded failure -> 0"

# F1: e5_env.py really imports the S13 packages (AST, not a grep of the docstring)
for f in e5_env.py e5_env_tqdm.py; do
  want="numpy pandas sklearn joblib scipy"; [[ $f == e5_env_tqdm.py ]] && want="$want tqdm"
  got="$(python3 -I - "$REPO/docs/benchmarks/workloads/$f" <<'PY'
import ast, sys
t = ast.parse(open(sys.argv[1]).read())
names = set()
for n in ast.walk(t):
    if isinstance(n, ast.Import):
        names |= {a.name.split(".")[0] for a in n.names}
    elif isinstance(n, ast.ImportFrom) and n.module:
        names.add(n.module.split(".")[0])
print(" ".join(sorted(names)))
PY
)"
  for m in $want; do [[ " $got " == *" $m "* ]] || pnote "$f does not import $m (has: $got)"; done
  verdict "$f imports $want"
done

# F2: Ctrl-C arm signals the pipedpeer run process itself, not a wrapper; none -> logged, rc 1
d="$(newdir)"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  cd "$REPO"
  # shellcheck source=/dev/null
  source "$CAMP/e6.sh"
  e6_interrupt_run E6c-none 2>"$d/err" && echo "none-rc0" || echo "none-rc1"
  # a wrapper whose command line mentions "pipedpeer run" but is not the client
  HOME="$d/pc00" bash -c 'sleep 30; : pipedpeer run decoy' &
  decoy=$!
  HOME="$d/pc00" FAKE_RUN_SLEEP=30 pipedpeer run job.py >/dev/null 2>&1 &
  sleep 1
  e6_interrupt_run E6c-live 2>>"$d/err" && echo "live-rc0" || echo "live-rc1"
  sleep 1
  pgrep -f "$d/verif/pipedpeer run" >/dev/null && echo "client-alive" || echo "client-gone"
  kill -0 "$decoy" 2>/dev/null && echo "decoy-alive" || echo "decoy-gone"
  kill "$decoy" 2>/dev/null || true
) >"$d/out" 2>&1
grep -qx 'none-rc1' "$d/out" || pnote "no run: expected rc 1: $(tr '\n' ' ' <"$d/out")"
grep -q 'no pipedpeer run process found' "$d/err" "$d/campaign.log" 2>/dev/null || pnote "no-run reason not logged"
grep -qx 'live-rc0' "$d/out" || pnote "live run: expected rc 0: $(tr '\n' ' ' <"$d/out")"
grep -qx 'client-gone' "$d/out" || pnote "SIGINT did not reach the run client"
grep -qx 'decoy-alive' "$d/out" || pnote "decoy wrapper was signalled"
verdict "e6 Ctrl-C: signal reaches the run process; no run -> logged rc 1"

# F2: e11 result_ok: null in the record -> empty + reason, never the text null; true/false as recorded
d="$(newdir)"
(
  setup_lab "$d" 2
  export LAB_RESULTS="$d"
  cd "$REPO"
  printf '%s\n' '{"run_id":"E11-W1-pipedpeer-n2-r1","result_ok":null}' \
    '{"run_id":"E11-W1-pipedpeer-n3-r1","result_ok":true}' \
    '{"run_id":"E11-W1-pipedpeer-n4-r1","result_ok":false}' >"$d/runs.jsonl"
  # shellcheck source=/dev/null
  source "$CAMP/e11.sh"
  for k in 2 3 4 9; do echo "k$k=[$(e11_result_ok "E11-W1-pipedpeer-n$k-r1" 2>>"$d/err")]"; done
) >"$d/out" 2>&1
grep -qx 'k2=\[\]' "$d/out" || pnote "null not empty: $(tr '\n' ' ' <"$d/out")"
grep -qx 'k3=\[true\]' "$d/out" || pnote "true not true"
grep -qx 'k4=\[false\]' "$d/out" || pnote "false not false"
grep -qx 'k9=\[\]' "$d/out" || pnote "no record not empty"
grep -q 'null' "$d/out" && pnote "text null leaked"
grep -q 'has no result_ok' "$d/err" "$d/campaign.log" 2>/dev/null || pnote "reason for the empty value not logged"
verdict "e11 result_ok: null/no record -> empty + reason; true/false as recorded"

# F2: LAB_MEM_SPARE fractional keeps the gate; junk is a clear error; whole numbers as before
d="$(newdir)"
(
  # shellcheck source=/dev/null
  source "$REPO/scripts/lib/lab.sh"
  gib=1073741824
  pp_meminfo() { case "$1" in MemTotal) echo $((100 * gib)) ;; MemAvailable) echo "$gib" ;; esac; }
  pp_running_mem() { echo 0; }
  export LAB_MEM_WAIT=0
  want=$((gib * 8 / 10))
  LAB_MEM_SPARE=0.5 pp_lab_mem_check "$want" t 2>&1 && echo "half-pass" || echo "half-refused"
  LAB_MEM_SPARE=0.1 pp_lab_mem_check "$want" t 2>&1 && echo "tenth-pass" || echo "tenth-refused"
  LAB_MEM_SPARE=1 pp_lab_mem_check "$want" t 2>&1 && echo "one-pass" || echo "one-refused"
  LAB_MEM_SPARE=lots pp_lab_mem_check "$want" t 2>&1 && echo "junk-pass" || echo "junk-refused"
) >"$d/out" 2>&1
grep -qx 'half-refused' "$d/out" || pnote "LAB_MEM_SPARE=0.5 gate skipped: $(tr '\n' ' ' <"$d/out")"
grep -qx 'tenth-pass' "$d/out" || pnote "0.1 spare should pass"
grep -qx 'one-refused' "$d/out" || pnote "spare 1 should refuse (1 GiB avail, 0.8 wanted)"
grep -qx 'junk-refused' "$d/out" || pnote "junk spare not refused"
grep -q 'LAB_MEM_SPARE must be a number' "$d/out" || pnote "no clear error for junk"
grep -q 'arithmetic\|syntax' "$d/out" && pnote "bash arithmetic error printed"
verdict "LAB_MEM_SPARE 0.5 gate applied; 0.1 passes; 1 as before; junk -> clear error"

total=$((pass + failed))
echo "PASS $pass/$total"
[[ $failed -eq 0 ]]
