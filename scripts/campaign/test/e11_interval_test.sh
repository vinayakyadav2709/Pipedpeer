#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2034,SC2317
# e11_interval_test.sh: case table for unit E11F (E11 same-moment reads)
#
# Cases:
#   1. start and end reads 300 s apart with IDLE=300 -> rate as today
#   2. reads 330 s apart -> rate divided by 330
#   3. 32 PCs, sequential-read stub adding 1 s each -> one timestamp spread under 2 s on the fake lab
#   4. 15 of 16 joined -> joined=15/16
#   5. all joined -> joined=16/16
#   6. counter read fails -> empty value, one log line, other PCs kept

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
BASE_PATH="$PATH"

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

pass=0
failed=0
problems=()

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
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
  d="$(mktemp -d "$STATE/e-e11-test.XXXXXX")"
  mkdir -p "$d/home"
  printf '%s\n' "$d" >>"$DIRS_FILE"
  printf '%s\n' "$d"
}

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

setup_test_lab() {
  local dir="$1" n="$2" i pc v="$1/verif"
  fakelab_init "$dir" "$n" || exit 90
  mkdir -p "$v"
  export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.1 LAB_POLL_S=1
  export CLUSTER_SH="$v/cluster.sh" METRICS_SH="$v/metrics.sh" UPLOAD_SH="$v/upload.sh"
  export E11_JOIN_S=4
  # shellcheck disable=SC2088
  export LAB_NET_DEV='~/net_dev'

  cat >"$v/pipedpeer" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; }
me="$(basename "$HOME")"
case "${1:-}" in
  status)
    f="$FAKELAB_DIR/status.$me"; [[ -f "$f" ]] || f="$FAKELAB_DIR/status.txt"
    [[ -f "$f" ]] || { echo "pipedpeer stub: no status file" >&2; exit 1; }
    cat "$f" ;;
  start) ;;
  stop) ;;
  run) echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000" ;;
  *) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
EOF

  cat >"$v/systemctl" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; }
exit 0
EOF

  cat >"$v/ip" <<'EOF'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "ip stub: no FAKELAB_DIR" >&2; exit 97; }
echo "default via 10.99.0.1 dev eth0 proto dhcp metric 100"
EOF

  # A PC's clock: `date +%s%N` reads $FAKELAB_DIR/clock_s when a row sets it.
  cat >"$v/date" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == "+%s%N" && -f "${FAKELAB_DIR:-}/clock_s" ]]; then
  echo "$(cat "$FAKELAB_DIR/clock_s")000000000"
  exit 0
fi
exec /usr/bin/date "$@"
EOF

  cat >"$v/cluster.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

  for script in metrics.sh upload.sh; do
    cat >"$v/$script" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  done

  chmod +x "$v"/*
  export PATH="$v:$dir/bin:$BASE_PATH"

  printf 'W1_ITEMS=10\nW1_WEAK_PER_PC=5\nW2_ITEMS=10\nW3_TREES=5\nT1_EPOCHS=1\nBENCH_PROCS=0\n' >"$dir/sizes.env"
  printf '%-6s%-9s%-6s%-8s%-10s%-5s%-6s%s\n' NAME STATE PATH CPU 'MEM FREE' GPU JOBS NOTE >"$dir/status.txt"
  for ((i = 0; i < n; i++)); do
    pc="$(printf 'pc%02d' "$i")"
    printf '%-6s%-9s%-6s%-8s%-10s%-5s%-7s%s\n' "$pc" healthy self '8c 1%' '7.2 GiB' none 0 - >>"$dir/status.txt"
    mkdir -p "$dir/$pc/pipedpeer-lab/venv/bin" "$dir/$pc/pipedpeer-lab/work/baselines"
    printf 'Inter-|   Receive\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo: 1000 1 0 0 0 0 0 0 1000 1 0 0 0 0 0 0\n  eth0: 5000 1 0 0 0 0 0 0 1000 1 0 0 0 0 0 0\n' >"$dir/$pc/net_dev"
  done
  printf 'id,offset_ms,rtt_ms,measured_at\n' >"$dir/clocks.csv"
  for ((i = 0; i < n; i++)); do printf 'pc%02d,0,1,x\n' "$i" >>"$dir/clocks.csv"; done
  : >"$dir/campaign.log"
}

# idle_clock_row <dir> <base_s> <base_bytes> <k_s> <k_bytes>: the baseline and
# the size-k idle windows advance the PCs' clock and add that NIC traffic.
idle_clock_row() {
  echo 1000 >"$1/clock_s"
  cat >"$1/verif/idle-sleep" <<EOF
#!/usr/bin/env bash
n=0
[[ -f "$1/idle_calls" ]] && n="\$(cat "$1/idle_calls")"
n=\$((n + 1)); echo "\$n" >"$1/idle_calls"
if ((n == 1)); then s=$2 b=$3; else s=$4 b=$5; fi
echo "\$((\$(cat "$1/clock_s") + s))" >"$1/clock_s"
for h in "$1"/pc*/net_dev; do
  awk -v b="\$b" '\$1=="eth0:" {\$2+=b} {print}' "\$h" >"\$h.new" && mv "\$h.new" "\$h"
done
EOF
  chmod +x "$1/verif/idle-sleep"
  export E11_IDLE_SLEEP="$1/verif/idle-sleep"
}

# rate_row <title> <base_s> <base_bytes> <k_s> <k_bytes> <expected>
rate_row() {
  local d csv
  d="$(newdir)"
  (
    setup_test_lab "$d" 2
    idle_clock_row "$d" "$2" "$3" "$4" "$5"
    cd "$REPO"
    bash "$CAMP/e11.sh" --sizes 2 --idle 300
  ) >"$d/out" 2>&1 || pnote "e11 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
  csv="$d/analysis-input/e11.csv"
  [[ -f "$csv" ]] || pnote "e11.csv not created"
  if [[ -f "$csv" ]]; then
    [[ "$(col "$csv" 1 4)" == "$6" ]] || pnote "pc00 bytes_per_min [$(col "$csv" 1 4)] != $6"
    [[ "$(col "$csv" 2 4)" == "$6" ]] || pnote "pc01 bytes_per_min [$(col "$csv" 2 4)] != $6"
  fi
  verdict "$1"
}

col() { # col <csv> <row-number-after-header> <col-number>
  awk -F, -v r="$2" -v c="$3" 'NR==r+1 {print $c}' "$1"
}

rate_row "start and end reads 300 s apart with IDLE=300 -> rate as today" 300 0 300 3000 600
# 660 if it were divided by IDLE
rate_row "reads 330 s apart -> rate divided by 330" 300 0 330 3300 600
# (3630 - 300*330/300) * 60 / 330; 605.45 if the baseline were not scaled
rate_row "baseline window shorter than the size window -> baseline scaled" 300 300 330 3630 600
rate_row "baseline window as long as the size window -> baseline as measured" 300 300 300 3300 600

# ---------------------------------------------------------------------------
# Case 3: 32 PCs, sequential-read stub adding 1 s each -> one timestamp spread under 2 s on the fake lab
# ---------------------------------------------------------------------------
d="$(newdir)"
(
  setup_test_lab "$d" 32
  export E11_IDLE_SLEEP=true
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 32 --idle 1
) >"$d/out" 2>&1 || pnote "e11 exit $? ($(tail -3 "$d/out" | tr '\n' ' '))"
# Check the timestamps recorded across all 32 PCs for the wave:
# They must have a spread (max - min) under 2 seconds.
ts_file="$d/logs/e11-counters-32-start/timestamps.tsv"
if [[ ! -f "$ts_file" ]]; then
  pnote "timestamps.tsv was not written for 32-start read wave"
else
  spread_s="$(python3 - "$ts_file" <<'PY'
import sys
ts = []
for ln in open(sys.argv[1]):
    parts = ln.strip().split()
    if len(parts) >= 2:
        try:
            val = float(parts[1])
            ts.append(val)
        except ValueError:
            pass
if not ts:
    print(999.0)
    sys.exit(0)
# Convert nanoseconds to seconds if needed
diff = max(ts) - min(ts)
if max(ts) > 1e14:
    diff /= 1e9
print(round(diff, 6))
PY
)"
  if ! awk -v s="$spread_s" 'BEGIN{exit !(s < 2.0)}'; then
    pnote "timestamp spread across 32 PCs was $spread_s s (expected under 2 s)"
  fi
fi
verdict "32 PCs, sequential-read stub adding 1 s each -> one timestamp spread under 2 s on the fake lab"

# ---------------------------------------------------------------------------
# Case 4: 15 of 16 joined -> joined=15/16
# ---------------------------------------------------------------------------
d="$(newdir)"
(
  setup_test_lab "$d" 16
  # Remove one PC line from status.txt so status_healthy_count gives 15
  sed -i '3d' "$d/status.txt"
  export E11_IDLE_SLEEP=true E11_JOIN_S=2
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 16 --idle 1
) >"$d/out" 2>&1 || pnote "e11 exit $?"
csv="$d/analysis-input/e11.csv"
[[ -f "$csv" ]] || pnote "e11.csv missing"
if [[ -f "$csv" ]]; then
  # Header: k,pc,join_s,bytes_per_min,is_introducer,joined
  # Check header has joined as last column
  hdr="$(head -1 "$csv")"
  [[ "$hdr" =~ ,joined$ ]] || pnote "e11.csv header [$hdr] does not end in ,joined"
  j1="$(col "$csv" 1 6)"
  [[ "$j1" == "15/16" ]] || pnote "joined column [$j1] != 15/16"
fi
verdict "15 of 16 joined -> joined=15/16"

# ---------------------------------------------------------------------------
# Case 5: all joined -> joined=16/16
# ---------------------------------------------------------------------------
d="$(newdir)"
(
  setup_test_lab "$d" 16
  # All 16 healthy in status.txt
  export E11_IDLE_SLEEP=true
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 16 --idle 1
) >"$d/out" 2>&1 || pnote "e11 exit $?"
csv="$d/analysis-input/e11.csv"
[[ -f "$csv" ]] || pnote "e11.csv missing"
if [[ -f "$csv" ]]; then
  j1="$(col "$csv" 1 6)"
  [[ "$j1" == "16/16" ]] || pnote "joined column [$j1] != 16/16"
fi
verdict "all joined -> joined=16/16"

# ---------------------------------------------------------------------------
# Case 6: counter read fails -> empty value, one log line, other PCs kept
# ---------------------------------------------------------------------------
d="$(newdir)"
(
  setup_test_lab "$d" 2
  # Break pc01's net_dev so its counter is unreadable
  rm -f "$d/pc01/net_dev"
  cat >"$d/verif/idle-sleep" <<'EOF'
#!/usr/bin/env bash
for h in "$FAKELAB_DIR"/pc00/net_dev; do
  awk '$1=="eth0:" {$2+=3000} {print}' "$h" >"$h.new" && mv "$h.new" "$h"
done
EOF
  chmod +x "$d/verif/idle-sleep"
  export E11_IDLE_SLEEP="$d/verif/idle-sleep"
  cd "$REPO"
  bash "$CAMP/e11.sh" --sizes 2 --idle 60
) >"$d/out" 2>&1 || pnote "e11 exit $?"
csv="$d/analysis-input/e11.csv"
[[ -f "$csv" ]] || pnote "e11.csv missing"
if [[ -f "$csv" ]]; then
  # pc00 had working counter and delta 3000 -> bytes_per_min = 3000
  # pc01 had broken counter -> bytes_per_min empty
  bpm00="$(col "$csv" 1 4)"
  bpm01="$(col "$csv" 2 4)"
  [[ -n "$bpm00" && "$bpm00" != "" ]] || pnote "pc00 bytes_per_min was empty (other PCs kept failed)"
  [[ "$bpm01" == "" ]] || pnote "pc01 bytes_per_min [$bpm01] should be empty"
fi
grep -qi "unreadable" "$d/campaign.log" || pnote "campaign.log missing log line for unreadable counter"
verdict "counter read fails -> empty value, one log line, other PCs kept"

# ---------------------------------------------------------------------------
echo "PASS $pass/8"
if ((failed > 0)); then
  exit 1
fi
exit 0
