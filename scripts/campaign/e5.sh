#!/usr/bin/env bash
# e5.sh (brief S13): shipping the environment. For each arm (pipedpeer, pip,
# ray) one cold run and then <updates> runs after a small change to the
# environment (tqdm added), on N PCs. Writes analysis-input/e5.csv:
#   system,phase,pc,ready_s,rx_bytes,reused
#   phase  cold, update1..updateU
#   ready_s  seconds from the start of the arm's phase until that PC could
#            compute (own clock corrected by clocks.csv); empty if unknown
#   rx_bytes received on the default-route NIC of that PC during the phase
#   reused 1 when rx_bytes is under 10% of the same PC's cold rx_bytes
#
#   e5.sh [--n N] [--updates U] [--dry-run] [-h]
#
# rx_bytes comes from /proc/net/dev counters read before and after each
# phase (same source sar uses, no time-bucket alignment).
# pipedpeer: ready = first `compute` step per node in the job's timeline.
# pip: ready = the PC's own clock after its venv install finished.
# ray: ready = the task start each node reports (docs/benchmarks/workloads/e5_ray.py).
# Why no `-e PIPEDPEER_RECEIPT=...` on the run: a receipt path the user names
# replaces the daemon's own, so the run record loses the other machines' steps
# (no `compute` on pc01, "ran on pc00 100%") and `pipedpeer jobs last --receipt`
# says "No receipt". The record is read after the run with `jobs last --export
# json`. The spread column says whether `compute` ran on 2 or more PCs; a run
# that stayed on one PC is written spread=0 and logged, never taken as good.
# TODO(real lab): cold-cache start (it does start cold: env_build runs each time).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/e11.sh"

e5_usage() { echo "usage: e5.sh [--n <N>] [--updates <U>] [--dry-run] [-h]"; }

# e5_py <mode>: per-PC ready seconds, "<pc> <seconds>" lines, from the fetched
# run dir (E5_DIR). Needs E5_HOSTS, E5_CLOCKS, E5_IDS.
e5_py() {
  python3 - "$1" <<'PY'
import json, os, sys

mode = sys.argv[1]
d = os.environ.get("E5_DIR", "")
ids = os.environ["E5_IDS"].split()


def warn(msg):
    sys.stderr.write("e5: " + msg + "\n")


off = {}
try:
    for l in open(os.environ["E5_CLOCKS"]):
        f = l.strip().split(",")
        if len(f) > 1 and f[0] != "id":
            try:
                off[f[0]] = float(f[1]) * 1e6
            except ValueError:
                warn("clocks.csv: bad offset for %s: %r" % (f[0], f[1]))
except OSError as e:
    warn("no clocks.csv (%s); PC clocks taken as equal to the driver's" % e)
for pc in ids:
    if pc not in off:
        warn("no clock offset for %s; taken as 0" % pc)
        off[pc] = 0.0

ip2pc, host2pc = {}, {}
for l in open(os.environ["E5_HOSTS"]):
    f = l.rstrip("\n").split("\t")
    if len(f) > 3 and not l.startswith("#"):
        ip2pc[f[1]] = f[0]
        host2pc[f[3]] = f[0]

def arm_start():
    try:
        t0 = int(open(os.path.join(d, "t0")).read().strip())
    except (OSError, ValueError) as e:
        warn("no t0 in %s (%s)" % (d, e))
        sys.exit(3)
    return t0 - off[ids[0]]


ready = {}
if mode == "pipedpeer":
    start = arm_start()
    try:
        tl = json.load(open(os.path.join(d, "timeline.json")))
    except (OSError, ValueError) as e:
        warn("no usable timeline.json (%s)" % e)
        sys.exit(3)
    names = tl.get("names") or {}

    def pcof(m):
        n = names.get(m, m)
        return host2pc.get(n) or (n if n in ids else None)

    for s in tl.get("steps") or []:
        if s.get("step") != "compute":
            continue
        node, mach = pcof(s.get("node")), pcof(s.get("machine"))
        if node is None or mach is None:
            warn("compute step on unknown node/machine %r/%r" % (s.get("node"), s.get("machine")))
            continue
        t = s["start_ns"] - off.get(mach, 0.0)
        if node not in ready or t < ready[node]:
            ready[node] = t
elif mode == "pip":
    start = int(os.environ["E5_START"])
    for l in os.environ["E5_PIP"].splitlines():
        pc, ns = l.split()
        ready[pc] = int(ns) - off.get(pc, 0.0)
elif mode == "ray":
    start = arm_start()
    for l in open(os.path.join(d, "out")):
        if not l.startswith("E5-RAY "):
            continue
        kv = dict(p.split("=", 1) for p in l.split()[1:] if "=" in p)
        pc = ip2pc.get(kv.get("node", ""))
        if pc is None or "start_ns" not in kv:
            warn("bad ray line: " + l.strip())
            continue
        ready[pc] = int(kv["start_ns"]) - off.get(pc, 0.0)
else:
    warn("unknown mode " + mode)
    sys.exit(2)

for pc in ids:
    if pc in ready:
        print(pc, round((ready[pc] - start) / 1e9, 3))
    else:
        warn("no ready time for %s" % pc)
PY
}

# e5_spread_of <id> <ready lines> <var>: 1 when 2 or more PCs have a compute
# step, else 0 (logged: the Pool stayed on one PC, the run is not a result).
e5_spread_of() {
  local k
  k="$(printf '%s\n' "$2" | awk 'NF {c++} END {print c+0}')"
  if ((k >= 2)); then
    printf -v "$3" '%s' 1
  else
    printf -v "$3" '%s' 0
    lab_log "e5 $1: NOT SPREAD: compute ran on $k PC(s), the Pool map did not reach a second PC; spread=0"
  fi
}

e5_main() {
  local DRY=0 N=32 UPDATES=3
  while (($#)); do
    case "$1" in
      --dry-run) DRY=1; shift ;;
      --n) N="${2:?--n needs a number}"; shift 2 ;;
      --updates) UPDATES="${2:?--updates needs a number}"; shift 2 ;;
      -h | --help) e5_usage; return 0 ;;
      *) e5_usage >&2; echo "e5.sh: unknown arg $1" >&2; return 2 ;;
    esac
  done

  mkdir -p "$LAB/analysis-input"
  if ((DRY)); then
    echo "E5-DRY-RUN pipedpeer cold+${UPDATES}updates pip cold+${UPDATES}updates ray cold+${UPDATES}updates n=$N"
    return 0
  fi

  local hfile out phase arm pc id dir cmd ready_txt t_start line p pip_txt
  local -a ids phases
  local -A ready rx spread
  local spread_v
  hfile="$(lab_hosts "$N")" || return 1
  mapfile -t ids <"$hfile"
  phases=(cold)
  for ((p = 1; p <= UPDATES; p++)); do phases+=("update$p"); done
  out="$LAB/analysis-input/e5.csv"
  export E5_HOSTS="$LAB/hosts.tsv" E5_CLOCKS="$LAB/clocks.csv" E5_IDS="${ids[*]}"

  "$CLUSTER_SH" up "$N" || { lab_log "e5: cluster up failed"; return 1; }
  # shellcheck disable=SC2064
  trap "\"$CLUSTER_SH\" down || true" EXIT

  # e5_rx_before / e5_rx_after fill rx[arm,phase,pc]; a counter that cannot
  # be read leaves it empty (read_counters logs why).
  local -A rx_before
  e5_rx_before() { for pc in "${ids[@]}"; do rx_before[$pc]="$(read_counters "$pc" rx || true)"; done; }
  e5_rx_after() { # <arm> <phase>
    local after
    for pc in "${ids[@]}"; do
      after="$(read_counters "$pc" rx || true)"
      if [[ -n "${rx_before[$pc]}" && -n "$after" ]]; then
        rx[$1,$2,$pc]=$((after - rx_before[$pc]))
      else
        rx[$1,$2,$pc]=""
      fi
    done
  }

  for arm in pipedpeer pip ray; do
    for phase in "${phases[@]}"; do
      id="E5-$arm-$phase-n$N"
      dir="$LAB/hosts/pc00/runs/$id"
      local script=e5_env.py
      [[ "$phase" != cold ]] && script=e5_env_tqdm.py
      e5_rx_before
      case "$arm" in
        pipedpeer)
          cmd="cd ~/pipedpeer-lab/work && pipedpeer run --distribute force $script && pipedpeer jobs last --export json >~/pipedpeer-lab/runs/$id/timeline.json && pipedpeer jobs last --receipt >~/pipedpeer-lab/runs/$id/receipt.txt"
          lab_detach pc00 "$id" "$cmd" || { lab_log "e5 $id: detach failed"; continue; }
          lab_wait pc00 "$id" 600 >/dev/null || lab_log "e5 $id: did not finish in time"
          lab_fetch pc00 "$id" "$dir" || lab_log "e5 $id: fetch failed"
          e5_rx_after "$arm" "$phase"
          if ! ready_txt="$(E5_DIR="$dir" e5_py pipedpeer)"; then
            lab_log "e5 $id: no ready times (timeline unusable)"; ready_txt=""
          else
            e5_spread_of "$id" "$ready_txt" spread_v
            spread[$arm,$phase]="$spread_v"
          fi
          ;;
        ray)
          local with=""
          [[ "$phase" != cold ]] && with=" --with tqdm"
          cmd="cd ~/pipedpeer-lab/work && RAY_ADDRESS=$(lab_ip pc00):6379 ~/pipedpeer-lab/venv/bin/python e5_ray.py --nodes $N$with"
          lab_detach pc00 "$id" "$cmd" || { lab_log "e5 $id: detach failed"; continue; }
          lab_wait pc00 "$id" 900 >/dev/null || lab_log "e5 $id: did not finish in time"
          lab_fetch pc00 "$id" "$dir" || lab_log "e5 $id: fetch failed"
          e5_rx_after "$arm" "$phase"
          if ! ready_txt="$(E5_DIR="$dir" e5_py ray)"; then
            lab_log "e5 $id: no ready times (ray output unusable)"; ready_txt=""
          fi
          ;;
        pip)
          if [[ "$phase" == cold ]]; then
            cmd='rm -rf ~/pipedpeer-lab/e5-venv && ~/pipedpeer-lab/venv/bin/python -m venv ~/pipedpeer-lab/e5-venv && ~/pipedpeer-lab/e5-venv/bin/pip install -q -r ~/pipedpeer-lab/requirements.txt && date +%s%N'
          else
            # shellcheck disable=SC2088  # the tilde is for the remote shell
            cmd='~/pipedpeer-lab/e5-venv/bin/pip install -q tqdm && date +%s%N'
          fi
          t_start="$(date +%s%N)"
          lab_fanout "e5-pip-$phase" "$hfile" "$cmd" >/dev/null ||
            lab_log "e5 $id: pip install failed on every PC"
          e5_rx_after "$arm" "$phase"
          pip_txt=""
          for pc in "${ids[@]}"; do
            line="$(tail -1 "$LAB/logs/e5-pip-$phase/$pc" 2>/dev/null | tr -d '[:space:]' || true)"
            if [[ "$line" =~ ^[0-9]{15,}$ ]]; then
              pip_txt+="$pc $line"$'\n'
            else
              lab_log "e5 $id: $pc printed no finish time (install failed?); ready_s left empty"
            fi
          done
          if ! ready_txt="$(E5_PIP="$pip_txt" E5_START="$t_start" e5_py pip)"; then
            lab_log "e5 $id: no ready times"; ready_txt=""
          fi
          ;;
      esac
      while read -r pc line; do
        [[ -n "$pc" ]] && ready[$arm,$phase,$pc]="$line"
      done <<<"$ready_txt"
    done
  done

  printf 'system,phase,pc,ready_s,rx_bytes,reused,spread\n' >"$out"
  local rv rxv cold reused
  for arm in pipedpeer pip ray; do
    for phase in "${phases[@]}"; do
      for pc in "${ids[@]}"; do
        rv="${ready[$arm,$phase,$pc]:-}"
        rxv="${rx[$arm,$phase,$pc]:-}"
        cold="${rx[$arm,cold,$pc]:-}"
        reused=""
        if [[ "$phase" == cold ]]; then
          reused=0
        elif [[ -n "$rxv" && -n "$cold" ]] && ((cold > 0)); then
          reused=$((rxv * 10 < cold ? 1 : 0))
        else
          lab_log "e5 $arm/$phase/$pc: no cold or phase rx_bytes; reused left empty"
        fi
        printf '%s,%s,%s,%s,%s,%s,%s\n' "$arm" "$phase" "$pc" "$rv" "$rxv" "$reused" "${spread[$arm,$phase]:-}" >>"$out"
      done
    done
  done
  echo "E5-DONE csv=$out"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  e5_main "$@"
fi
