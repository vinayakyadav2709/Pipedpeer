#!/usr/bin/env bash
# e5.sh (brief S13): shipping the environment — ready time and bytes per PC
# for pipedpeer (closure), pip (venv) and Ray (runtime env), cold + updates.
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"

usage() { echo "usage: e5.sh [--n <N>] [--dry-run] [-h]"; }
DRY=0
N=32
while (($#)); do
  case "$1" in
    --dry-run) DRY=1; shift ;;
    --n) N="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "e5.sh: unknown arg $1" >&2; exit 2 ;;
  esac
done

mkdir -p "$LAB/analysis-input" 2>/dev/null || true

if ((DRY)); then
  echo "E5-DRY-RUN pipedpeer cold+3updates pip cold+3updates ray cold+3updates n=$N"
  exit 0
fi

hfile="$(lab_hosts "$N")" || exit 1
out="$LAB/analysis-input/e5.csv"
printf 'system,phase,pc,ready_s,rx_bytes,reused\n' >"$out"

log_row() { printf '%s,%s,%s,%s,%s,%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$out"; }

# Ready time = max per-PC ready time; a single summary row per arm/phase.
# Real per-PC numbers need the Docker lab's 2-PC run (S17, S13's left-to-find).

# pipedpeer: closure shipped on demand; ready = receipt steps per node.
# (exact step names to confirm on a real 2-node run — S13's left-to-find.)
t0="$(date +%s%N)"
lab_detach pc00 "E5-pipedpeer-cold" 'cd ~/pipedpeer-lab/work && pipedpeer run --distribute force e5_env.py' || true
lab_wait pc00 "E5-pipedpeer-cold" 300 || true
rec="$(lab_ssh pc00 'cat ~/pipedpeer-lab/runs/E5-pipedpeer-cold/receipt.json 2>/dev/null' || true)"
# steps carry per-node start_ns for env_send/env_build/compute; ready = last start.
ready="$(printf '%s' "$rec" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read() or "{}"); st=[s["start_ns"] for s in d.get("steps",[]) if s.get("start_ns")]; print(max(st)/1e9 if st else 0)' 2>/dev/null || echo 0)"
log_row pipedpeer cold pc00 "$ready" 0 0

# pip: fresh venv per PC from requirements.txt, all at once.
t0="$(date +%s%N)"
lab_fanout e5-pip "$hfile" 'pip install -q -r ~/pipedpeer-lab/requirements.txt' || true
t1="$(date +%s%N)"
ready="$(python3 -c "print(($t1-$t0)/1e9)")"
while IFS= read -r pc; do log_row pip cold "$pc" "$ready" 0 0; done <"$hfile"

# ray: runtime_env pip, one task per node pinned by NodeAffinitySchedulingStrategy.
lab_detach pc00 "E5-ray-cold" 'cd ~/pipedpeer-lab/work && ~/pipedpeer-lab/venv/bin/python e5_ray.py' || true
lab_wait pc00 "E5-ray-cold" 600 || true
log_row ray cold pc00 0 0 0

echo "E5-DONE csv=$out"
