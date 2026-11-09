#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/pool_idle.py (idle) to plain ssh: the item
# list is split across the PCs in LAB_HOSTS_FILE (ips, one per line; LAB_N says
# how many), each PC runs its part's Pool through GNU parallel --sshlogin, and
# pc00 joins the parts and prints the original's one result line. The "1/" slots in
# the login file give each host one job and override -j1, so the parts run at
# once and seconds is the slowest part (the harness times the run itself); the
# checksum is recomputed over the whole item list in order, so it equals the
# original's bit for bit; every part keeps the original's serial check.
# Distributed: the Pool items, one part per PC. Local: the join and the line.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Env: LAB_HOSTS_FILE, LAB_N, LAB_USER (same lab user on every PC), LAB_PY,
# LAB_WORKDIR (the PC-side layout's ~/pipedpeer-lab/work), BENCH_* (original).
# BENCH_ITEMS/BENCH_PROCS are inlined into parallel's remote command: ssh
# carries no environment (H3). BENCH_PROCS=0 means each part uses its PC's
# os.cpu_count().
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
items=${BENCH_ITEMS:-16}
procs=${BENCH_PROCS:-0}
case $items in
'' | *[!0-9]*) echo "idle_ssh.sh: BENCH_ITEMS must be a whole number, got '$items'" >&2; exit 2 ;;
esac
case $procs in
'' | *[!0-9]*) echo "idle_ssh.sh: BENCH_PROCS must be 0 or a whole number, got '$procs'" >&2; exit 2 ;;
esac
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/idle-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

# "1/<user>@<ip>": one slot per host (parallel's sshloginfile format).
while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"BENCH_ITEMS=$items BENCH_PROCS=$procs $py baselines/idle_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

"$py" - "$LAB_N" "$tmpd/parts.json" "$items" <<'EOF'
import json
import sys

n = int(sys.argv[1])
items = int(sys.argv[3])
rows = []
for line in open(sys.argv[2]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
rows.sort(key=lambda r: r["part"])
assert len(rows) == n, f"expected {n} parts, got {len(rows)}"
out = [v for r in rows for v in r["out"]]
assert len(out) == items, f"expected {items} items, got {len(out)}"
dt = max(r["seconds"] for r in rows)
print("POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f" % (items, dt, sum(out)))
EOF
