#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/pool_map.py (W1) to plain ssh: the item
# list is split across the PCs in LAB_HOSTS_FILE (ips, one per line; LAB_N says
# how many), each PC runs its part's Pool through GNU parallel --sshlogin, and
# pc00 joins the parts and prints the original's one result line. The "1/" slots in
# the login file give each host one job and override -j1, so the parts run at
# once and seconds is the slowest part (the harness times the run itself); the
# checksum is recomputed over the whole item list in item order, so it equals
# the original's bit for bit.
# Distributed: the Pool items, one part per PC. Local: the join and the line.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Env: LAB_HOSTS_FILE, LAB_N, LAB_USER (same lab user on every PC), LAB_PY,
# LAB_WORKDIR (the PC-side layout's ~/pipedpeer-lab/work), BENCH_* (original).
# BENCH_ITEMS/BENCH_PROCS/BENCH_VERIFY are inlined into parallel's remote
# command: ssh carries no environment, so a part would otherwise use its own
# defaults (H3). BENCH_PROCS=0 means each part uses its PC's os.cpu_count().
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
items=${BENCH_ITEMS:-48}
procs=${BENCH_PROCS:-0}
verify=${BENCH_VERIFY:-0}
case $items in
'' | *[!0-9]*) echo "w1_ssh.sh: BENCH_ITEMS must be a whole number, got '$items'" >&2; exit 2 ;;
esac
case $procs in
'' | *[!0-9]*) echo "w1_ssh.sh: BENCH_PROCS must be 0 or a whole number, got '$procs'" >&2; exit 2 ;;
esac
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/w1-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

# "1/<user>@<ip>": one slot per host (parallel's sshloginfile format).
while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"BENCH_ITEMS=$items BENCH_PROCS=$procs BENCH_VERIFY=$verify $py baselines/w1_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

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
if len(rows) != n:
    sys.stderr.write(f"w1_ssh: expected {n} parts, got {len(rows)}\n")
    raise SystemExit(2)
if [r["part"] for r in rows] != list(range(n)):
    sys.stderr.write(f"w1_ssh: parts are {[r['part'] for r in rows]}, expected 0..{n - 1}\n")
    raise SystemExit(2)
expected = [items // n + (1 if p < items % n else 0) for p in range(n)]
for r in rows:
    if len(r["out"]) != expected[r["part"]]:
        sys.stderr.write(
            f"w1_ssh: part {r['part']} returned {len(r['out'])} items, expected {expected[r['part']]}\n"
        )
        raise SystemExit(2)
# item i is at index i // n of part i % n (the part slices are strided), so
# this rebuilds the original's item order before summing.
out = [rows[i % n]["out"][i // n] for i in range(items)]
dt = max(r["seconds"] for r in rows)
print("POOLMAP-OK items=%d seconds=%.4f checksum=%.6f" % (items, dt, sum(out)))
EOF
