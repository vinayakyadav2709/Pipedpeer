#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/pool_map.py (W1) to plain ssh: the item
# list is split across the PCs in LAB_HOSTS_FILE (ips, one per line; LAB_N says
# how many), each PC runs its part's Pool through GNU parallel --sshlogin, and
# pc00 joins the parts and prints the original's one result line. The "1/" slots in
# the login file give each host one job and override -j1, so the parts run at
# once and seconds is the slowest part (the harness times the run itself); the
# checksum is recomputed over the whole item list in order, so it equals the
# original's bit for bit.
# Distributed: the Pool items, one part per PC. Local: the join and the line.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Env: LAB_HOSTS_FILE, LAB_N, LAB_USER (same lab user on every PC), LAB_PY,
# LAB_WORKDIR (the PC-side layout's ~/pipedpeer-lab/work), BENCH_* (original).
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/w1-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

# "1/<user>@<ip>": one slot per host (parallel's sshloginfile format).
while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"$py baselines/w1_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

"$py" - "$LAB_N" "$tmpd/parts.json" <<'EOF'
import json
import sys

n = int(sys.argv[1])
rows = []
for line in open(sys.argv[2]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
rows.sort(key=lambda r: r["part"])
assert len(rows) == n, f"expected {n} parts, got {len(rows)}"
out = [v for r in rows for v in r["out"]]
expected_items = int(__import__("os").environ.get("BENCH_ITEMS", "0"))
assert len(out) == expected_items, f"expected {expected_items} items, got {len(out)}"
dt = max(r["seconds"] for r in rows)
print("POOLMAP-OK items=%d seconds=%.4f checksum=%.6f" % (expected_items, dt, sum(out)))
EOF
