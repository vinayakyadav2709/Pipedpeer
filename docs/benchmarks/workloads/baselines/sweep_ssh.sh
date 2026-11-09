#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/sweep_map.py (SW1) to plain ssh: the item
# list is split across the PCs in LAB_HOSTS_FILE (ips, one per line; LAB_N says
# how many), each PC runs its part's Pool through GNU parallel --sshlogin, and
# pc00 joins the parts and prints sweep_map.py's one result line. The "1/"
# slots in the login file give each host one job and override -j1, so the parts
# run at once and seconds is the slowest part (the harness times the run); the
# checksum is recomputed over the whole item list in order.
# Distributed: the sweep items, one strided part per PC.
# Local: building the items and (payload arms) one fixed-record payload file per
# part, the join and the line.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Payload arms ship each part's file with --transferfile (and --cleanup); the
# part reads those records instead of regenerating them, so the transfer is
# measured rather than hidden. All BENCH_* values are inlined in the remote
# command: parallel does not pass this shell's environment to an --sshlogin
# job.
# Env: LAB_HOSTS_FILE, LAB_N, LAB_USER (same lab user on every PC), LAB_PY,
# LAB_WORKDIR (the PC-side layout's ~/pipedpeer-lab/work), BENCH_*.
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
: "${BENCH_ITEMS:?BENCH_ITEMS is not set}"
: "${BENCH_TASK_MS:?BENCH_TASK_MS is not set}"
: "${BENCH_ITERS_PER_MS:?BENCH_ITERS_PER_MS is not set}"
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
procs=${BENCH_PROCS:-0}
payload_bytes=${BENCH_PAYLOAD_BYTES:-0}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/sweep-ssh.XXXXXX")
pfx="sweep-part-$$"
trap 'rm -rf "$tmpd"; rm -f "$pfx"-part-*.bin' EXIT

# "1/<user>@<ip>": one slot per host (parallel's sshloginfile format).
while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

if [ "$payload_bytes" -gt 0 ]; then
	# One fixed-record file per part, in that part's item order. The /./ in the
	# --transferfile path makes parallel place it under the remote work dir.
	"$py" - "$BENCH_ITEMS" "$LAB_N" "$payload_bytes" "$tmpd/$pfx" <<'PYEOF'
import sys

items, parts, record = (int(x) for x in sys.argv[1:4])
prefix = sys.argv[4]
for part in range(parts):
    with open(f"{prefix}-part-{part}.bin", "wb") as f:
        for i in range(part, items, parts):
            f.write(bytes([i % 251]) * record)
PYEOF
	seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
		--transferfile "$tmpd/./$pfx-part-{}.bin" --cleanup \
		"BENCH_ITEMS=$BENCH_ITEMS BENCH_TASK_MS=$BENCH_TASK_MS BENCH_ITERS_PER_MS=$BENCH_ITERS_PER_MS BENCH_PROCS=$procs BENCH_PAYLOAD_BYTES=$payload_bytes $py baselines/sweep_ssh_part.py --part {} --parts $LAB_N --payload-file $pfx-part-{}.bin" \
		>"$tmpd/parts.json"
else
	seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
		"BENCH_ITEMS=$BENCH_ITEMS BENCH_TASK_MS=$BENCH_TASK_MS BENCH_ITERS_PER_MS=$BENCH_ITERS_PER_MS BENCH_PROCS=$procs BENCH_PAYLOAD_BYTES=$payload_bytes $py baselines/sweep_ssh_part.py --part {} --parts $LAB_N" \
		>"$tmpd/parts.json"
fi

"$py" - "$LAB_N" "$BENCH_ITEMS" "$tmpd/parts.json" <<'PYEOF'
import json
import sys

parts = int(sys.argv[1])
items = int(sys.argv[2])
rows = []
for line in open(sys.argv[3]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
got = {r["part"]: r for r in rows}
missing = [p for p in range(parts) if p not in got]
if missing:
    sys.exit(f"sweep_ssh.sh: part {missing[0]} returned no result")
expected = [len(range(p, items, parts)) for p in range(parts)]
for p, r in got.items():
    if r.get("items") != expected[p]:
        sys.exit(
            f"sweep_ssh.sh: part {p} returned {r.get('items')} items, "
            f"expected {expected[p]}"
        )
    if len(r.get("out", [])) != expected[p]:
        sys.exit(f"sweep_ssh.sh: part {p} returned {len(r.get('out', []))} values, expected {expected[p]}")
out = [got[i % parts]["out"][i // parts] for i in range(items)]
dt = max(r["seconds"] for r in rows)
print("SWEEP-OK items=%d seconds=%.4f checksum=%.6f" % (items, dt, sum(out)))
PYEOF
