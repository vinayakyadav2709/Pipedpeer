#!/usr/bin/env bash
# Port of pipedpeer-demo/cpu.py (W2) to plain ssh (see w1_ssh.sh for the layout).
# --items N sets the Pool size, for the parts and the join; without it the size
# is BENCH_ITEMS when set, else cpu.py's 128 (H4). The parts get BENCH_ITEMS and
# BENCH_PROCS inlined into parallel's remote command, because ssh carries no
# environment (H3). The stage timings and TOTAL are measured: the Pool+joblib
# time is the slowest PC's, the numpy and pandas stages run on pc00 (H5).
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
items=${BENCH_ITEMS:-128}
while (( $# )); do
	case $1 in
	--items)
		[ $# -ge 2 ] || { echo "w2_ssh.sh: --items needs a value" >&2; exit 2; }
		items=$2
		shift 2
		;;
	--items=*)
		items=${1#--items=}
		shift
		;;
	*)
		echo "w2_ssh.sh: unknown argument '$1'" >&2
		exit 2
		;;
	esac
done
case $items in
'' | *[!0-9]*) echo "w2_ssh.sh: --items must be a whole number, got '$items'" >&2; exit 2 ;;
esac
procs=${BENCH_PROCS:-0}
case $procs in
'' | *[!0-9]*) echo "w2_ssh.sh: BENCH_PROCS must be 0 or a whole number, got '$procs'" >&2; exit 2 ;;
esac
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/w2-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"BENCH_ITEMS=$items BENCH_PROCS=$procs $py baselines/w2_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

"$py" - "$LAB_N" "$tmpd/parts.json" "$items" <<'PYEOF'
import json, os, sys
import time
import numpy as np
import pandas as pd

n = int(sys.argv[1])
items = int(sys.argv[3])
rows = []
for line in open(sys.argv[2]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
rows.sort(key=lambda r: r["part"])
if len(rows) != n:
    sys.stderr.write(f"w2_ssh: expected {n} parts, got {len(rows)}\n")
    raise SystemExit(2)
pool_parts = [[] for _ in range(n)]
for r in rows:
    pool_parts[r["part"]] = r["pool_out"]
pool_out = []
for i in range(items):
    part = i % n
    try:
        pool_out.append(pool_parts[part][i // n])
    except IndexError:
        sys.stderr.write(f"w2_ssh: part {part} returned too few items for {items} items\n")
        raise SystemExit(2)
joblib_count = sum(r["joblib_count"] for r in rows)
dt = max(r["seconds"] for r in rows)  # slowest PC's Pool+joblib

print(f"cpu.py on {os.cpu_count()} local cores")
print(f"  Pool and joblib took {dt:.1f}s together (slowest PC)")
print(f"Pool: {items} items, checksum {sum(pool_out)}")
print(f"joblib: 64 Mandelbrot tiles, {joblib_count} points inside")

# numpy (local, on pc00)
t0 = time.monotonic()
a = np.random.RandomState(3).rand(2304, 2304)
s = np.linalg.svd(a, compute_uv=False)
numpy_s = time.monotonic() - t0
print(f"  numpy took {numpy_s:.1f}s")
print(f"numpy: largest singular value {s[0]:.4f}")

# pandas (local, on pc00)
t0 = time.monotonic()
rng = np.random.RandomState(5)
cities = np.array([f"city_{i:02d}" for i in range(50)], dtype=object)
df = pd.DataFrame({
    "city": cities[rng.randint(0, 50, 3000000)],
    "temp": rng.normal(20, 8, 3000000),
    "rain": rng.exponential(2.0, 3000000),
})
out = df.groupby("city").agg({"temp": "mean", "rain": "sum"})
pandas_s = time.monotonic() - t0
print(f"  pandas took {pandas_s:.1f}s")
print(f"pandas: 3,000,000 rows -> {len(out)} cities, total rain {out['rain'].sum():.1f}")
# Same shape as cpu.py's TOTAL: the distributed stages on the slowest PC plus
# the stages that stay on pc00.
print(f"TOTAL {dt + numpy_s + pandas_s:.1f}s")
PYEOF
