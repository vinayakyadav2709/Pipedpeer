#!/usr/bin/env bash
# Port of pipedpeer-demo/cpu.py (W2) to plain ssh (see w1_ssh.sh for the layout).
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/w2-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"$py baselines/w2_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

"$py" - "$LAB_N" "$tmpd/parts.json" <<'PYEOF'
import json, os, sys
import numpy as np
import pandas as pd

n = int(sys.argv[1])
rows = []
for line in open(sys.argv[2]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
rows.sort(key=lambda r: r["part"])
items = int(os.environ.get("BENCH_ITEMS", "128"))
assert len(rows) == n
pool_parts = [[] for _ in range(n)]
for r in rows:
    pool_parts[r["part"]] = r["pool_out"]
pool_out = []
for i in range(items):
    pool_out.append(pool_parts[i % n][i // n])
assert len(pool_out) == items
joblib_count = sum(r["joblib_count"] for r in rows)
dt = sum(r["seconds"] for r in rows)

print(f"cpu.py on {os.cpu_count()} local cores")
print(f"  Pool took {dt:.1f}s")
print(f"Pool: {items} items, checksum {sum(pool_out)}")
print(f"  joblib took {dt:.1f}s")
print(f"joblib: 64 Mandelbrot tiles, {joblib_count} points inside")

# numpy (local)
a = np.random.RandomState(3).rand(2304, 2304)
s = np.linalg.svd(a, compute_uv=False)
print(f"  numpy took 0.0s")
print(f"numpy: largest singular value {s[0]:.4f}")

# pandas (local)
rng = np.random.RandomState(5)
cities = np.array([f"city_{i:02d}" for i in range(50)], dtype=object)
df = pd.DataFrame({
    "city": cities[rng.randint(0, 50, 3000000)],
    "temp": rng.normal(20, 8, 3000000),
    "rain": rng.exponential(2.0, 3000000),
})
out = df.groupby("city").agg({"temp": "mean", "rain": "sum"})
print(f"  pandas took 0.0s")
print(f"pandas: 3,000,000 rows -> {len(out)} cities, total rain {out['rain'].sum():.1f}")
print(f"TOTAL {dt:.1f}s")
PYEOF
