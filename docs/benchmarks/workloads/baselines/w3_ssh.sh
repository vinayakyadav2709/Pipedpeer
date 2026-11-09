#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/w3_rf.py (W3) to plain ssh (see w1_ssh.sh).
# BENCH_TREES is inlined into parallel's remote command (ssh carries no
# environment, H3), and the join checks that the parts trained the whole forest
# and names any part that came back short.
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
: "${LAB_N:?LAB_N: the number of PCs in this run}"
trees=${BENCH_TREES:-150}
case $trees in
'' | *[!0-9]*) echo "w3_ssh.sh: BENCH_TREES must be a whole number, got '$trees'" >&2; exit 2 ;;
esac
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/w3-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done <"$LAB_HOSTS_FILE" >"$tmpd/login"

seq 0 $((LAB_N - 1)) | parallel -j1 --sshloginfile "$tmpd/login" --workdir "$workdir" \
	"BENCH_TREES=$trees $py baselines/w3_ssh_part.py --part {} --parts $LAB_N" >"$tmpd/parts.json"

"$py" - "$LAB_N" "$tmpd/parts.json" "$trees" <<'PYEOF'
import json, os, sys
import numpy as np
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

n = int(sys.argv[1])
trees = int(sys.argv[3])
rng = np.random.RandomState(7)
n_samples, n_features = 200_000, 20
X = rng.rand(n_samples, n_features)
y = ((X[:, 0] * 2.0 + X[:, 1] - X[:, 2] * X[:, 3] + rng.normal(0, 0.05, n_samples)) > 0.5).astype(int)
_, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

rows = []
for line in open(sys.argv[2]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
rows.sort(key=lambda r: r["part"])
if len(rows) != n:
    sys.stderr.write(f"w3_ssh: expected {n} parts, got {len(rows)}\n")
    raise SystemExit(2)
for r in rows:
    p = r["part"]
    expected = trees // n + (1 if p < trees % n else 0)
    if r.get("trees") != expected:
        sys.stderr.write(f"w3_ssh: part {p} trained {r.get('trees')} trees, expected {expected}\n")
        raise SystemExit(2)
proba = np.mean([np.array(r["proba"]) for r in rows], axis=0)
y_pred = proba.argmax(axis=1)
acc = accuracy_score(y_test, y_pred)
print(f"accuracy: {acc:.4f}")
PYEOF
