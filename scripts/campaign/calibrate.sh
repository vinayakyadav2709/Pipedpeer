#!/usr/bin/env bash
# Find a workload's campaign sizes on this machine: the strong-scaling size
# one PC alone runs in about 125 s, and for workloads with weak scaling (W1)
# the per-PC size that is about 60 s of work on one PC (the runner multiplies
# it by N). docs/benchmarks/lab-campaign.md 1.2; results/lab/workloads.md.
#
#   scripts/campaign/calibrate.sh W1|W2|W3 [strong_s] [weak_s]
#
# Runs the plain workload with the campaign env's python (PY, default
# ~/.local/share/pipedpeer-lab/env/bin/python3), doubling the size from the
# smoke size until one run takes at least CAL_MIN_S (8) seconds, then once
# more at 3x that size, and fits wall = c0 + c1 * size through the two points
# (c0 is start-up and the fixed sections, c1 the per-unit cost). Each point is
# the fastest of CAL_REPS (2) runs. W1's BENCH_PROCS is this machine's nproc,
# as for every plain run. CAL_VERIFY=1 then runs the strong size once and
# records its wall as verify_s. The last line is `CALIBRATE {json}` for the runner.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
PY="${PY:-$HOME/.local/share/pipedpeer-lab/env/bin/python3}"
[[ -x $PY ]] || { echo "no python at $PY; run scripts/campaign/env/install.sh or set PY" >&2; exit 2; }
[[ $# -ge 1 ]] || { echo "usage: $0 W1|W2|W3 [strong_s] [weak_s]" >&2; exit 2; }

exec "$PY" - "$@" <<'PY'
import json, os, re, subprocess, sys, time

wid = sys.argv[1]
strong_s = float(sys.argv[2]) if len(sys.argv) > 2 else 125.0
weak_s = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0
min_s = float(os.environ.get("CAL_MIN_S", "8"))
reps = int(os.environ.get("CAL_REPS", "2"))
nproc = os.cpu_count()

spec = json.load(open("scripts/campaign/workloads.json"))
w = next((w for w in spec["workloads"] if w["id"] == wid), None)
if w is None or w["strong"] is None:
    sys.exit(f"{wid}: not a workload with a calibrated size")
var = w["size_var"]


def wall(size):
    env = dict(os.environ, **{var: str(size)})
    for k, v in w["system_env"]["plain"].items():
        env[k] = v.format(nproc_submitter=nproc, nproc_workers=nproc)
    best = None
    for _ in range(reps):
        t0 = time.monotonic()
        p = subprocess.run([sys.executable, w["script"], *w["args"]], env=env,
                           capture_output=True, text=True)
        t = time.monotonic() - t0
        if p.returncode != 0 or not re.search(w["result_regex"], p.stdout):
            sys.exit(f"{wid} {var}={size} failed:\n{p.stdout}{p.stderr}")
        best = t if best is None else min(best, t)
    print(f"  {wid} {var}={size}: {best:.2f} s (best of {reps})", flush=True)
    return best


x1 = int(w["smoke"][var])
t1 = wall(x1)
while t1 < min_s:
    x1 *= 2
    t1 = wall(x1)
x2 = 3 * x1
t2 = wall(x2)
c1 = (t2 - t1) / (x2 - x1)
c0 = t1 - c1 * x1
if c1 <= 0:
    sys.exit(f"{wid}: wall did not grow with {var} ({t1:.2f} s -> {t2:.2f} s); machine too noisy, rerun")


def size_for(target):
    return max(1, round((target - c0) / c1))


out = {"workload": wid, "host": os.uname().nodename, "nproc": nproc, "var": var,
       "c0_s": round(c0, 3), "c1_s": c1, "points": [[x1, round(t1, 3)], [x2, round(t2, 3)]],
       "strong": {var: str(size_for(strong_s))}, "strong_target_s": strong_s}
print(f"{wid} on {out['host']} ({nproc} cpus): wall = {c0:.2f} s + {c1:.3g} s x {var}")
print(f"  strong ({strong_s:.0f} s on this PC): {var}={out['strong'][var]}")
if w["weak_per_pc"] is not None:
    out["weak_per_pc"] = {var: str(size_for(weak_s))}
    out["weak_target_s"] = weak_s
    print(f"  weak ({weak_s:.0f} s per PC): {var}={out['weak_per_pc'][var]} x N")
if os.environ.get("CAL_VERIFY") == "1":
    reps = 1
    out["verify_s"] = round(wall(int(out["strong"][var])), 2)
print("CALIBRATE " + json.dumps(out))
PY
