"""Run every workload's plain, ray and dask versions on this one machine at
the smoke size and check their result lines agree (workloads.json).

    $LAB_ENV/bin/python3 scripts/campaign/env/smoke.py [procs]

Starts a local Ray head and a Dask scheduler plus worker (procs CPUs each,
default 4), stops them at the end. Run it on a machine with no campaign
cluster up: the cleanup is `ray stop --force`. Prints one row per run and exits 1 when a
port's result differs from plain's or a run fails. T1 runs plain and torchrun
with 2 ranks for one epoch and only checks both print a loss.
"""
import json
import os
import re
import subprocess
import sys
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.dirname(sys.executable)
PROCS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
SCHED = "tcp://127.0.0.1:8786"


def result(w, out):
    return "\n".join(m.group(0) for m in re.finditer(w["result_regex"], out))


def run(w, system, script, extra=None, argv=None):
    env = dict(os.environ, **w["smoke"], RAY_ADDRESS="127.0.0.1:6379", DASK_SCHEDULER=SCHED)
    for k, v in w["system_env"].get(system, {}).items():
        env[k] = v.format(nproc_submitter=PROCS, nproc_workers=PROCS)
    env.update(extra or {})
    cmd = argv or [sys.executable, os.path.join(REPO, script)]
    t0 = time.monotonic()
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=REPO)
    wall = time.monotonic() - t0
    if p.returncode != 0:
        sys.stderr.write(p.stdout + p.stderr)
    return p.returncode, wall, result(w, p.stdout)


def start_clusters():
    subprocess.run([os.path.join(BIN, "ray"), "start", "--head", f"--num-cpus={PROCS}",
                    "--include-dashboard=false", "--disable-usage-stats"],
                   check=True, capture_output=True)
    state = os.path.expanduser("~/.local/share/pipedpeer-lab")
    os.makedirs(state, exist_ok=True)
    log = open(os.path.join(state, "smoke-dask.log"), "w")
    sched = subprocess.Popen([os.path.join(BIN, "dask"), "scheduler", "--port", "8786",
                              "--dashboard-address", ":0"], stdout=log, stderr=log)
    worker = subprocess.Popen([os.path.join(BIN, "dask"), "worker", SCHED, "--nworkers",
                               str(PROCS), "--nthreads", "1", "--dashboard-address", ":0",
                               # auto would give each worker RAM x 1/os.cpu_count(): too
                               # little when PROCS is below the core count
                               "--memory-limit", "0"],
                              stdout=log, stderr=log)
    return [worker, sched]


def main():
    spec = json.load(open(os.path.join(REPO, "scripts/campaign/workloads.json")))
    procs = start_clusters()
    time.sleep(5)  # workers register with the scheduler
    bad = 0
    print("workload system rc wall_s result")
    try:
        for w in spec["workloads"]:
            if w["id"] == "T1":
                for name, var in w["variants"].items():
                    rc, wall, res = run(w, "plain", w["script"], var)
                    print(f"T1/{name} plain {rc} {wall:.1f} {res!r}")
                    rc2, wall2, res2 = run(w, "torchrun", None, var, [
                        os.path.join(BIN, "torchrun"), "--standalone", "--nproc-per-node=2",
                        os.path.join(REPO, w["ports"]["torchrun"])])
                    print(f"T1/{name} torchrun {rc2} {wall2:.1f} {res2!r}")
                    bad += rc != 0 or rc2 != 0 or not res or not res2
                continue
            rc, wall, ref = run(w, "plain", w["script"])
            print(f"{w['id']} plain {rc} {wall:.1f} {ref!r}")
            bad += rc != 0 or not ref
            for system, script in w["ports"].items():
                rc, wall, res = run(w, system, script)
                same = rc == 0 and res == ref
                print(f"{w['id']} {system} {rc} {wall:.1f} {res!r} {'same' if same else 'DIFFERS'}")
                bad += not same
    finally:
        subprocess.run([os.path.join(BIN, "ray"), "stop", "--force"], capture_output=True)
        for p in procs:
            p.terminate()
            p.wait()
    print("SMOKE", "FAIL" if bad else "OK")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
