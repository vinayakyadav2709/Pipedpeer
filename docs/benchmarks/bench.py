#!/usr/bin/env python3
"""Measure the numbers the report's results chapter needs.

Nothing here estimates or extrapolates: every field written to results.json
is either a wall-clock measurement of a real `pipedpeer run`, or a recorded
failure. Runs happen against whatever cluster is already up; bring one up
with scripts/lab-up.sh first.

  python3 docs/benchmarks/bench.py --repeat 5
"""
import argparse
import json
import os
import pathlib
import platform
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
CLI = ROOT / "bin" / "pipedpeer"
WORKLOADS = pathlib.Path(__file__).resolve().parent / "workloads"
RAW = pathlib.Path(__file__).resolve().parent / "raw"
LAB_PORTS = (38081, 38082, 38083)


def health(port, timeout=2):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def live_ports():
    return [p for p in LAB_PORTS if health(p)]


def run_cli(args, cwd, env=None, timeout=900):
    """Run the CLI and return (seconds, returncode, combined output)."""
    e = dict(os.environ)
    e.update(env or {})
    t0 = time.perf_counter()
    proc = subprocess.run([str(CLI)] + args, cwd=str(cwd), env=e,
                          capture_output=True, text=True, timeout=timeout)
    return time.perf_counter() - t0, proc.returncode, proc.stdout + proc.stderr


def workspace_for(script_name):
    """Each workload gets its own directory, anchored so the job ships only
    that directory (findProjectRoot walks up for .pipedpeerignore or .git)."""
    d = RAW / "ws" / pathlib.Path(script_name).stem
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    (d / ".pipedpeerignore").write_text("results/\n")
    shutil.copy(WORKLOADS / script_name, d / script_name)
    return d


def summarise(samples):
    ok = [s for s in samples if s["ok"]]
    secs = [s["seconds"] for s in ok]
    return {
        "runs": len(samples),
        "ok": len(ok),
        "median_s": round(statistics.median(secs), 4) if secs else None,
        "min_s": round(min(secs), 4) if secs else None,
        "max_s": round(max(secs), 4) if secs else None,
        "samples": samples,
    }


def measure(label, script, repeat, extra_args=(), env=None, verify_token=None):
    ws = workspace_for(script)
    samples = []
    for i in range(repeat):
        secs, rc, out = run_cli([ "run", script, *extra_args ], ws, env)
        ok = rc == 0 and (verify_token is None or verify_token in out)
        samples.append({"iteration": i, "seconds": round(secs, 4), "ok": ok,
                        "returncode": rc})
        (RAW / f"{label}_run{i}.log").write_text(out)
        print(f"  {label} [{i}] {secs:7.3f}s rc={rc} ok={ok}", flush=True)
    return summarise(samples)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=5)
    args = ap.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    if not CLI.exists():
        sys.exit(f"build the CLI first: {CLI} missing")

    ports = live_ports()
    print(f"lab workers up: {ports}")

    results = {
        "host": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "cluster": {"lab_ports": ports, "worker_count": len(ports)},
        "note": ("All workers are containers on a single host, so these figures "
                 "measure overhead, cache behaviour and correctness. They do not "
                 "measure multi-machine speedup: every worker shares this host's "
                 "CPU and memory."),
        "measurements": {},
    }

    print("\nN1 end-to-end latency, trivial job")
    results["measurements"]["n1_trivial_latency"] = measure(
        "N1_trivial", "trivial.py", args.repeat,
        extra_args=["--remote"], verify_token="TRIVIAL-OK")

    print("\nN7 interception overhead when the cost model declines")
    results["measurements"]["n7_shim_declines"] = measure(
        "N7_shim_on", "pool_map.py", args.repeat,
        extra_args=["--remote"], verify_token="POOLMAP-OK")
    results["measurements"]["n7_shim_off"] = measure(
        "N7_shim_off", "pool_map.py", args.repeat,
        extra_args=["--remote", "-e", "PIPEDPEER_SHIM=0"],
        verify_token="POOLMAP-OK")

    print("\nN3 Pool.map with the cluster forced on")
    results["measurements"]["n3_forced_distribute"] = measure(
        "N3_forced", "pool_map.py", args.repeat,
        extra_args=["--remote", "--distribute", "force"],
        verify_token="POOLMAP-OK")

    print("\nN5 numpy: matmul should stay local, SVD may move")
    results["measurements"]["n5_numpy"] = measure(
        "N5_numpy", "numpy_ops.py", max(2, args.repeat // 2),
        extra_args=["--remote"], verify_token="NUMPY-OK")

    out = pathlib.Path(__file__).resolve().parent / "results.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
