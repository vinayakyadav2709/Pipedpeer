import csv
import json
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSE = os.path.join(HERE, "..", "analyse.py")


def synth(path):
    """Plain W1 at N=1 takes 100 s; pipedpeer and ray scale perfectly, dask at
    half; every group has a warm-up (much slower) and one failed run."""
    rng = random.Random(1)
    lines = []

    def add(**kw):
        base = {"experiment": "e1", "workload": "W1", "warmup": False, "result_ok": True, "error": None}
        base.update(kw)
        lines.append(base)

    for system, eff in (("plain", 1.0), ("pipedpeer", 1.0), ("ray", 1.0), ("dask", 0.5)):
        for n in ([1] if system == "plain" else [1, 2, 4]):
            ideal = 100.0 / (n * eff if n > 1 else 1)
            add(system=system, n=n, repeat=0, warmup=True, wall_s=ideal * 3)
            for rep in range(1, 6):
                add(system=system, n=n, repeat=rep, wall_s=ideal + rng.uniform(-1, 1))
            add(system=system, n=n, repeat=6, wall_s=1.0, error="exit 1", result_ok=False, failing_pc="pc01")
    for n in (1, 4):
        for rep in range(1, 4):
            lines.append({"experiment": "e1b", "workload": "W1", "system": "pipedpeer", "n": n, "mode": "weak",
                          "repeat": rep, "warmup": False, "result_ok": True, "wall_s": 60.0 * (1 if n == 1 else 1.25)})
    for s in ("pipedpeer", "pipedpeer-headcount", "ray", "dask"):
        for rep in range(1, 4):
            lines.append({"experiment": "e4", "workload": "W1", "system": s, "n": 4, "repeat": rep,
                          "warmup": False, "result_ok": True, "wall_s": 40 + rep})
    for m in ("pipedpeer", "pip", "ray"):
        lines.append({"experiment": "e5", "workload": "E5-env", "system": m, "n": 32, "phase": "cold",
                      "repeat": 0, "result_ok": True, "wall_s": 300, "mb_down": 2000})
    for n in (8, 16):
        lines.append({"experiment": "e11", "workload": "E11-idle", "system": "pipedpeer", "n": n, "repeat": 1,
                      "result_ok": True, "join_s": 5.0, "bytes_per_node_per_min": 1000.0 * n})
    for s in ("pipedpeer-exact", "torchrun"):
        lines.append({"experiment": "e8", "workload": "T1", "system": s, "n": 2, "variant": "mlp-c", "repeat": 1,
                      "result_ok": True, "wall_s": 50, "result": "BENCH_LOSS 0.4321", "bytes_per_rank": 10 ** 6})
    with open(path, "w") as f:
        for ln in lines:
            f.write(json.dumps(ln) + "\n")


def rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def test_speedup_median_iqr_and_exclusions(tmp_path):
    runs = tmp_path / "runs.jsonl"
    synth(runs)
    p = subprocess.run([sys.executable, ANALYSE, "--runs", str(runs)], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    e1 = {(r["system"], int(r["n"])): r for r in rows(tmp_path / "csv" / "e1.csv")}
    assert set(e1) == {("plain", 1), ("pipedpeer", 1), ("pipedpeer", 2), ("pipedpeer", 4),
                       ("ray", 1), ("ray", 2), ("ray", 4), ("dask", 1), ("dask", 2), ("dask", 4)}
    for r in e1.values():
        assert r["runs_used"] == "5" and r["warmups_excluded"] == "1" and r["failed_excluded"] == "1"
        assert float(r["iqr_wall_s"]) < 2.0, "a warm-up or failed run leaked into the stats"
    assert abs(float(e1[("pipedpeer", 4)]["speedup_vs_plain"]) - 4.0) < 0.1
    assert abs(float(e1[("dask", 4)]["speedup_vs_plain"]) - 2.0) < 0.1
    assert abs(float(e1[("plain", 1)]["speedup_vs_plain"]) - 1.0) < 1e-9
    e1b = {int(r["n"]): float(r["weak_efficiency"]) for r in rows(tmp_path / "csv" / "e1b.csv")}
    assert e1b == {1: 1.0, 4: 0.8}
    for name in ("e1_speedup", "e1b_efficiency", "e4_uneven", "e5_env", "e5_env_mb", "e11_membership"):
        for ext in ("png", "pdf"):
            assert (tmp_path / "figures" / ("%s.%s" % (name, ext))).stat().st_size > 1000, name
    table = (tmp_path / "e8_table.md").read_text()
    assert "| mlp-c | 2 | torchrun | 1 | 50.00 |" in table and "0.4321" in table
    summary = (tmp_path / "summary.txt").read_text()
    assert "e1: 70 runs, 50 used, 10 warm-ups excluded, 10 failed excluded" in summary
    assert "FAILED" in summary and "pc01" in summary


def test_empty_experiment_skips_its_figure(tmp_path):
    runs = tmp_path / "runs.jsonl"
    runs.write_text(json.dumps({"experiment": "e4", "workload": "W1", "system": "ray", "n": 4,
                                "warmup": False, "error": "boom", "result_ok": False}) + "\n")
    p = subprocess.run([sys.executable, ANALYSE, "--runs", str(runs)], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert "skipped (no usable runs): e4" in p.stdout
    assert not (tmp_path / "figures" / "e4_uneven.png").exists()
