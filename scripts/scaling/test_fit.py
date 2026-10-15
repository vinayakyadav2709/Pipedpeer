import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import fit


def test_slope_recovers_exponent():
    xs = [1, 10, 100, 1000]
    assert abs(fit.slope(xs, [3 * x**1.5 for x in xs]) - 1.5) < 1e-9
    assert fit.slope([5], [1]) is None
    assert fit.slope([0, 1], [1, 1]) is None


def test_flags_superlinear_only_beyond_margin():
    xs = [1, 4, 16, 64, 256]
    series = {
        "send": [(x, 0.01 * x**1.4) for x in xs],     # 1.4 > 1 + 0.15
        "pickle": [(x, 0.01 * x**1.1) for x in xs],   # within margin
        "queue": [(x, 0.5) for x in xs],              # flat, expected flat
        "compute": [(x, 1.0 if x < 64 else 0.02 * x**2) for x in xs],  # only the tail grows
    }
    got = {f["step"]: f["superlinear"] for f in fit.fit("payload", series)}
    assert got == {"send": True, "pickle": False, "queue": False, "compute": True}


def test_report_reads_csv_and_records(tmp_path):
    rows = []
    for val in (1, 10, 100):
        log = str(tmp_path / f"r{val}.log")
        open(log, "w").close()
        steps = [{"step": "send", "dur_ns": int(1e6 * val**2), "chunk": c} for c in (0, 1)]
        with open(log + ".steps.json", "w") as f:
            json.dump({"steps": steps}, f)
        rows.append({"suite": "m2", "case": "payload", "variant": str(val), "status": "ok",
                     "wall_s": "1.0", "script_s": "0.5", "log": log})
    rows.append(dict(rows[0], status="fail"))
    with open(tmp_path / "cpu.csv", "w") as f:
        f.write("log,cpu_s,cpu_submitter_s,cpu_peers_s,load1_before,load1_after\n")
        for r in rows[:3]:
            v = float(r["variant"])
            f.write(f"{r['log']},{v**1.5:.3f},{v:.3f},{v:.3f},30,31\n")
    path = tmp_path / "results.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    out = fit.report(fit.load(str(path)))
    # Wall-clock steps hint, CPU series flag.
    assert "send vs payload" not in out
    assert "wall only" in out
    assert "- cpu vs payload: exponent +1.50" in out
    assert "| payload | runs | cpu | cpu_peers | cpu_submitter | load | script | send | wall |" in out
