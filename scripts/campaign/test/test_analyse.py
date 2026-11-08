"""Case table for scripts/campaign/analyse.py on synthetic runs.jsonl.

Each test maps to a row of the brief's case table (S11); two labs: the main one
covering the analysis cases, a "tight" one with only well-behaved repeats.
"""
import csv
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent
ANALYSE = HERE / "analyse.py"

VMSTAT_TMPL = """\
procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu----- -----timestamp-----
 r  b   swpd   free   buff  cache    si   so    bi    bo    in    cs us sy id wa st
 1  0      0 100000  20000 300000     0    0     5    10    50    20 {u1} {s1} 10  0  0 2026-11-08 10:00:00
 2  0      0 100000  20000 300000     0    0     5    10    50    20 {u2} {s2} 10  0  0 2026-11-08 10:00:01
"""


def _row(exp, workload, system, n, repeat, wall, warmup=False, ok=True,
         size=None, ref="majority"):
    size = {"BENCH_ITEMS": 4800} if size is None else size
    return {
        "run_id": f"{exp}-{workload}-{system}-n{n}-r{repeat}",
        "experiment": exp, "workload": workload, "system": system, "n": n,
        "repeat": repeat, "warmup": warmup, "size": size,
        "start_ns": 0, "end_ns": 0, "first_out_ns": 0, "wall_s": wall,
        "exit_code": 0, "result_lines": [], "result_ok": ok, "ref": ref,
        "hosts": ["pc00"], "git_commit": "709941d",
        "pipedpeer_version": "0.0.1", "receipt": None, "timeline": None,
        "note": "",
    }


def _seq(exp, workload, system, n, walls, warmup=(), ok=True, size=None,
         ref="majority"):
    """One group: warmup row r0 + repeats r1..rN."""
    out = [_row(exp, workload, system, n, 0, w, warmup=True, size=size, ref=ref)
           for w in warmup]
    out += [_row(exp, workload, system, n, i, w, ok=ok, size=size, ref=ref)
            for i, w in enumerate(walls, 1)]
    return out


def _main_rows():
    r = []
    # E1 W1: pipedpeer gets warm-up 500 s, a result_ok=false repeat, noisy CI
    r += _seq("E1", "W1", "pipedpeer", 1, [100, 99, 101, 100, 100],
              warmup=(500,), ref="reference")
    n2 = _seq("E1", "W1", "pipedpeer", 2, [48, 50, 52, 49, 51])
    n2[-1]["result_ok"] = False
    r += n2
    r += _seq("E1", "W1", "pipedpeer", 4, [25, 25, 25, 26, 25])
    r += _seq("E1", "W1", "plain", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W1", "plain", 2, [60] * 5)      # ratio 1.2 -> pipedpeer_faster
    r += _seq("E1", "W1", "plain", 4, [25] * 5)
    r += _seq("E1", "W1", "ray", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W1", "ray", 2, [50, 50, 50, 50, 150])  # 3x median, no n=4
    r += _seq("E1", "W1", "dask", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W1", "dask", 2, [49, 50, 50, 51, 52])  # same_within_10pct
    r += _seq("E1", "W1", "dask", 4, [13] * 5)
    # E1 W2: 1.5x repeat (no warn), unresolved ratio, spread 0.2
    r += _seq("E1", "W2", "pipedpeer", 1, [100, 100, 100, 100, 150],
              ref="reference")
    r += _seq("E1", "W2", "pipedpeer", 2, [48, 49, 50, 51, 52])
    r += _seq("E1", "W2", "pipedpeer", 4, [12.5] * 5)
    r += _seq("E1", "W2", "plain", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W2", "plain", 2, [40, 45, 50, 55, 60])  # unresolved
    r += _seq("E1", "W2", "plain", 4, [25] * 5)
    r += _seq("E1", "W2", "ray", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W2", "ray", 2, [50] * 5)
    r += _seq("E1", "W2", "dask", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W2", "dask", 2, [50] * 5)        # speedup exactly 2.0
    r += _seq("E1", "W2", "dask", 4, [25] * 5)
    # E1 W3: other_faster ratio
    r += _seq("E1", "W3", "pipedpeer", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W3", "pipedpeer", 2, [50] * 5)
    r += _seq("E1", "W3", "pipedpeer", 4, [12.5] * 5)
    r += _seq("E1", "W3", "plain", 1, [100] * 5, ref="reference")
    r += _seq("E1", "W3", "plain", 2, [44, 44, 44, 44, 45])  # other_faster
    r += _seq("E1", "W3", "plain", 4, [25] * 5)
    # E1b weak scaling -> efficiency = t(1)/t(N)
    r += _seq("E1b", "W1", "pipedpeer", 1, [100] * 5,
              size={"BENCH_ITEMS": 300}, ref="reference")
    r += _seq("E1b", "W1", "pipedpeer", 2, [98] * 5,
              size={"BENCH_ITEMS": 600})
    return r


def _tight_rows():
    r = _seq("E1", "W1", "pipedpeer", 1, [100, 99, 101, 100, 100],
             ref="reference")
    r += _seq("E1", "W1", "pipedpeer", 2, [48, 50, 52, 49, 51])
    r += _seq("E1", "W1", "plain", 1, [100.5] * 5, ref="reference")
    r += _seq("E1", "W1", "plain", 2, [50] * 5)
    return r


MACHINES_HEADER = ("id,hostname,ip,os_id,os_version,kernel,cpu_model,sockets,"
                   "cores_per_socket,threads_per_core,nproc,ram_gib,nic,"
                   "nic_speed_mbps,python3,home_fs")

E4_CSV = """\
system,repeat,wall_s,result_ok,share_pc00,share_pc01
pipedpeer,1,10,1,0.5,0.5
pipedpeer,2,11,1,0.6,0.4
ray,1,20,1,1.0,1.0
ray,2,21,1,1.0,1.0
"""

E5_CSV = """\
system,phase,pc,ready_s,rx_bytes,reused
pipedpeer,cold,pc00,5,1000,1
pipedpeer,cold,pc01,9,3000,0
pip,cold,pc00,30,900000,0
pip,cold,pc01,40,1000000,0
ray,cold,pc00,4,900,0
ray,cold,pc01,6,800,0
pipedpeer,update,pc00,1,200,1
pipedpeer,update,pc01,2,300,1
"""

E6_CSV = """\
system,kind,i,kill_at_s,wall_s,result_ok,stop_latency_s
pipedpeer,kill,1,1.0,12,1,
pipedpeer,kill,2,1.0,14,1,
pipedpeer,kill,3,1.0,13,0,
pipedpeer,ctrl-c,1,,,10,2
pipedpeer,ctrl-c,2,,,11,3
ray,kill,1,1.0,30,1,
ray,kill,2,1.0,32,1,
ray,kill,3,1.0,31,1,
"""

E11_CSV = """\
k,pc,join_s,bytes_per_min,is_introducer
8,pc00,10,1000,1
8,pc01,12,500,0
16,pc00,15,1500,1
16,pc01,17,700,0
32,pc00,20,2000,1
32,pc01,22,900,0
"""

PREDICTIONS = """\
# predictions written before the runs
PRED ray W1 min_n=2 speedup=2.0
PRED plain W1 min_n=4 speedup=1.0
"""


def _write_main_lab(lab: Path):
    (lab / "runs.jsonl").write_text(
        "\n".join(__import__("json").dumps(r) for r in _main_rows()) + "\n")
    rows = [MACHINES_HEADER]
    for i, pc in enumerate(f"pc{i:02d}" for i in range(4)):
        rows.append(f"{pc},cluster{pc},10.0.0.{i + 10},fedora,41,6.8.0,"
                    f"Xeon,1,4,2,8,16,eth0,1000,3.12,/home/{pc}")
    (lab / "machines.csv").write_text("\n".join(rows) + "\n")
    (lab / "predictions.md").write_text(PREDICTIONS)
    inp = lab / "analysis-input"
    inp.mkdir()
    (inp / "e4.csv").write_text(E4_CSV)
    (inp / "e5.csv").write_text(E5_CSV)
    (inp / "e6.csv").write_text(E6_CSV)
    (inp / "e11.csv").write_text(E11_CSV)
    cpu = {"pc00": (45, 45, 50, 50), "pc01": (40, 40, 40, 40),
           "pc02": (35, 35, 35, 35), "pc03": (30, 30, 30, 30)}
    for pc, (u1, s1, u2, s2) in cpu.items():
        m = lab / "hosts" / pc / "metrics"
        m.mkdir(parents=True)
        (m / "E1-n32.vmstat").write_text(
            VMSTAT_TMPL.format(u1=u1, s1=s1, u2=u2, s2=s2))


def _write_tight_lab(lab: Path):
    (lab / "runs.jsonl").write_text(
        "\n".join(__import__("json").dumps(r) for r in _tight_rows()) + "\n")


def _run(lab: Path, *flags):
    return subprocess.run([sys.executable, str(ANALYSE), "--lab", str(lab),
                           *flags], check=False, capture_output=True, text=True)


@pytest.fixture(scope="session")
def labs(tmp_path_factory):
    main = tmp_path_factory.mktemp("lab")
    _write_main_lab(main)
    plain = _run(main)
    assert plain.returncode == 0, plain.stderr
    spread = _run(main, "--spread")
    assert spread.returncode == 0, spread.stderr
    tight = tmp_path_factory.mktemp("tight")
    _write_tight_lab(tight)
    tspread = _run(tight, "--spread")
    assert tspread.returncode == 0, tspread.stderr
    return {"main": main, "plain": plain.stdout, "spread": spread.stdout,
            "tight": tight, "tight_out": tspread.stdout}


def _read_csv(lab: Path, name: str):
    with (lab / "analysis" / name).open(newline="") as f:
        return list(csv.DictReader(f))


def _pick(rows, **kw):
    hits = [r for r in rows
            if all(r[k] == str(v) for k, v in kw.items())]
    assert len(hits) == 1, (kw, hits)
    return hits[0]


def test_speedup_perfect(labs):  # 5 reps N=1 [100]*5, N=2 [50]*5 -> 2.0, CI [2,2]
    r = _pick(_read_csv(labs["main"], "E1.csv"), workload="W2", system="dask", n=2)
    assert float(r["speedup"]) == pytest.approx(2.0)
    assert float(r["speedup_lo"]) == pytest.approx(2.0, abs=1e-6)
    assert float(r["speedup_hi"]) == pytest.approx(2.0, abs=1e-6)


def test_speedup_noisy(labs):  # N=2 [48..52] vs N=1 [100,99,101,100,100]
    r = _pick(_read_csv(labs["main"], "E1.csv"), workload="W1",
              system="pipedpeer", n=2)
    assert float(r["speedup"]) == pytest.approx(2.0, rel=1e-6)
    lo, hi = float(r["speedup_lo"]), float(r["speedup_hi"])
    assert lo < 2.0 < hi


def test_warmup_excluded(labs):  # warm-up r0 at 500 s
    r = _pick(_read_csv(labs["main"], "E1.csv"), workload="W1",
              system="pipedpeer", n=1)
    assert float(r["median_s"]) == pytest.approx(100.0)
    assert r["repeats"] == "5"
    assert r["ref"] == "reference"
    assert r["speedup"] == ""  # no speedup defined at n=1


def test_slow_repeat_warn(labs):  # one repeat 3x its group median
    assert "WARN slow-repeat E1-W1-ray-n2-r5 3.00× median" in labs["plain"]
    # opposite: 1.5x median (E1-W2 pipedpeer N=1) must not warn
    assert "WARN slow-repeat E1-W2-pipedpeer" not in labs["plain"]


def test_result_ok_false(labs):  # one repeat with result_ok false
    assert "WARN result_ok-false E1-W1-pipedpeer-n2-r5" in labs["plain"]
    r = _pick(_read_csv(labs["main"], "E1.csv"), workload="W1",
              system="pipedpeer", n=2)
    assert r["result_ok_all"] == "false"


def test_missing_warn(labs):  # ray missing at n=4 while others ran it
    assert "WARN missing E1 W1 ray n=4" in labs["plain"]


def test_ratio_pipedpeer_faster(labs):  # ratio CI pinned above 1.10
    r = _pick(_read_csv(labs["main"], "E1-ratios.csv"), workload="W1", n=2,
              system="plain")
    assert r["vs"] == "pipedpeer"
    assert float(r["ratio"]) == pytest.approx(1.2, rel=1e-6)
    assert r["verdict"] == "pipedpeer_faster"


def test_ratio_same(labs):  # ratio CI inside +-10%
    r = _pick(_read_csv(labs["main"], "E1-ratios.csv"), workload="W1", n=2,
              system="dask")
    assert r["verdict"] == "same_within_10pct"


def test_ratio_unresolved(labs):  # ratio CI crosses both lines
    r = _pick(_read_csv(labs["main"], "E1-ratios.csv"), workload="W2", n=2,
              system="plain")
    assert r["verdict"] == "unresolved"


def test_ratio_other_faster(labs):  # ratio CI pinned below 1/1.10
    r = _pick(_read_csv(labs["main"], "E1-ratios.csv"), workload="W3", n=2,
              system="plain")
    assert r["verdict"] == "other_faster"


def test_spread_raise(labs):  # max spread 0.2 -> k = min(15, ceil(5*4)) = 15
    assert "RAISE-REPEATS 15" in labs["spread"]
    assert "REPEATS-OK" not in labs["spread"]


def test_spread_ok(labs):  # all spreads < 0.10
    assert "REPEATS-OK" in labs["tight_out"]


def test_e1b_efficiency(labs):  # t(1)/t(N) = 100/98 with degenerate CI
    rows = _read_csv(labs["main"], "E1b-efficiency.csv")
    r = _pick(rows, system="pipedpeer", n=2)
    assert float(r["efficiency"]) == pytest.approx(100 / 98, rel=1e-6)
    assert float(r["efficiency_lo"]) == pytest.approx(100 / 98, rel=1e-6)
    assert float(r["efficiency_hi"]) == pytest.approx(100 / 98, rel=1e-6)


def test_e4_e5_e6_e11_tables(labs):
    pip = _pick(_read_csv(labs["main"], "E4.csv"), system="pipedpeer")
    assert float(pip["median_wall_s"]) == pytest.approx(10.5)
    assert pip["result_ok_all"] == "true"
    assert float(pip["share_pc00"]) == pytest.approx(0.55)
    cold = _pick(_read_csv(labs["main"], "E5.csv"), system="pipedpeer",
                 phase="cold")
    assert float(cold["ready_s_max"]) == pytest.approx(9.0)
    assert float(cold["ready_s_median"]) == pytest.approx(7.0)
    assert float(cold["rx_total"]) == pytest.approx(4000)
    assert cold["reused_all"] == "false"
    upd = _pick(_read_csv(labs["main"], "E5.csv"), system="pipedpeer",
                phase="update")
    assert upd["reused_all"] == "true"
    kill = _pick(_read_csv(labs["main"], "E6.csv"), system="pipedpeer",
                 kind="kill")
    assert kill["runs"] == "3"
    assert float(kill["wall_s_median"]) == pytest.approx(13.0)
    assert kill["result_ok_all"] == "false"
    cc = _pick(_read_csv(labs["main"], "E6.csv"), system="pipedpeer",
               kind="ctrl-c")
    assert float(cc["stop_latency_s_median"]) == pytest.approx(2.5)
    assert cc["kill_at_s_median"] == ""
    e11 = _pick(_read_csv(labs["main"], "E11.csv"), k=8)
    assert e11["pcs"] == "2"
    assert float(e11["join_s_median"]) == pytest.approx(11.0)
    assert float(e11["bytes_per_min_total"]) == pytest.approx(1500)
    assert e11["introducer_pc"] == "pc00"


def test_input_skip_lines(labs):  # tight lab has no analysis-input/
    out = labs["tight_out"]
    for exp in ("E4", "E5", "E6", "E11"):
        assert f"SKIP {exp}" in out


def test_figures(labs):  # PNG and PDF for every listed figure
    fig = labs["main"] / "figures"
    names = ("e1-speedup-w1", "e1-speedup-w2", "e1-speedup-w3",
             "e1b-efficiency", "e4-bars", "e5-bars", "e11-membership",
             "cpu-util-w1-n32")
    for name in names:
        assert (fig / f"{name}.png").exists(), name
        assert (fig / f"{name}.pdf").exists(), name


def test_predictions(labs):
    out = labs["plain"]
    # plain predicted min_n=4 but beats its own N=1 already at n=2
    assert "WARN prediction-missed plain W1 min_n=4 speedup=1.0" in out
    # ray's prediction (min_n=2, speedup 2.0) holds: no warning
    assert "WARN prediction-missed ray W1" not in out


def _lab_with(tmp_path_factory, rows):
    lab = tmp_path_factory.mktemp("d1")
    (lab / "runs.jsonl").write_text(
        "\n".join(__import__("json").dumps(r) for r in rows) + "\n")
    return lab


def test_missing_warn_smallest_n(tmp_path_factory):  # M3
    rows = _seq("E1", "W1", "pipedpeer", 1, [100] * 5, ref="reference")
    rows += _seq("E1", "W1", "pipedpeer", 2, [50] * 5)
    rows += _seq("E1", "W1", "pipedpeer", 4, [25] * 5)
    rows += _seq("E1", "W1", "ray", 4, [26] * 5)    # never ran n=1, n=2
    res = _run(_lab_with(tmp_path_factory, rows))
    assert res.returncode == 0, res.stderr
    assert "WARN missing E1 W1 ray n=1" in res.stdout
    assert "WARN missing E1 W1 ray n=2" in res.stdout
    assert "missing E1 W1 pipedpeer" not in res.stdout   # opposite: complete


def test_spread_boundary_never_repeats_same_k(tmp_path_factory):  # L3
    # IQR/median is exactly 0.10 with 5 repeats: asking for 5 again is no advice
    rows = _seq("E1", "W1", "pipedpeer", 1, [90, 95, 100, 105, 110],
                ref="reference")
    res = _run(_lab_with(tmp_path_factory, rows), "--spread")
    assert res.returncode == 0, res.stderr
    assert "RAISE-REPEATS 6" in res.stdout
    assert "RAISE-REPEATS 5" not in res.stdout


def test_e1b_efficiency_noisy_ci(tmp_path_factory):  # T2
    rows = _seq("E1b", "W1", "pipedpeer", 1, [100, 98, 102, 100, 101],
                size={"BENCH_ITEMS": 300}, ref="reference")
    rows += _seq("E1b", "W1", "pipedpeer", 2, [96, 100, 98, 99, 97],
                 size={"BENCH_ITEMS": 600})
    lab = _lab_with(tmp_path_factory, rows)
    res = _run(lab)
    assert res.returncode == 0, res.stderr
    r = _pick(_read_csv(lab, "E1b-efficiency.csv"), system="pipedpeer", n=2)
    pt, lo, hi = (float(r[k]) for k in
                  ("efficiency", "efficiency_lo", "efficiency_hi"))
    assert lo < pt < hi
    assert 0.9 < lo and hi < 1.1
