"""E16-E20 (lab/more-bench): the energy sampler and its joules, the idle
footprint probe, the hog shares, E19's generated scripts, E20's port
diffs, and analyse_more.py on synthetic records."""
import ast
import importlib.util
import json
import os
import subprocess
import sys
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CAMPAIGN = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(CAMPAIGN, "lib"))
sys.path.insert(0, CAMPAIGN)

import analyse_more  # noqa: E402
from campaign import energy  # noqa: E402

S = 1_000_000_000_000_000_000  # a sample time, ns


def experiment(name):
    spec = importlib.util.spec_from_file_location("exp_" + name, os.path.join(CAMPAIGN, "experiments", name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def energy_text(samples, max_uj=1000, zones=None):
    zones = zones or {"intel-rapl:0": {"name": "package-0", "max_uj": max_uj},
                      "intel-rapl:1": {"name": "psys", "max_uj": max_uj}}
    lines = [json.dumps({"ts_ns": S, "kind": "energy-start", "available": True, "source": "powercap",
                         "zones": zones})]
    for t, uj in samples:
        lines.append(json.dumps({"ts_ns": t, "kind": "energy", "uj": uj}))
    return "\n".join(lines) + "\n"


def test_joules_sum_package_zones_undo_the_wrap_and_interpolate():
    sec = 10**9
    # package-0 grows 100 J a second and wraps at 1000 J; psys is not summed.
    w, top = 100_000_000, 999_999_999
    st = energy.parse(energy_text([(S + i * sec, {"intel-rapl:0": (800_000_000 + w * i) % (top + 1),
                                                  "intel-rapl:1": 5 * i}) for i in range(6)], max_uj=top))
    assert st["available"] and energy.package_zones(st) == ["intel-rapl:0"]
    j, why = energy.joules(st, S + sec // 2, S + 4 * sec + sec // 2)
    assert why is None
    assert j == pytest.approx(400.0)  # 4 s at 100 W, across one wrap
    j, why = energy.joules(st, S - sec, S + sec)
    assert j is None and "cover" in why


def test_unavailable_header_gives_the_reason_and_no_joules():
    text = json.dumps({"ts_ns": S, "kind": "energy-start", "available": False,
                       "reason": "container: shared"}) + "\n"
    st = energy.parse(text)
    j, why = energy.joules(st, S, S + 10**9)
    assert (st["available"], j, why) == (False, None, "container: shared")
    f = energy.fields({"pc00": st}, ["pc00"], S, S + 10**9, ["pc00", "pc01"])
    assert f["energy_available"] is False and f["energy_j"] is None
    assert f["energy_unavailable"] == {"pc00": "container: shared", "pc01": "no energy samples"}


def test_energy_sh_samples_a_fake_powercap_tree(tmp_path):
    zone = tmp_path / "sys" / "powercap" / "intel-rapl:0"
    sub = tmp_path / "sys" / "powercap" / "intel-rapl:0:0"
    zone.mkdir(parents=True)
    sub.mkdir()
    (zone / "name").write_text("package-0\n")
    (zone / "max_energy_range_uj").write_text("262143328850\n")
    (zone / "energy_uj").write_text("1000\n")
    (sub / "name").write_text("core\n")
    (sub / "energy_uj").write_text("7\n")
    env = dict(os.environ, PIPEDPEER_LAB_HOME=str(tmp_path / "home"), PL_ENERGY_SYSFS=str(tmp_path / "sys"),
               PL_ENERGY_PERIOD_S="0.3")
    sh = os.path.join(CAMPAIGN, "otel", "energy.sh")
    out = subprocess.run(["sh", sh, "start"], env=env, capture_output=True, text=True, timeout=30).stdout
    try:
        assert out.startswith("PLENERGY started"), out
        time.sleep(0.7)
        (zone / "energy_uj").write_text("3000000\n")
        time.sleep(0.8)
        assert "running" in subprocess.run(["sh", sh, "status"], env=env, capture_output=True, text=True).stdout
    finally:
        subprocess.run(["sh", sh, "stop"], env=env, capture_output=True, timeout=30)
    st = energy.parse((tmp_path / "home" / "logs" / "energy.jsonl").read_text())
    assert st["zones"] == {"intel-rapl:0": {"name": "package-0", "max_uj": 262143328850}}  # no subzone
    assert len(st["samples"]) >= 3
    t0, t1 = st["samples"][0][0], st["samples"][-1][0]
    assert energy.joules(st, t0, t1) == (round((3000000 - 1000) / 1e6, 3), None)
    assert "stopped" in subprocess.run(["sh", sh, "status"], env=env, capture_output=True, text=True).stdout


def test_energy_sh_says_why_when_there_is_no_counter(tmp_path):
    (tmp_path / "sys" / "powercap").mkdir(parents=True)
    env = dict(os.environ, PIPEDPEER_LAB_HOME=str(tmp_path / "home"), PL_ENERGY_SYSFS=str(tmp_path / "sys"))
    out = subprocess.run(["sh", os.path.join(CAMPAIGN, "otel", "energy.sh"), "start"], env=env,
                         capture_output=True, text=True, timeout=30).stdout
    assert out.startswith("PLENERGY unavailable no RAPL zone"), out
    st = energy.parse((tmp_path / "home" / "logs" / "energy.jsonl").read_text())
    assert st["available"] is False and "no RAPL zone" in st["reason"]


def test_footprint_probe_counts_a_marked_process_and_its_children():
    import signal
    child = subprocess.Popen(["sh", "-c", "sleep 30 & sleep 30; wait"], start_new_session=True,
                             env=dict(os.environ, PL_CAMPAIGN_DASK="1"))
    try:
        time.sleep(0.3)
        out = subprocess.run([sys.executable, os.path.join(CAMPAIGN, "experiments", "scripts", "footprint_probe.py"),
                              "dask", "1"], capture_output=True, text=True, timeout=30).stdout
    finally:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
    fp = json.loads(next(ln for ln in out.splitlines() if ln.startswith("PLFP "))[5:])
    # The sh and both sleeps (a container lab on this host may add its own).
    assert fp["procs"] >= 3 and fp["names"].get("sleep", 0) >= 2 and fp["names"].get("sh", 0) >= 1
    assert fp["rss_mb_avg"] > 0 and fp["samples"] >= 2


def test_hog_share_against_the_fair_share(tmp_path):
    e18 = experiment("e18")
    chunks = {"chunks": [{"pc": "pc01", "t0": 0, "t1": 1, "items": 10}, {"pc": "pc02", "t0": 0, "t1": 1, "items": 30},
                         {"pc": "pc03", "t0": 0, "t1": 1, "items": 60}]}
    (tmp_path / "chunks.json").write_text(json.dumps(chunks))
    per, hog, fair = e18.shares("chunks.json", str(tmp_path), {"pc01"}, ["pc01", "pc02", "pc03", "pc04"])
    assert per == {"pc01": 10, "pc02": 30, "pc03": 60} and (hog, fair) == (0.1, 0.25)


def test_e19_scripts_parse_and_change_only_what_each_phase_says():
    e19 = experiment("e19")
    src = {(p, ray): e19.source(p, off, rich, ray) for p, off, rich in e19.PHASES for ray in (False, True)}
    for s in src.values():
        ast.parse(s)
    top = lambda s: {a.name for n in ast.parse(s).body if isinstance(n, (ast.Import, ast.ImportFrom))  # noqa: E731
                     for a in n.names} | {n.module for n in ast.parse(s).body if isinstance(n, ast.ImportFrom)}
    assert "networkx" in top(src[("cold", False)]) and "rich.text" not in top(src[("edit", False)])
    assert "rich.text" in top(src[("newdep", False)])
    assert src[("cold", False)] != src[("edit", False)].replace("phase edit", "phase cold")  # the edit
    assert src[("cold", False)].replace("cold", "warm") == src[("warm", False)]
    assert "SEED_OFFSET = 1" in src[("edit", False)] and "seed=i + SEED_OFFSET" in src[("edit", False)]
    assert "'pip': ['networkx', 'rich']" in src[("newdep", True)].replace('"', "'")
    assert "import networkx" not in "\n".join(ln for ln in src[("cold", True)].splitlines() if not ln.startswith(" "))


def test_e19_restages_its_workspace_for_every_phase_and_nothing_else():
    e19 = experiment("e19")
    from campaign import runner
    staged = {("pc00", runner.REMOTE_HOME + "/work/E19-ray"), ("pc00", runner.REMOTE_HOME + "/work/E19-pipedpeer"),
              ("pc00", runner.REMOTE_HOME + "/work/W1-ray")}
    assert e19.forget_stage(staged, "E19", "ray") == staged - {("pc00", runner.REMOTE_HOME + "/work/E19-ray")}


def test_e20_ports_change_a_few_lines_and_the_originals_none():
    e20 = experiment("e20")
    from campaign.workloads import Workload
    for d in e20.WORKLOADS:
        wl = Workload(d)
        assert e20.lines_changed(wl, "plain") == 0 and e20.lines_changed(wl, "pipedpeer") == 0
        for s in ("ray", "dask"):
            assert 2 <= e20.lines_changed(wl, s) <= 9, (wl.id, s)


def test_analyse_more_puts_joules_on_archived_runs_and_summarises(tmp_path):
    sec = 10**9
    os.makedirs(tmp_path / "energy")
    os.makedirs(tmp_path / "csv")
    for pc in ("pc00", "pc01"):
        (tmp_path / "energy" / ("%s.jsonl" % pc)).write_text(
            energy_text([(S + i * sec, {"intel-rapl:0": 100 + 50_000_000 * i}) for i in range(40)]))
    runs = [
        {"run_id": "i", "experiment": "e16", "phase": "idle-power", "power_w": {"pc00": 20.0, "pc01": 20.0},
         "start_ns": S, "end_ns": S + 10 * sec, "hosts": ["pc00", "pc01"]},
        {"run_id": "a", "experiment": "e1", "workload": "W1", "system": "pipedpeer", "n": 2, "hosts": ["pc00", "pc01"],
         "start_ns": S + 2 * sec, "end_ns": S + 12 * sec, "wall_s": 10.0, "result_ok": True},
        {"run_id": "b", "experiment": "e18", "system": "dask", "n": 2, "fault": "none", "wall_s": 10.0,
         "result_ok": True, "hog_share": 0.5, "fair_share": 0.5},
        {"run_id": "c", "experiment": "e18", "system": "dask", "n": 2, "fault": "hog", "wall_s": 12.0,
         "result_ok": True, "hog_share": 0.3, "fair_share": 0.5},
        {"run_id": "d", "experiment": "e19", "system": "ray", "phase": "cold", "ttfr_s": 40.0, "result_ok": True},
        {"run_id": "e", "experiment": "e20", "workload": "W6", "system": "plain", "n": 1, "wall_s": 30.0,
         "result_ok": True, "script_lines_changed": 0},
        {"run_id": "f", "experiment": "e20", "workload": "W6", "system": "ray", "n": 2, "wall_s": 10.0,
         "result_ok": True, "script_lines_changed": 6},
    ]
    wrote = analyse_more.run(runs, str(tmp_path), str(tmp_path / "csv"))
    assert {"e16_energy.csv", "e16_summary.json", "e18_summary.json", "e19_summary.json",
            "e20_summary.json"} <= set(wrote)
    s16 = json.loads((tmp_path / "e16_summary.json").read_text())
    g = next(x for x in s16["groups"] if x["experiment"] == "e1")
    assert g["median_energy_j"] == pytest.approx(1000.0)  # 2 PCs x 50 W x 10 s
    assert g["median_energy_above_idle_j"] == pytest.approx(600.0)  # minus 2 x 20 W x 10 s
    s18 = json.loads((tmp_path / "e18_summary.json").read_text())
    assert s18["dask"]["slowdown"] == 1.2 and s18["dask"]["hog_share_hog"] == 0.3
    assert json.loads((tmp_path / "e19_summary.json").read_text()) == {"ray": {"cold": 40.0}}
    s20 = {x["system"]: x for x in json.loads((tmp_path / "e20_summary.json").read_text())}
    assert s20["ray"]["speedup_vs_plain"] == 3.0 and s20["ray"]["script_lines_changed"] == 6
