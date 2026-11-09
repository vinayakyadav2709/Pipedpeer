"""Case table for AN1: analysis fixes and sweep tables.

Cases:
1. a W1 group, one result_ok=false run: wall of that run excluded, count of 1 dropped
2. a group, exit_code=124: excluded
3. result_ok=null, exit 0: kept
4. 2 of 5 runs left: group marked invalid
5. E13 with 4 task sizes, 4 systems: 16 rows
6. noop missing one system: that system absent, one warning
7. lat line parsed: p50/p90/p99 as floats
8. e17 row job_result_ok=0: counted as failure in the table's count
9. e19 baseline row: empty join columns; not averaged in
10. e18.csv absent: one log line, no table, no error
11. e20 with both arms: overhead sign correct (bare faster -> positive overhead)
"""
import json
import sys
from pathlib import Path
import pytest

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

from analyse import EXP_COLS, Group, check_groups, exp_speed_rows, load_runs, table_e11
import analyse_sweeps


def test_w1_group_result_ok_false_excluded(tmp_path):
    # Case 1: a W1 group, one result_ok=false run -> wall of that run excluded, count of 1 dropped
    runs_file = tmp_path / "runs.jsonl"
    runs = [
        {"run_id": f"E1-W1-pipedpeer-n2-r{i}", "experiment": "E1", "workload": "W1",
         "system": "pipedpeer", "n": 2, "repeat": i, "warmup": False,
         "wall_s": 50.0 + i, "exit_code": 0, "result_ok": (i != 5), "ref": "majority"}
        for i in range(1, 6)
    ]
    runs_file.write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    groups = load_runs(runs_file)
    g = groups["E1"]["W1"]["pipedpeer"][2]
    assert len(g.walls) == 4
    assert 55.0 not in g.walls  # r5 had wall 55.0, excluded
    assert g.dropped == 1


def test_group_exit_code_124_excluded(tmp_path):
    # Case 2: a group, exit_code=124 -> excluded
    runs_file = tmp_path / "runs.jsonl"
    runs = [
        {"run_id": "E1-W1-dask-n2-r1", "experiment": "E1", "workload": "W1",
         "system": "dask", "n": 2, "repeat": 1, "warmup": False,
         "wall_s": 12.0, "exit_code": 124, "result_ok": True, "ref": "majority"},
        {"run_id": "E1-W1-dask-n2-r2", "experiment": "E1", "workload": "W1",
         "system": "dask", "n": 2, "repeat": 2, "warmup": False,
         "wall_s": 10.0, "exit_code": 0, "result_ok": True, "ref": "majority"},
    ]
    runs_file.write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    groups = load_runs(runs_file)
    g = groups["E1"]["W1"]["dask"][2]
    assert g.walls == [10.0]
    assert g.dropped == 1


def test_group_result_ok_null_exit_0_kept(tmp_path):
    # Case 3: result_ok=null, exit 0 -> kept
    runs_file = tmp_path / "runs.jsonl"
    runs = [
        {"run_id": "E7-idle-pipedpeer-n1-r1", "experiment": "E7", "workload": "idle",
         "system": "pipedpeer", "n": 1, "repeat": 1, "warmup": False,
         "wall_s": 3.5, "exit_code": 0, "result_ok": None, "ref": None},
    ]
    runs_file.write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    groups = load_runs(runs_file)
    g = groups["E7"]["idle"]["pipedpeer"][1]
    assert g.walls == [3.5]
    assert g.dropped == 0


def test_group_under_planned_repeats_invalid(tmp_path):
    # Case 4: 2 of 5 runs left -> group marked invalid
    g = Group()
    g.planned = 5
    # 2 successful runs added
    g.add("r1", 10.0, True, "ref", {}, exit_code=0)
    g.add("r2", 10.5, True, "ref", {}, exit_code=0)
    # 3 failed runs recorded
    g.add("r3", 11.0, False, "ref", {}, exit_code=0)
    g.add("r4", 12.0, True, "ref", {}, exit_code=1)
    g.add("r5", 13.0, False, "ref", {}, exit_code=124)
    assert len(g.walls) == 2
    assert g.dropped == 3
    assert g.invalid is True
    assert g.status == "invalid"


def test_e13_table_16_rows(tmp_path):
    # Case 5: E13 with 4 task sizes, 4 systems -> 16 rows
    lab = tmp_path / "lab"
    lab.mkdir()
    runs = []
    task_map = {"TS1": 1, "TS10": 10, "TS100": 100, "TS1000": 1000}
    systems = ["pipedpeer", "ray", "dask", "ssh"]
    for w, ms in task_map.items():
        for s in systems:
            for rep in range(1, 6):
                runs.append({
                    "run_id": f"E13-{w}-{s}-n8-r{rep}", "experiment": "E13", "workload": w,
                    "system": s, "n": 8, "repeat": rep, "warmup": False,
                    "size": {"items": 1000, "task_ms": ms, "BENCH_ITEMS": 1000, "BENCH_TASK_MS": ms},
                    "wall_s": 15.0 + ms * 0.01, "exit_code": 0, "result_ok": True,
                    "receipt": {"remote_items": 700 if s == "pipedpeer" else None},
                })
    (lab / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    cols, rows = analyse_sweeps.table_e13(lab)
    assert len(rows) == 16
    assert "task_ms" in cols
    assert "system" in cols
    assert "median_wall_s" in cols
    assert "iqr_s" in cols
    assert "ratio_to_1core_ideal" in cols


def test_e15_noop_missing_system_warning(tmp_path, capsys):
    # Case 6: noop missing one system -> that system absent, one warning
    lab = tmp_path / "lab"
    lab.mkdir()
    runs = []
    # Only pipedpeer and ray; dask is missing
    for s in ["pipedpeer", "ray"]:
        runs.append({
            "run_id": f"E15-noop-{s}-n8-r1", "experiment": "E15", "workload": "noop",
            "system": s, "n": 8, "repeat": 1, "warmup": False,
            "wall_s": 2.5, "exit_code": 0, "result_ok": True,
            "result_lines": ["NOOP-OK items=50000 seconds=2.500 tasks_per_s=20000.0"],
        })
    (lab / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    cols, rows = analyse_sweeps.table_e15(lab)
    systems_in_table = {r["system"] for r in rows}
    assert any("pipedpeer" in s for s in systems_in_table)
    assert any("ray" in s for s in systems_in_table)
    assert not any("dask" in s for s in systems_in_table)
    out = capsys.readouterr().out
    assert "WARN" in out and "dask" in out


def test_e16_lat_line_parsed_floats():
    # Case 7: lat line parsed -> p50/p90/p99 as floats
    line = "LAT-OK n=1000 p50_ms=0.450 p90_ms=0.620 p99_ms=1.150"
    parsed = analyse_sweeps.parse_lat_line(line)
    assert isinstance(parsed["p50_ms"], float)
    assert isinstance(parsed["p90_ms"], float)
    assert isinstance(parsed["p99_ms"], float)
    assert parsed["p50_ms"] == pytest.approx(0.450)
    assert parsed["p90_ms"] == pytest.approx(0.620)
    assert parsed["p99_ms"] == pytest.approx(1.150)


def test_e17_row_job_result_ok_0_counted_failure(tmp_path):
    # Case 8: e17 row job_result_ok=0 -> counted as failure in the table's count
    csv_text = """i,kill_at_s,job_wall_s,job_result_ok,new_join_s,new_join_ok,new_remote_items,old_invite_joined
1,5.0,15.2,1,12.0,1,64,0
2,5.0,14.8,0,13.0,1,64,0
3,5.0,15.0,1,11.5,1,64,0
"""
    inp = tmp_path / "e17.csv"
    inp.write_text(csv_text)
    cols, rows = analyse_sweeps.table_e17_from_csv(inp)
    assert len(rows) == 1
    r = rows[0]
    assert r["runs"] == 3
    assert r["failures"] == 1  # 1 failure counted


def test_e19_baseline_row_not_averaged_in_join(tmp_path):
    # Case 9: e19 baseline row -> empty join columns; not averaged in
    csv_text = """system,kind,i,pc,join_s,first_work_s,wall_s,result_ok
pipedpeer,baseline,1,,,20.0,1
pipedpeer,join,1,pc01,10.0,12.0,22.0,1
pipedpeer,join,2,pc01,12.0,14.0,24.0,1
"""
    inp = tmp_path / "e19.csv"
    inp.write_text(csv_text)
    cols, rows = analyse_sweeps.table_e19_from_csv(inp)
    r = [row for row in rows if row["system"] == "pipedpeer"][0]
    # join_s_median should be median of [10.0, 12.0] = 11.0, baseline empty column not averaged in
    assert float(r["join_s_median"]) == pytest.approx(11.0)
    assert float(r["first_work_s_median"]) == pytest.approx(13.0)


def test_e18_csv_absent_logged_skipped(tmp_path, capsys):
    # Case 10: e18.csv absent -> one log line, no table, no error
    lab = tmp_path / "lab"
    lab.mkdir()
    res = analyse_sweeps.build_e18_table(lab)
    assert res is None
    out = capsys.readouterr().out
    assert "SKIP E18" in out or "e18.csv" in out


def test_e20_overhead_sign_bare_faster(tmp_path):
    # Case 11: e20 with both arms -> overhead sign correct (bare faster -> positive overhead)
    lab = tmp_path / "lab"
    lab.mkdir()
    runs = []
    # pipedpeer (with sandbox) slower: 11.0s; pipedpeer-bare faster: 10.0s
    for rep in range(1, 6):
        runs.append({
            "run_id": f"E20-SB-pipedpeer-n1-r{rep}", "experiment": "E20", "workload": "SB",
            "system": "pipedpeer", "n": 1, "repeat": rep, "warmup": False,
            "wall_s": 11.0, "exit_code": 0, "result_ok": True,
        })
        runs.append({
            "run_id": f"E20-SB-pipedpeer-bare-n1-r{rep}", "experiment": "E20", "workload": "SB",
            "system": "pipedpeer-bare", "n": 1, "repeat": rep, "warmup": False,
            "wall_s": 10.0, "exit_code": 0, "result_ok": True,
        })
    (lab / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    cols, rows = analyse_sweeps.table_e20(lab)
    r = rows[0]
    assert float(r["overhead_pct"]) > 0  # bare faster -> positive overhead (10%)
    assert float(r["overhead_pct"]) == pytest.approx(10.0, rel=1e-2)


# --- review rows: each finding with its opposite ---
def _write(tmp_path, runs):
    p = tmp_path / "runs.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in runs) + "\n")
    return p


def _e1(i, wall, ok=True, exit_code=0, n=2):
    return {"run_id": f"E1-W1-pipedpeer-n{n}-r{i}", "experiment": "E1", "workload": "W1",
            "system": "pipedpeer", "n": n, "repeat": i, "warmup": False, "wall_s": wall,
            "exit_code": exit_code, "result_ok": ok, "ref": "majority"}


def test_slow_repeat_names_the_slow_run_after_a_drop(tmp_path):
    runs = [_e1(1, 10.0, ok=False)] + [_e1(i, 10.0) for i in (2, 3, 4)] + [_e1(5, 40.0)]
    groups = load_runs(_write(tmp_path, runs))
    warns = []
    check_groups(groups, "E1", warns)
    slow = [w for w in warns if w.startswith("slow-repeat")]
    assert len(slow) == 1 and "-r5 " in slow[0]


def test_slow_repeat_named_with_nothing_dropped(tmp_path):
    runs = [_e1(i, 10.0) for i in (1, 2, 3, 4)] + [_e1(5, 40.0)]
    warns = []
    check_groups(load_runs(_write(tmp_path, runs)), "E1", warns)
    assert any(w.startswith("slow-repeat") and "-r5 " in w for w in warns)


def test_null_result_ok_is_not_warned_as_false(tmp_path):
    runs = [_e1(i, 10.0, ok=None) for i in range(1, 6)]
    warns = []
    check_groups(load_runs(_write(tmp_path, runs)), "E1", warns)
    assert not any(w.startswith("result_ok-false") for w in warns)


def test_false_result_ok_is_warned(tmp_path):
    runs = [_e1(i, 10.0, ok=(i != 3)) for i in range(1, 6)]
    warns = []
    check_groups(load_runs(_write(tmp_path, runs)), "E1", warns)
    assert any(w.startswith("result_ok-false") and "-r3" in w for w in warns)


def test_every_run_failed_gives_an_invalid_row_and_no_crash(tmp_path):
    runs = [_e1(i, 10.0, n=1) for i in range(1, 6)]
    runs += [_e1(i, 10.0, ok=False, n=2) for i in range(1, 6)]
    groups = load_runs(_write(tmp_path, runs))
    warns = []
    check_groups(groups, "E1", warns)
    assert any("invalid" in w and "0 of 5" in w for w in warns)
    row = [r for r in exp_speed_rows(groups, "E1") if r["n"] == 2][0]
    assert row["median_s"] is None and row["status"] == "invalid" and row["dropped"] == 5
    assert "speedup" not in row


def test_all_runs_good_row_is_valid(tmp_path):
    groups = load_runs(_write(tmp_path, [_e1(i, 10.0) for i in range(1, 6)]))
    row = exp_speed_rows(groups, "E1")[0]
    assert row["status"] == "valid" and row["dropped"] == 0
    assert "status" in EXP_COLS and "dropped" in EXP_COLS


def test_timed_out_run_with_no_wall_counts_as_dropped(tmp_path):
    runs = [_e1(i, 10.0) for i in range(1, 5)] + [_e1(5, None, ok=None, exit_code=124)]
    g = load_runs(_write(tmp_path, runs))["E1"]["W1"]["pipedpeer"][2]
    assert g.dropped == 1 and len(g.walls) == 4 and g.invalid is True


def test_one_short_of_planned_is_invalid():
    g = Group(planned=5)
    for i in range(4):
        g.add(f"r{i}", 10.0, True, "x", {})
    assert g.invalid is True


def test_full_planned_is_valid():
    g = Group(planned=5)
    for i in range(5):
        g.add(f"r{i}", 10.0, True, "x", {})
    assert g.invalid is False


def _noop(i, ok=True, lines=None):
    return {"run_id": f"E15-noop-ray-n8-r{i}", "experiment": "E15", "workload": "noop",
            "system": "ray", "n": 8, "repeat": i, "warmup": False, "wall_s": 5.0,
            "exit_code": 0, "result_ok": ok, "size": {"items": 1000},
            "result_lines": ["NOOP-OK items=1000 seconds=4 tasks_per_s=250"] if lines is None else lines}


def test_planned_repeats_come_from_the_matrix(tmp_path):
    g = load_runs(_write(tmp_path, [_noop(i) for i in (1, 2, 3)]))["E15"]["noop"]["ray"][8]
    assert g.planned == 3 and g.invalid is False


def test_two_of_three_planned_is_invalid(tmp_path):
    g = load_runs(_write(tmp_path, [_noop(i) for i in (1, 2)]))["E15"]["noop"]["ray"][8]
    assert g.planned == 3 and g.invalid is True


def _ts(system, receipt=None, exit_code=0):
    r = {"run_id": f"E13-TS10-{system}-n8-r1", "experiment": "E13", "workload": "TS10",
         "system": system, "n": 8, "repeat": 1, "warmup": False, "wall_s": 2.0,
         "exit_code": exit_code, "result_ok": exit_code == 0,
         "size": {"items": 1000, "task_ms": 10, "payload_bytes": 10000}}
    if receipt is not None:
        r["receipt"] = receipt
    return r


def test_no_receipt_leaves_remote_share_empty():
    _, rows = analyse_sweeps.table_e13([_ts("dask")])
    assert rows[0]["remote_items_ratio"] is None


def test_receipt_gives_remote_share():
    _, rows = analyse_sweeps.table_e13([_ts("pipedpeer", {"remote_items": 750})])
    assert rows[0]["remote_items_ratio"] == pytest.approx(0.75)


def test_sweep_row_counts_drops():
    _, rows = analyse_sweeps.table_e13([_ts("ray"), _ts("ray", exit_code=124)])
    assert rows[0]["runs"] == 1 and rows[0]["dropped"] == 1


def test_sweep_row_no_drops():
    _, rows = analyse_sweeps.table_e13([_ts("ray")])
    assert rows[0]["dropped"] == 0


def test_noop_without_a_rate_line_is_not_invented(capsys):
    _, rows = analyse_sweeps.table_e15([_noop(1, lines=[])])
    assert rows == []
    assert "E15-noop-ray-n8-r1" in capsys.readouterr().out


def test_noop_rate_line_is_read():
    _, rows = analyse_sweeps.table_e15([_noop(1)])
    assert rows[0]["median_tasks_per_s"] == pytest.approx(250.0)


def _e19_csv(tmp_path, extra=""):
    p = tmp_path / "e19.csv"
    p.write_text("system,kind,i,pc,join_s,first_work_s,wall_s,result_ok\n"
                 "ray,baseline,1,,,,100,1\n"
                 "ray,join,1,pc3,5,9,60,1\n"
                 "ray,join,2,pc3,5,9,62,1\n" + extra)
    return p


def test_e19_baseline_and_join_walls_are_separate(tmp_path):
    _, rows = analyse_sweeps.table_e19_from_csv(_e19_csv(tmp_path))
    assert rows[0]["wall_baseline_s_median"] == pytest.approx(100.0)
    assert rows[0]["wall_join_s_median"] == pytest.approx(61.0)
    assert rows[0]["result_ok_all"] is True


def test_e19_failed_run_leaves_the_wall_median(tmp_path):
    _, rows = analyse_sweeps.table_e19_from_csv(_e19_csv(tmp_path, "ray,join,3,pc3,5,9,900,0\n"))
    assert rows[0]["wall_join_s_median"] == pytest.approx(61.0)
    assert rows[0]["result_ok_all"] is False


def test_e20_ignores_the_reference_runs_of_another_experiment():
    base = {"workload": "SB", "n": 1, "warmup": False, "exit_code": 0, "result_ok": True}
    runs = [dict(base, experiment="E20", system="pipedpeer", wall_s=11.0),
            dict(base, experiment="E20", system="pipedpeer-bare", wall_s=10.0),
            dict(base, experiment="E99", system="pipedpeer", wall_s=99.0)]
    _, rows = analyse_sweeps.table_e20(runs)
    assert rows[0]["runs_with"] == 1 and rows[0]["overhead_pct"] == pytest.approx(10.0)


def test_unreadable_runs_line_is_logged(tmp_path, capsys):
    p = tmp_path / "runs.jsonl"
    p.write_text('{"workload": "noop"}\n{broken\n')
    assert len(analyse_sweeps._load_run_records(p)) == 1
    assert "runs.jsonl" in capsys.readouterr().out


def _e11_row(joined):
    return {"k": "16", "pc": "pc00", "join_s": "4", "bytes_per_min": "600",
            "is_introducer": "1", "joined": joined}


def test_e11_table_shows_a_partial_join():
    _, rows = table_e11([_e11_row("15/16")])
    assert rows[0]["joined"] == "15/16"


def test_e11_table_shows_a_full_join():
    _, rows = table_e11([_e11_row("16/16")])
    assert rows[0]["joined"] == "16/16"
