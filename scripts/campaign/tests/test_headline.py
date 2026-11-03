"""The headline experiments' data (lib/campaign/timeline.py) and figures
(analyse_headline.py), on synthetic records."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
sys.path.insert(0, os.path.join(HERE, ".."))

import analyse  # noqa: E402
import analyse_headline as ah  # noqa: E402
from campaign import timeline  # noqa: E402

S = 1_000_000_000_000  # a job's start_ns


def test_bins_spread_a_chunk_over_the_seconds_it_ran():
    chunks = [{"pc": "pc01", "t0": 0.5, "t1": 2.5, "items": 20}, {"pc": "pc02", "t0": 1.2, "t1": 1.2, "items": 3}]
    per = timeline.bin_chunks(chunks, wall_s=3)
    assert per["pc01"] == [5.0, 10.0, 5.0]
    assert per["pc02"] == [0.0, 3.0, 0.0]
    assert sum(per["pc01"]) == 20


def test_pipedpeer_chunks_use_step_items_or_estimate_from_busy_time():
    steps = [{"step": "compute", "machine": "nodeA", "start_ns": S + 10**9, "dur_ns": 2 * 10**9, "items": 30},
             {"step": "compute", "machine": "nodeB", "start_ns": S, "dur_ns": 10**9, "items": 10},
             {"step": "queue", "machine": "nodeB", "start_ns": S, "dur_ns": 10**9}]
    nodes = {"nodeA": "pc00", "nodeB": "pc01"}
    chunks, src = timeline.pipedpeer_chunks({"steps": steps}, nodes, S, 40)
    assert src == "receipt-steps"
    assert [(c["pc"], c["t0"], c["t1"], c["items"]) for c in chunks] == [("pc00", 1.0, 3.0, 30), ("pc01", 0.0, 1.0, 10)]
    for s in steps:
        s.pop("items", None)
    chunks, src = timeline.pipedpeer_chunks({"steps": steps}, nodes, S, 60)
    assert src == "receipt-steps-estimated"
    assert [round(c["items"]) for c in chunks] == [40, 20]  # 60 items over 3 busy seconds
    chunks, src = timeline.pipedpeer_chunks({"local_items": 60, "dispatched_items": 0}, nodes, S, 60, "pc00", 9.0)
    assert (src, chunks) == ("receipt-local-only", [{"pc": "pc00", "t0": 0.0, "t1": 9.0, "items": 60}])


def test_ray_batches_follow_pool_chunksize_and_dask_tasks_map_to_pcs():
    assert timeline.ray_batch(8000, 2) == 1000 and timeline.ray_batch(1001, 2) == 126
    ips = {"10.0.0.11": "pc01"}
    chunks, _ = timeline.ray_chunks({"tasks": [[S / 1e6 + 500, S / 1e6 + 1500, "10.0.0.11", "t"]]}, ips, S, 8, 1)
    assert chunks == [{"pc": "pc01", "t0": 0.5, "t1": 1.5, "items": 2}]
    chunks, _ = timeline.dask_chunks({"done": {"%d tcp://10.0.0.11:4000" % (S // 10**9 + 2): 7}}, ips, S)
    assert chunks == [{"pc": "pc01", "t0": 2.0, "t1": 2.0, "items": 7}]


def test_rerun_counts_per_system():
    class B:
        def __init__(self, kind):
            self.kind = kind
    r = {"receipt": {"mapped_items": 10, "steps": [{"step": "compute", "items": 8}, {"step": "compute", "items": 5}]}}
    assert timeline.rerun_items(B("pipedpeer"), r, 10) == (3, "receipt-steps")
    old = {"receipt": {"mapped_items": 10, "local_items": 10, "remote_items": 2, "steps": [],
                       "parts": [{"ok": False, "error": "timeout", "items": 4}]}}
    assert timeline.rerun_items(B("pipedpeer"), old, 10) == (6, "receipt-counters")
    assert timeline.rerun_items(B("dask"), {}, 10, dask_dump={"lost": 2, "erred": 0}, counted=15) == (7, "dask-scheduler-plugin")
    assert timeline.rerun_items(B("ray"), {}, 10, counted=12) == (2, "ray-timeline-batches")


def run_line(**kw):
    base = {"run_id": "x", "experiment": "e12", "workload": "W1", "system": "pipedpeer", "n": 2, "warmup": False,
            "error": None, "result_ok": True, "start_ns": S, "wall_s": 4.0, "hosts": ["pc00", "pc01"]}
    base.update(kw)
    return base


def test_headline_csvs_and_figures(tmp_path):
    tl = {"bin_s": 1, "per_pc": {"pc00": [5, 5, 5, 5], "pc01": [5, 5, 5, 5], "pc02": [0, 0, 4, 5]}}
    runs = [
        run_line(timeline=tl, join_every_s=2, events=[{"kind": "join", "t_ns": S + 2 * 10**9, "pcs": ["pc02"],
                                                       "seen_ns": S + 3 * 10**9}]),
        run_line(experiment="e13", run_id="c", n=16, size={"BENCH_ITEMS": "9"}, wall_s=80.0, timeline=tl,
                 kills=1, items_rerun=7, churn_every_s=20, churn_down_s=10,
                 events=[{"kind": "kill", "t_ns": S + 10**9, "pc": "pc01"},
                         {"kind": "restart", "t_ns": S + 2 * 10**9, "pc": "pc01"}]),
        run_line(experiment="e1b", run_id="b1", n=16, size={"BENCH_ITEMS": "9"}, wall_s=60.0, mode="weak"),
        run_line(experiment="e1b", run_id="b2", n=16, size={"BENCH_ITEMS": "9"}, wall_s=62.0, mode="weak"),
    ]
    for system, base in (("pipedpeer", 10.0), ("dask", 12.0)):
        for rtt, f in ((0, 1.0), (20, 1.1), (80, 1.5)):
            runs.append(run_line(experiment="e15", run_id="n", system=system, n=4, rtt_added_ms=rtt, wall_s=base * f))
    events = [{"pc": "pc00", "event": "head_start", "t_ns": S, "clock": "pc", "machine_id": "m0"},
              {"pc": "pc00", "event": "pc_map", "t_ns": 0, "clock": "pc", "machine_id": "m0"},
              {"pc": "pc01", "event": "pc_map", "t_ns": 0, "clock": "pc", "machine_id": "m1"},
              {"pc": "pc03", "event": "ssh_ok", "t_ns": S + 30 * 10**9, "clock": "pc00", "machine_id": "m1"},
              {"pc": "pc01", "event": "labpc", "t_ns": S - 60 * 10**9, "clock": "pc", "machine_id": "m1"},
              {"pc": "pc01", "event": "install_end", "t_ns": S + 300 * 10**9, "clock": "pc", "machine_id": ""},
              {"pc": "pc00", "event": "cluster_ready", "t_ns": S + 400 * 10**9, "clock": "pc00", "machine_id": ""},
              {"pc": "pc00", "event": "first_job_start", "t_ns": S + 420 * 10**9, "clock": "pc00", "machine_id": ""},
              {"pc": "pc00", "event": "first_work", "t_ns": S + 421 * 10**9, "clock": "pc00", "machine_id": ""},
              {"pc": "pc01", "event": "first_work", "t_ns": S + 425 * 10**9, "clock": "pc00", "machine_id": "m1"}]
    (tmp_path / "timeline.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    csvdir, figdir = tmp_path / "csv", tmp_path / "fig"
    csvdir.mkdir()
    figdir.mkdir()
    made, skipped = ah.run(runs, str(tmp_path), str(csvdir), str(figdir), analyse.plt_module(), analyse.save)
    assert sorted(made) == ["e12_join", "e13_churn", "e14_staircase", "e15_network"] and not skipped
    e13 = ah.e13_rows(runs, [runs[1]])[0]
    assert (e13["no_churn_wall_s"], e13["slowdown"]) == (61.0, round(80 / 61, 3))
    rows, summary = ah.e14_table(events)
    pc01 = next(r for r in rows if r["pc"] == "pc01")
    assert (pc01["labpc_s"], pc01["ssh_ok_s"], pc01["first_work_s"]) == (-60.0, 30.0, 425.0)
    assert summary["head_to_first_job_start_s"] == 420.0 and summary["head_to_all_pcs_working_s"] == 425.0
    e15 = {(r["system"], r["rtt_added_ms"]): r["slowdown_vs_0ms"] for r in ah.e15_rows(runs[4:])}
    assert e15[("dask", 80)] == 1.5
    assert json.load(open(tmp_path / "e14_summary.json"))["pcs_with_first_work"] == 2


def test_timeline_reads_the_steps_from_the_receipt_file(tmp_path):
    """runs.jsonl's inline receipt is a summary without steps; the timeline
    and the rerun count read the whole receipt the backend kept."""
    import gzip
    import json
    from campaign import receipts
    whole = {"mapped_items": 10, "steps": [{"step": "compute", "items": 8}, {"step": "compute", "items": 5}]}
    path = tmp_path / "receipt.json.gz"
    with gzip.open(path, "wt") as f:
        json.dump(whole, f)
    res = {"receipt": receipts.summary(whole), "receipt_path": str(path)}

    class B:
        kind = "pipedpeer"
    assert "steps" not in res["receipt"]
    assert receipts.full(res)["steps"] == whole["steps"]
    assert timeline.rerun_items(B(), res, 10) == (3, "receipt-steps")
