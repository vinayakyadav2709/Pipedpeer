import json
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
sys.path.insert(0, HERE)

from campaign import runner, systems  # noqa: E402
from campaign.workloads import Workload  # noqa: E402
from fakecluster import FakeCluster, FakeRemote, no_sleep  # noqa: E402

W1 = {
    "id": "W1", "script": "docs/benchmarks/workloads/pool_map.py",
    "ports": {"ray": "docs/benchmarks/workloads/pool_map.py", "dask": "docs/benchmarks/workloads/pool_map.py"},
    "strong": {"BENCH_ITEMS": "96"}, "weak_per_pc": {"BENCH_ITEMS": "10"}, "smoke": {"BENCH_ITEMS": "8"},
    "system_env": {"plain": {"BENCH_PROCS": "{nproc_submitter}"}, "ray": {"BENCH_PROCS": "{nproc_workers}"}},
    "result_regex": r"items=\d+|checksum=[-0-9.e+]+", "result_compare": "equal",
    "timing_regex": r"seconds=([0-9.]+)",
}

REQUIRED = ["run_id", "experiment", "workload", "system", "n", "repeat", "warmup", "start_ns", "end_ns",
            "wall_s", "result_ok", "hosts", "git_commit", "pipedpeer_version"]


@pytest.fixture
def make(tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    wl_file = tmp_path / "workloads.json"
    wl_file.write_text(json.dumps({"workloads": [W1]}))
    monkeypatch.setenv("PL_WORKLOADS", str(wl_file))
    monkeypatch.setenv("PL_WAIT_TIMEOUT", "5")

    def make(cluster=None, argv=("e1",)):
        cluster = cluster or FakeCluster()
        args = runner.parse_args(list(argv) + ["--seed", "7"])
        ctx = runner.Context(args, remote=FakeRemote(cluster), inv=cluster.inv, out_dir=str(tmp_path / "out"))
        ctx.start()
        return ctx, cluster

    return make


def lines(ctx):
    with open(ctx.recorder.path) as f:
        return [json.loads(x) for x in f]


def test_every_line_has_the_schema_and_warmups_come_first(make):
    ctx, _ = make()
    ctx.scaling(["W1"], ["plain", "pipedpeer", "ray", "dask"], [1, 2], 2)
    got = lines(ctx)
    for ln in got:
        missing = [k for k in REQUIRED if k not in ln]
        assert not missing, (missing, ln)
        assert ln["error"] is None if "error" in ln else True, ln
    # N=1: 4 systems x (1 warm-up + 2 repeats); N=2: 3 systems (no plain) x 3
    assert len(got) == 12 + 9
    for n in (1, 2):
        block = [x for x in got if x["n"] == n]
        k = len({x["system"] for x in block})
        assert all(x["warmup"] for x in block[:k]), "warm-ups are not first in N=%d" % n
        assert not any(x["warmup"] for x in block[k:])
        assert sorted(x["system"] for x in block if x["warmup"]) == sorted({x["system"] for x in block})
    assert got[0]["system"] == "plain", "plain's warm-up must come first: it is the reference"
    assert not any(x["system"] == "plain" and x["n"] != 1 for x in got)
    assert all(x["result_ok"] is True for x in got), [x for x in got if not x["result_ok"]]


def test_repeat_order_is_random_per_repeat_and_reproducible_by_seed(make):
    orders = []
    for _ in range(2):
        ctx, _ = make()
        ctx.recorder.path = ctx.recorder.path + ".%d" % len(orders)
        ctx.scaling(["W1"], ["pipedpeer", "ray", "dask"], [1], 6)
        orders.append([x["system"] for x in lines(ctx) if not x["warmup"]])
    assert orders[0] == orders[1], "same seed, different order"
    rounds = [tuple(orders[0][i:i + 3]) for i in range(0, 18, 3)]
    assert len(set(rounds)) > 1, "every repeat ran the systems in the same order"


def test_a_failed_run_is_recorded_and_the_experiment_goes_on(make):
    def fail(host, cmd):
        if host == "pc02" and " start --address=" in cmd:
            return 1, "ray: could not connect to the head\n"
    ctx, _ = make(FakeCluster(fail=fail))
    ctx.scaling(["W1"], ["pipedpeer", "ray"], [1, 2], 1)
    got = lines(ctx)
    bad = [x for x in got if x.get("error")]
    assert bad and all(x["system"] == "ray" and x["n"] == 2 for x in bad)
    assert bad[0]["failing_pc"] == "pc02"
    assert bad[0]["result_ok"] is False and bad[0]["log_path"]
    assert os.path.isdir(os.path.join(ctx.out_dir, bad[0]["log_path"]))
    assert any(x["system"] == "pipedpeer" and x["n"] == 2 and not x.get("error") for x in got)


def test_result_ok_compares_with_the_plain_run_at_the_same_size(make):
    ctx, cluster = make()
    ctx.scaling(["W1"], ["plain"], [1], 1)
    cluster.result = "checksum=9.9"  # every later run disagrees with plain
    ctx.scaling(["W1"], ["pipedpeer"], [2], 1)
    got = lines(ctx)
    assert all(x["result_ok"] for x in got if x["system"] == "plain")
    pp = [x for x in got if x["system"] == "pipedpeer"]
    assert pp and all(x["result_ok"] is False and "differs" in x["error"] for x in pp)
    assert all(x["result_ref"] == "plain" for x in pp)


def test_a_missing_reference_runs_plain_first(make):
    ctx, cluster = make()
    ctx.scaling(["W1"], ["pipedpeer"], [2], 1)
    got = lines(ctx)
    assert all(x["result_ok"] is True and x["result_ref"] == "plain" for x in got)
    assert any(x["system"] == "plain" for x in got) is False, "the reference run must not be recorded"
    assert ctx.recorder.cache.get("W1", {"BENCH_ITEMS": "96"})["source"] == "plain"


def test_an_uncalibrated_size_fails_the_run_not_the_campaign(make):
    ctx, _ = make()
    ctx.workloads["W1"] = Workload(dict(W1, strong={"BENCH_ITEMS": "0"}))
    ctx.scaling(["W1"], ["pipedpeer"], [1], 1)
    got = lines(ctx)
    assert got and all("calibrate" in x["error"] for x in got)


def test_pipedpeer_runs_on_exactly_n_pcs_and_pc00_is_one(make):
    ctx, cluster = make()
    b = ctx.backend("pipedpeer")
    hosts = b.activate(3)
    assert [h.id for h in hosts] == ["pc00", "pc01", "pc02"]
    assert cluster.daemons == {"pc00", "pc01", "pc02"}
    b.activate(1)
    assert cluster.daemons == {"pc00"}
    ctx.release_all()
    assert cluster.daemons == set(cluster.ids), "release must bring every daemon back"


def test_pipedpeer_refuses_a_cluster_that_never_reaches_n(make):
    def fail(host, cmd):
        if host == "pc02" and "pipedpeer start" in cmd:
            return 0, "started"  # says it started, never joins
    cluster = FakeCluster(fail=fail)
    ctx, _ = make(cluster)
    cluster.daemons.discard("pc02")
    with pytest.raises(systems.StepError) as e:
        ctx.backend("pipedpeer").activate(3)
    assert e.value.host == "pc00" and "want exactly 2" in str(e.value)


def test_ray_head_does_no_work_and_n_workers_join(make):
    ctx, cluster = make()
    hosts = ctx.backend("ray").activate(2)
    assert [h.id for h in hosts] == ["pc00", "pc01", "pc02"]
    assert cluster.ray == {"pc00": "head:0", "pc01": "4", "pc02": "4"}
    ctx.backend("dask").activate(2)
    assert cluster.ray == {}, "starting Dask must stop Ray"
    assert set(cluster.dask) == {"pc00", "pc01", "pc02"} and cluster.dask["pc00"] == "scheduler"
    ctx.backend("dask").deactivate()
    ctx.backend("dask").deactivate()  # idempotent
    assert cluster.dask == {}


def test_caps_reach_every_system(make):
    ctx, cluster = make()
    ctx.backend("ray").activate(2, caps=[2, 1])
    assert cluster.ray["pc01"] == "2" and cluster.ray["pc02"] == "1"
    starts = [c for h, c in cluster.calls if h == "pc02" and " start --address=" in c]
    assert "taskset -c 0 " in starts[-1]
    ctx.backend("pipedpeer-headcount").activate(2, caps=[2, 1])
    assert cluster.daemon_caps["pc00"] == "0-1" and cluster.daemon_caps["pc01"] == "0"
    assert "PIPEDPEER_POOL_SPLIT=headcount" in cluster.daemon_env["pc01"]


def test_system_env_fills_core_counts(make):
    ctx, _ = make()
    ctx.scaling(["W1"], ["ray"], [2], 1)
    runs = [c for h, c in ctx.remote.c.calls if "RAY_ADDRESS" in c and "pool_map.py" in c]
    assert runs and "BENCH_PROCS=8" in runs[-1], runs[-1]  # 2 workers x 4 cores


def test_weak_scaling_multiplies_the_per_pc_size(make):
    ctx, _ = make()
    ctx.scaling(["W1"], ["pipedpeer"], [1, 4], 1, mode="weak", reference="first")
    got = lines(ctx)
    assert {x["n"]: x["size"]["BENCH_ITEMS"] for x in got} == {1: "10", 4: "40"}
    assert all(x["result_ref"] == "pipedpeer" for x in got)


def test_rehearsal_defaults(make):
    ctx, _ = make(argv=("e1", "--rehearsal"))
    assert ctx.ns([1, 2, 4, 8]) == [1, 2] and ctx.repeats(5) == 1
    assert ctx.size_for(ctx.workloads["W1"], "strong", 2) == {"BENCH_ITEMS": "8"}


def test_every_experiment_module_loads():
    names = sorted(f[:-3] for f in os.listdir(os.path.join(HERE, "..", "experiments")) if f.endswith(".py"))
    assert names == ["e1", "e10", "e11", "e1b", "e3", "e4", "e5", "e6", "e7", "e8", "e9"]
    for n in names:
        mod = runner.load_experiment(n)
        assert callable(mod.main), n


def test_guided_timer_records_one_line_per_attempt(tmp_path):
    import io
    mod = runner.load_experiment("e10")
    feed = io.StringIO("2\ny\n\n\n7\n0\nnone\n\nfail\n3\n2\nport stuck\nn\n")
    args = runner.parse_args(["e10"])
    mod.main(args, str(tmp_path), out=io.StringIO(), inp=feed)
    got = [json.loads(x) for x in open(tmp_path / "runs.jsonl")]
    assert [(x["participant"], x["system"], x["success"]) for x in got] == [
        ("p1", "pipedpeer", True), ("p1", "dask", False)]
    assert got[0]["commands_typed"] == 7 and got[1]["script_lines_changed"] == 2
    assert got[1]["error"] == "gave up" and all(x["experiment"] == "e10" for x in got)


def test_runs_log_where_the_node_collector_reads(make):
    """otel/README.md "Runner integration" 3-5: each run's output in
    logs/<system>/…<run_id>.log on pc00, Ray on fixed ports with the metrics
    agent on 8090, Dask workers on fixed port ranges and appending logs."""
    ctx, cluster = make()
    ctx.scaling(["W1"], ["pipedpeer", "ray", "dask"], [2], 1)
    got = {ln["system"]: ln["run_id"] for ln in lines(ctx) if ln["repeat"] == 1}
    cmds = [c for _, c in cluster.calls]
    assert any("logs/pipedpeer-run/%s.log" % got["pipedpeer"] in c and "pipedpeer run" in c for c in cmds)
    assert any("logs/ray/run-%s.log" % got["ray"] in c for c in cmds)
    assert any("logs/dask/run-%s.log" % got["dask"] in c for c in cmds)
    starts = [c for c in cmds if " start --head" in c or " start --address=" in c]
    assert starts and all("--metrics-export-port=8090" in c and "--node-manager-port=6380" in c for c in starts)
    bg = [c for c in cmds if c.startswith("bg: ")]
    assert any(" worker " in c and "--worker-port 9000:9099" in c and ">> " in c and "dask/worker.log" in c for c in bg)
    assert all("--no-dashboard" not in c for c in bg)


@pytest.mark.parametrize("argv,env,want", [
    (("e1", "--rehearsal"), {}, False),          # a rehearsal never uploads
    (("e1",), {}, False),                        # results outside results/lab (tmp_path) never upload
    (("e1", "--rehearsal"), {"PL_SYNC": "1"}, True),   # a harness with a stand-in may force it
    (("e1",), {"PL_SYNC": "0"}, False),
])
def test_sync_only_for_a_real_campaign(make, monkeypatch, argv, env, want):
    calls = []
    monkeypatch.setattr(runner.subprocess, "call", lambda a, **kw: calls.append((a, kw)) or 0)
    monkeypatch.delenv("PL_SYNC", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    ctx, _ = make(argv=argv)
    ctx.sync()
    assert bool(calls) == want, calls
    if calls:
        assert calls[0][1]["env"]["LAB_RESULTS"] == ctx.out_dir


def test_receipt_is_bounded_inline_and_whole_in_a_file(make):
    """REPORT.md gap 1: per-item steps never go into runs.jsonl."""
    import gzip
    cluster = FakeCluster()
    cluster.receipt_steps = 20000
    ctx, _ = make(cluster)
    ctx.scaling(["W1"], ["pipedpeer"], [2], 1)
    ln = [x for x in lines(ctx) if x["repeat"] == 1][0]
    rc = ln["receipt"]
    assert "steps" not in rc and rc["steps_total"] == 20001
    assert rc["remote_items"] == 3 and rc["local_items"] == 5
    by = {(x["step"], x["node"]): x for x in rc["steps_by"]}
    assert by[("pickle", "n0")]["count"] == 10000 and by[("pickle", "n0")]["bytes"] == 100000
    assert len(json.dumps(ln)) < 5000
    full = json.load(gzip.open(ln["receipt_path"]))
    assert len(full["steps"]) == 20001


def test_size_override_is_per_pc_in_weak_mode():
    """REPORT.md gap 12: --size must not switch weak scaling off."""
    wl = Workload(W1)
    assert wl.size("weak", 4, override={"BENCH_ITEMS": "48"}) == {"BENCH_ITEMS": "192"}
    assert wl.size("strong", 4, override={"BENCH_ITEMS": "48"}) == {"BENCH_ITEMS": "48"}


def test_a_failed_timed_command_keeps_its_stderr(tmp_path, monkeypatch):
    """REPORT.md gap 13: raw over a local connection has no pty, so stderr
    would be dropped; the timed command merges it into the output."""
    from campaign.remote import Remote
    fake = tmp_path / "ansible-fake"
    fake.write_text('#!/bin/sh\n[ "$1" = sh ] && exec sh -c "$3" 2>/dev/null\n')
    fake.chmod(0o755)
    monkeypatch.setenv("PL_ANSIBLE_FAKE", str(fake))
    t = Remote(str(tmp_path / "inv.ini")).timed("pc00", "echo out; echo Traceback-boom >&2; exit 3", timeout=30)
    assert t.rc == 3 and "out" in t.out and "Traceback-boom" in t.out, t
