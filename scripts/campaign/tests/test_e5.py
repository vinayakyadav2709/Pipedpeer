"""E5's cold case: pipedpeer's cold ship runs before the campaign env reaches
the workers (the day's prep.e5cold), records how much of its closure each
PC already held, and the E5 row does not run it a second time."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
sys.path.insert(0, HERE)

from campaign import runner  # noqa: E402
from fakecluster import FakeCluster, FakeRemote, no_sleep  # noqa: E402

CLOSURE = ["/nix/store/aaa-python3-3.14.7", "/nix/store/bbb-python3.14-numpy-2.5.1", "/nix/store/ccc-e5-env"]


def respond(host, cmd):
    if "pipedpeer run" in cmd:
        return 0, "Store path: /nix/store/ccc-e5-env\nE5-ENV-OK numpy=1 sum=1\n"
    if "PLSTORE" in cmd:
        return 0, "PLSTORE 40\n"
    if "-qR" in cmd:
        return 0, "".join("PLPATH %s %d\n" % (p, 100_000_000) for p in CLOSURE)
    if "PLHAVE" in cmd:
        # The listing was taken before the run: workers held only python.
        held = CLOSURE if host == "pc00" else CLOSURE[:1]
        return 0, "".join("PLHAVE %s\n" % p.rsplit("/", 1)[1] for p in held) + "PLDONE\n"
    if "/v1/store?path=" in cmd:
        return 0, '{"cached": true}'
    if "PLPIN" in cmd:
        return 0, "".join("PLPIN %s==1\n" % p for p in ("numpy", "pandas", "scikit-learn", "joblib", "torch"))
    return None


def run(tmp_path, monkeypatch, *argv):
    no_sleep(monkeypatch)
    monkeypatch.setenv("PL_SYSTEMS", "pipedpeer")
    cluster = FakeCluster(fail=respond)
    args = runner.parse_args(["e5", "--n", "3", "--repeats", "1", "--seed", "7", *argv])
    ctx = runner.Context(args, remote=FakeRemote(cluster), inv=cluster.inv, out_dir=str(tmp_path / "out"))
    ctx.start()
    runner.load_experiment("e5").main(ctx)
    with open(ctx.recorder.path) as f:
        return [json.loads(x) for x in f], cluster


def test_early_cold_ship_records_what_each_pc_held(tmp_path, monkeypatch):
    got, cluster = run(tmp_path, monkeypatch, "--part", "pipedpeer-cold")
    assert [(x["system"], x["phase"]) for x in got] == [("pipedpeer", "cold")]
    line = got[0]
    assert line["before_campaign_env"] is True and line["result_ok"] is True
    assert line["closure_paths"] == 3 and line["closure_mb"] == 300.0
    assert line["present_before"]["pc01"] == {"paths": 1, "mb": 100.0, "python_packages": []}
    assert line["present_before"]["pc00"]["python_packages"] == ["python3.14-numpy-2.5.1"]
    # No pins read and no pip: the env is not on the PCs yet.
    assert not any("PLPIN" in c or "pip install" in c for _, c in cluster.calls)
    # The listing is taken before the job starts.
    order = [c for _, c in cluster.calls if "PLSTORE" in c or "pipedpeer run" in c]
    assert "PLSTORE" in order[0] and "pipedpeer run" in order[-1]


def test_the_e5_row_does_not_repeat_the_early_cold_ship(tmp_path, monkeypatch):
    run(tmp_path, monkeypatch, "--part", "pipedpeer-cold")
    got, _ = run(tmp_path, monkeypatch)
    cold = [(x["system"], x.get("before_campaign_env")) for x in got if x.get("phase") == "cold"]
    assert cold == [("pipedpeer", True), ("pip", None)]
    upd = [x for x in got if x.get("phase") == "update" and x["system"] == "pipedpeer"]
    assert upd and upd[0]["before_campaign_env"] is False and upd[0]["present_before"]


def test_without_the_early_ship_the_row_runs_cold_itself(tmp_path, monkeypatch):
    got, _ = run(tmp_path, monkeypatch)
    cold = [(x["system"], x.get("before_campaign_env")) for x in got if x.get("phase") == "cold"]
    assert cold == [("pipedpeer", False), ("pip", None)]
