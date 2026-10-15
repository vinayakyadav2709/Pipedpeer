import time

from pipedpeer_shim import record


def setup_function():
    record._STEPS.clear()


def test_steps_match_c24(monkeypatch):
    monkeypatch.setenv("PIPEDPEER_NODE_ID", "key-abc")
    before = time.time_ns()
    record.step("pickle", 0.5, 2048, chunk=3, node="peer1")
    record.step("compute", 1.25)
    a, b = record.steps()
    assert a == {"machine": "key-abc", "node": "peer1", "chunk": 3,
                 "step": "pickle", "start_ns": a["start_ns"],
                 "dur_ns": 500_000_000, "bytes": 2048}
    assert a["start_ns"] + a["dur_ns"] >= before
    assert "bytes" not in b and b["chunk"] == -1 and b["dur_ns"] == 1_250_000_000
    assert {s["step"] for s in (a, b)} <= record.NAMES


def test_into_appends_to_receipt():
    record.step("send", 0.1, 10)
    r = record.into({"remote_items": 1, "steps": [{"step": "queue"}]})
    assert [s["step"] for s in r["steps"]] == ["queue", "send"]
    assert r["remote_items"] == 1


def test_step_is_cheap():
    n = 100_000
    t0 = time.perf_counter()
    for _ in range(n):
        record.step("compute", 0.001)
    per = (time.perf_counter() - t0) / n
    # ponytail: generous bound for loaded CI; the target is < 1 us per step.
    assert per < 5e-6, per
