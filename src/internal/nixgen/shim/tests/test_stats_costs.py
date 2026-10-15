import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipedpeer_shim import stats  # noqa: E402


def _s(step, nbytes, sec, machine="m0", node=""):
    return {"machine": machine, "node": node, "chunk": 0, "step": step,
            "start_ns": 0, "dur_ns": int(sec * 1e9), "bytes": nbytes}


def test_speed_index_relative_to_ref():
    steps = [_s("compute", 1000, 1.0, node="a"), _s("compute", 1000, 0.5, node="b"),
             _s("send", 1000, 9.0, node="a")]
    idx = stats.speed_index(steps, ref="a")
    assert idx == {"a": 1.0, "b": 2.0}


def test_op_cost_fits_fixed_plus_per_byte():
    steps = [_s("send", n, 0.01 + 1e-6 * n) for n in (1000, 10000, 100000)]
    a, b = stats.op_cost(steps)["send"]
    assert abs(a - 0.01) < 1e-6 and abs(b - 1e-6) < 1e-9
    assert abs(stats.predict("send", 50000, stats.op_cost(steps)) - 0.06) < 1e-6
    assert stats.predict("compute", 1, {}) is None
