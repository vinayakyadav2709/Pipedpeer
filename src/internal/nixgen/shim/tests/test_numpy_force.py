"""Numpy matmul under force/offload: when the cluster send fails, the local
fallback computes with numpy's own matmul, never the patched one (it once
re-entered the gate, which under force always says yes, and re-dispatched
every block). _pool_send is stubbed; no daemon."""
import pytest

np = pytest.importorskip("numpy")

from pipedpeer_shim import core, numpy_ops  # noqa: E402

_ATTRS = ["matmul", "dot", "tensordot"]


@pytest.fixture
def setup(monkeypatch):
    saved = {a: getattr(np, a) for a in _ATTRS}
    saved_lin = {a: getattr(np.linalg, a) for a in ("svd", "eig")}
    monkeypatch.delenv("PIPEDPEER_NUMPY_NESTED", raising=False)
    monkeypatch.setattr(core, "_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(core, "_ENABLED", True)
    monkeypatch.setattr(core, "_NUM_SHARDS", "2")
    calls = []
    monkeypatch.setattr(numpy_ops, "_log", lambda m: None)
    monkeypatch.setattr(numpy_ops, "_decide", lambda *a, **k: None)
    monkeypatch.setattr(numpy_ops, "_pool_required", lambda *a, **k: 0)

    def make(force, send):
        monkeypatch.setattr(core, "_FORCE", force)

        def counting(header, gpk, items, timeout):
            calls.append(len(items))
            return send(header, gpk, items, timeout)
        monkeypatch.setattr(numpy_ops, "_pool_send", counting)
        numpy_ops._install_numpy()
        return calls
    yield make
    for a, v in saved.items():
        setattr(np, a, v)
    for a, v in saved_lin.items():
        setattr(np.linalg, a, v)


def _fail(*a):
    raise RuntimeError("no cluster")


def _ok(header, gpk, items, timeout):
    import pickle
    other = pickle.loads(gpk)["_other"]
    return [pickle.dumps(pickle.loads(i) @ other) for i in items]


def _mats():
    rng = np.random.default_rng(0)
    return rng.random((256, 16)), rng.random((16, 8))


def test_force_send_fails_falls_back_without_redistributing(setup):
    calls = setup(True, _fail)
    a, b = _mats()
    want = a @ b
    got = np.matmul(a, b)
    assert len(calls) == 1, calls
    assert np.allclose(got, want)


def test_force_dispatch_once_and_matches(setup):
    calls = setup(True, _ok)
    a, b = _mats()
    got = np.matmul(a, b)
    assert len(calls) == 1
    assert np.allclose(got, a @ b)


def test_not_forced_small_stays_local(setup):
    calls = setup(False, _fail)
    a, b = _mats()
    got = np.matmul(a, b)
    assert calls == []
    assert np.allclose(got, a @ b)
