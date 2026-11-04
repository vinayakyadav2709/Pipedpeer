"""Groupby shuffle under force: the local bucket and the unobserved-category
helper aggregate with pandas' own methods, so the shuffle runs once per call
(it once re-entered the patched agg and recursed until OOM). _pool_send is
stubbed; no daemon."""
import pickle

import pytest

pd = pytest.importorskip("pandas")
from pandas.core.groupby import generic as gbg  # noqa: E402

from pipedpeer_shim import core, pandas_ops  # noqa: E402

_ATTRS = ["agg", "sum", "mean", "count", "size", "min", "max", "first", "last",
          "std", "var", "median", "nunique", "prod", "any", "all"]


@pytest.fixture
def forced(monkeypatch):
    saved = {a: getattr(gbg.DataFrameGroupBy, a) for a in _ATTRS}
    orig_agg = saved["agg"]
    logs, sent = [], []
    monkeypatch.setattr(core, "_NUM_SHARDS", "2")
    monkeypatch.setattr(core, "_FORCE", True)
    monkeypatch.setattr(pandas_ops, "_OFFLOAD_HERE", False)
    monkeypatch.setattr(pandas_ops, "_should_spill", lambda *a, **k: True)
    monkeypatch.setattr(pandas_ops, "_pool_required", lambda *a, **k: 0)
    monkeypatch.setattr(pandas_ops, "_log", logs.append)

    def fake_send(header, gpk, items, timeout):
        g = pickle.loads(gpk)
        sent.append(len(items))
        out = []
        for raw in items:
            sub = pickle.loads(raw)
            gb = sub.groupby(g["_by"], as_index=g["_as_index"], sort=False,
                             dropna=g["_dropna"], observed=True)
            out.append(pickle.dumps(orig_agg(gb, g["_spec"], *g["_args"], **g["_kwargs"])))
        return out
    monkeypatch.setattr(pandas_ops, "_pool_send", fake_send)
    pandas_ops._install_pandas()
    yield logs, sent
    for a, v in saved.items():
        setattr(gbg.DataFrameGroupBy, a, v)


def _frame(n=2000):
    return pd.DataFrame({"k": [i % 37 for i in range(n)],
                         "v": [float(i) for i in range(n)]})


def _shuffles(logs):
    return [m for m in logs if "hash-shuffling" in m]


def test_agg_shuffles_once_and_matches_plain_pandas(forced):
    logs, sent = forced
    df = _frame()
    got = df.groupby("k").agg({"v": "sum"})
    assert len(_shuffles(logs)) == 1, logs
    assert sent == [1]
    assert got["v"].sum() == df["v"].sum()
    assert len(got) == 37


@pytest.mark.parametrize("name", ["sum", "mean"])
def test_named_aggs_do_not_recurse(forced, name):
    logs, _ = forced
    df = _frame()
    got = getattr(df.groupby("k"), name)()
    assert len(_shuffles(logs)) == 1, logs
    assert len(got) == 37


def test_categorical_unobserved_helper_does_not_reshuffle(forced):
    logs, _ = forced
    df = _frame()
    df["k"] = pd.Categorical(df["k"], categories=list(range(40)))
    got = df.groupby("k", observed=False).agg({"v": "sum"})
    assert len(_shuffles(logs)) == 1, logs
    assert len(got) == 40


def test_offload_ships_bucket_zero_too(forced, monkeypatch):
    logs, sent = forced
    monkeypatch.setattr(pandas_ops, "_OFFLOAD_HERE", True)
    df = _frame()
    got = df.groupby("k").agg({"v": "sum"})
    assert sent == [2]
    assert got["v"].sum() == df["v"].sum()


def test_force_off_small_frame_never_shuffles(forced, monkeypatch):
    logs, sent = forced
    monkeypatch.setattr(pandas_ops, "_should_spill", lambda *a, **k: False)
    df = _frame(100)
    got = df.groupby("k").agg({"v": "sum"})
    assert not _shuffles(logs) and not sent
    assert got["v"].sum() == df["v"].sum()
