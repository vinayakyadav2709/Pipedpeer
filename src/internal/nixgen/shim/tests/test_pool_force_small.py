"""A forced small map goes remote; an unforced one stays local. No daemon:
_race and _local are stubbed."""
from pipedpeer_shim import core, pool


def _pool(monkeypatch, force):
    monkeypatch.setattr(pool, "_cluster_ready", lambda: True)
    monkeypatch.setattr(core, "_FORCE", force)
    monkeypatch.setattr(pool, "_OFFLOAD_HERE", False)
    p = pool._ClusterPool(2)
    calls = []
    monkeypatch.setattr(p, "_resize", lambda: None)
    monkeypatch.setattr(p, "_race", lambda f, items, *a, **k: calls.append("remote") or [f(i) for i in items])
    monkeypatch.setattr(p, "_local", lambda f, items, *a, **k: calls.append("local") or [f(i) for i in items])
    return p, calls


def _double(x):
    return x * 2


def test_forced_one_item_ships_and_returns_its_value(monkeypatch):
    p, calls = _pool(monkeypatch, True)
    assert p.map(_double, [7]) == [14]
    assert calls == ["remote"]


def test_unforced_one_item_stays_local(monkeypatch):
    p, calls = _pool(monkeypatch, False)
    assert p.map(_double, [7]) == [14]
    assert calls == ["local"]
