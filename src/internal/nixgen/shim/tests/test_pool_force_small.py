"""Maps too small to measure: forced ones run on a peer, the rest stay local
and say why.

The whole of _run, _race, the offload path and _remote_chunk run for real. Only
the wire is replaced: fake daemon below receives what would have gone out over
HTTP, runs the kernel the way a worker rebuilds it, and answers with a receipt
naming where it ran. remote_used comes from that receipt, so a row can only
pass if the item left this machine.
"""
import copy
import pickle
from collections import namedtuple

import pytest

from pipedpeer_shim import core, pool, pool_func, pool_pacing, pool_race, stats
from pipedpeer_shim.transfer import _dumps, _loads


def _double(x):
    return x * 2


def _add(a, b):
    return a + b


def _boom(x):
    raise ValueError("boom %s" % x)


class _Unsendable:
    """Neither its source nor its value can leave this process."""

    def __call__(self, x):
        return x * 2

    def __reduce__(self):
        raise TypeError("cannot be pickled")


_UNSENDABLE = _Unsendable()


def _is_raised(results):
    return len(results) == 1 and isinstance(results[0], core._Raised)


Case = namedtuple("Case", "name call force daemon want remote_used local_items sent decision log")

CASES = [
    # H1: a forced map under the measuring head runs on a peer.
    Case("forced_one_item", lambda p: p.map(_double, [7]), True, "peer",
         [14], 1, 0, [7], (True, "forced"), None),
    Case("unforced_one_item", lambda p: p.map(_double, [7]), False, "peer",
         [14], 0, 1, [], (False, "too few"), None),
    Case("forced_four_items", lambda p: p.map(_double, [1, 2, 3, 4]), True, "peer",
         [2, 4, 6, 8], 4, 0, [1, 2, 3, 4], (True, "forced"), None),
    Case("unforced_four_items", lambda p: p.map(_double, [1, 2, 3, 4]), False, "peer",
         [2, 4, 6, 8], 0, 4, [], (False, "too few"), None),
    # The daemon had no free peer and ran it itself: nothing went elsewhere,
    # and the receipt counts the item as this machine's.
    Case("forced_daemon_ran_it_on_origin", lambda p: p.map(_double, [7]), True, "origin",
         [14], 0, 1, [7], (True, "forced"), None),
    # The cluster could not take it: the run still finishes, and says so.
    Case("forced_daemon_refuses", lambda p: p.map(_double, [7]), True, "refuse",
         [14], 0, 1, [7], (True, "forced"), "ran on this machine"),
    # L7: starmap.
    Case("forced_starmap", lambda p: p.starmap(_add, [(1, 2)]), True, "peer",
         [3], 1, 0, [(1, 2)], (True, "forced"), None),
    Case("unforced_starmap", lambda p: p.starmap(_add, [(1, 2)]), False, "peer",
         [3], 0, 1, [], (False, "too few"), None),
    # L7: catch, as _SubmitBatcher calls it.
    Case("forced_catch_ok", lambda p: p._run(_double, [(7,)], starmap=True, catch=True), True, "peer",
         [14], 1, 0, [(7,)], (True, "forced"), None),
    Case("forced_catch_raises_on_peer_comes_back_as_value",
         lambda p: p._run(_boom, [(7,)], starmap=True, catch=True), True, "peer",
         _is_raised, 0, 1, [(7,)], (True, "forced"), "ran on this machine"),
    # L7: no items at all.
    Case("forced_empty", lambda p: p.map(_double, []), True, "peer",
         [], 0, 0, [], None, None),
    Case("unforced_empty", lambda p: p.map(_double, []), False, "peer",
         [], 0, 0, [], None, None),
    # L6 and L7: a function that cannot be sent stays local and says why.
    Case("forced_unsendable", lambda p: p.map(_UNSENDABLE, [7]), True, "peer",
         [14], 0, 1, [], (False, "cannot be sent"), "cannot ship"),
]


@pytest.fixture
def receipt():
    saved = copy.deepcopy(stats._STATS)
    stats._STATS.update(remote_used=0, local_items=0, decisions=[], decisions_dropped=0)
    yield stats._STATS
    stats._STATS.clear()
    stats._STATS.update(saved)


def _daemon(mode, sent):
    def send(header, globals_pickle, items, timeout, info, opener=None):
        vals = [_loads(b) for b in items]
        sent.extend(vals)
        if mode == "refuse":
            raise RuntimeError("503 from the daemon")
        gvars = pickle.loads(globals_pickle)
        if pool_pacing._FUNC_KEY in gvars:
            fn = pickle.loads(gvars[pool_pacing._FUNC_KEY])
        else:
            ns = dict(gvars)
            exec(header["func_src"], ns)
            fn = ns[header["func_name"]]
        out = [fn(*v) if header["starmap"] else fn(v) for v in vals]
        where = "peer:10.0.0.2:4242" if mode == "peer" else "local"
        info["receipt"] = {"parts": [{"where": where, "items": len(vals), "ms": 1}]}
        return [_dumps(r) for r in out]
    return send


@pytest.fixture
def make_pool(monkeypatch):
    made = []

    def make(force, mode, sent):
        monkeypatch.setattr(pool, "_cluster_ready", lambda: True)
        monkeypatch.setattr(core, "_FORCE", force)
        monkeypatch.setattr(core, "_ENABLED", True)
        monkeypatch.setattr(core, "_VERBOSE", True)
        monkeypatch.setattr(pool, "_OFFLOAD_HERE", False)
        monkeypatch.setattr(pool_race, "_OFFLOAD_HERE", False)
        monkeypatch.setattr(pool_race, "_cluster_pacing",
                            lambda: {"inflight": 2, "part_s": 0.5, "cores": 0, "peers": 1})
        monkeypatch.setattr(pool, "_pool_send", _daemon(mode, sent))
        p = pool._ClusterPool(2)
        monkeypatch.setattr(p, "_resize", lambda: None)
        made.append(p)
        return p

    yield make
    for p in made:
        p.close()


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_small_map(case, make_pool, receipt, capsys):
    sent = []
    p = make_pool(case.force, case.daemon, sent)
    got = case.call(p)
    if callable(case.want):
        assert case.want(got), got
    else:
        assert got == case.want
    assert receipt["remote_used"] == case.remote_used
    assert receipt["local_items"] == case.local_items
    assert sent == case.sent
    decisions = receipt["decisions"]
    if case.decision is None:
        assert decisions == []
    else:
        shipped, why = case.decision
        assert [d["shipped"] for d in decisions] == [shipped]
        assert why in decisions[0]["why"]
    if case.log is not None:
        assert case.log in capsys.readouterr().err


def test_unforced_small_maps_say_so_once_per_pool(make_pool, receipt):
    # A loop of small maps must not fill the receipt's capped decision list
    # and push out the lines that matter.
    p = make_pool(False, "peer", [])
    for _ in range(3):
        p.map(_double, [7])
    assert len(receipt["decisions"]) == 1


def test_forced_small_map_is_said_every_time(make_pool, receipt):
    p = make_pool(True, "peer", [])
    for _ in range(3):
        p.map(_double, [7])
    assert [d["shipped"] for d in receipt["decisions"]] == [True, True, True]


def test_payload_helpers_are_the_real_ones():
    # The unsendable row means nothing if the helpers accept this callable.
    assert pool_func._func_payload(_UNSENDABLE) is None
    assert pool_func._func_pickle(_UNSENDABLE) is None
