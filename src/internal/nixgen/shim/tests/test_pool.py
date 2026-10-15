"""Pool scheduler: FAC2 chunk sizes, per-node depth, cancelled losers, the
lost-race limit. run_remote is stubbed; no daemon (pacing is patched)."""
import threading
import time

import pytest

from pipedpeer_shim import pool, pool_func, pool_pacing, pool_race, stats


@pytest.fixture
def pacing(monkeypatch):
    def set_(inflight=2, part_s=0.5, cores=0, peers=1):
        v = {"inflight": inflight, "part_s": part_s, "cores": cores, "peers": peers}
        monkeypatch.setattr(pool_race, "_cluster_pacing", lambda: v)
    set_()
    return set_


def local(vals):
    time.sleep(0.002 * len(vals))
    return [v * 2 for v in vals]


def test_fac2_chunks_shrink_toward_the_tail():
    left, sizes = 1000, []
    while left:
        k = pool_pacing._fac2_size(left, 3, 10)
        sizes.append(k)
        left -= k
    assert sum(sizes) == 1000
    assert sizes == sorted(sizes, reverse=True), sizes
    assert sizes[0] >= 150 and sizes[-1] <= 10, sizes
    assert pool_pacing._fac2_size(0, 3, 10) == 0
    assert pool_pacing._fac2_size(5, 3, 10) == 2      # fair share caps the floor


def test_chunk_floor_fills_the_receiving_cores():
    assert pool_pacing._chunk_floor(0.01, 2, {"part_s": 0.5, "cores": 0}) == 100
    assert pool_pacing._chunk_floor(0.01, 2, {"part_s": 0.5, "cores": 8}) == 400
    assert pool_pacing._chunk_floor(0, 4, {"part_s": 0.5, "cores": 8}) == 64


def test_depth_from_the_daemon_is_respected(pacing):
    pacing(inflight=3, peers=1)
    lock = threading.Lock()
    live, peak = [0], [0]

    def remote(chunk):
        with lock:
            live[0] += 1
            peak[0] = max(peak[0], live[0])
        time.sleep(0.05)
        with lock:
            live[0] -= 1
        return [(i, v * 2) for i, v in chunk]

    r = pool_race._race_items(list(range(400)), 0.002, 1, local, remote)
    assert r == [v * 2 for v in range(400)]
    assert 1 <= peak[0] <= 3, "%d chunks in flight, daemon allowed 3" % peak[0]


def test_losing_copy_is_cancelled(pacing):
    pacing(inflight=1, peers=1)
    seen = []

    def remote(chunk):
        f = pool_pacing._TLS.flight
        seen.append(f)
        deadline = time.monotonic() + 10
        while not f.cancelled and time.monotonic() < deadline:
            time.sleep(0.01)
        return None            # the request died when its socket was shut

    t0 = time.monotonic()
    r = pool_race._race_items(list(range(20)), 0.001, 2, local, remote)
    assert r == [v * 2 for v in range(20)]
    time.sleep(0.2)
    assert seen and all(f.cancelled for f in seen), "a losing remote copy was left running"
    assert time.monotonic() - t0 < 5


def test_three_lost_races_pause_sending(pacing, monkeypatch):
    pacing(inflight=1, peers=1)
    monkeypatch.setitem(stats._STATS, "races_lost", 0)
    monkeypatch.setitem(pool_pacing._LOST, "n", 0)
    monkeypatch.setitem(pool_pacing._LOST, "until", 0.0)
    calls = []

    def remote(chunk):          # always beaten: holds its chunk until cut off
        calls.append(len(chunk))
        f = pool_pacing._TLS.flight
        while not f.cancelled:
            time.sleep(0.005)
        return None

    for _ in range(10):
        if pool_pacing._remote_paused():
            break
        r = pool_race._race_items(list(range(40)), 0.0001, 1, local, remote)
        assert r == [v * 2 for v in range(40)]
        time.sleep(0.1)         # the cancelled thread counts its loss
    assert pool_pacing._remote_paused(), "still sending after %d lost races" % stats._STATS["races_lost"]
    assert stats._STATS["races_lost"] >= pool_pacing._LOST_LIMIT
    assert stats._STATS["wasted_peer_s"] > 0
    n = len(calls)
    pool_race._race_items(list(range(40)), 0.0001, 1, local, remote)
    assert len(calls) == n, "sent a chunk while paused"


def _ignore_first_sigterm():
    import signal

    def once(*_):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, once)


def _sleep_long(x):
    time.sleep(60)
    return x


def test_stop_pool_kills_a_worker_that_ignores_sigterm():
    import multiprocessing
    p = multiprocessing.get_context("fork").Pool(2, initializer=_ignore_first_sigterm)
    procs = list(p._pool)
    p.map_async(_sleep_long, range(2))
    time.sleep(0.5)             # both workers inside _sleep_long
    p.close()
    t0 = time.monotonic()
    pool_pacing._stop_pool(p, grace=1.0)
    assert time.monotonic() - t0 < 5, "pool exit took %.1fs" % (time.monotonic() - t0)
    assert not any(w.is_alive() for w in procs)


def test_by_value_callable_rides_the_globals_frame_not_the_header():
    import pickle
    pytest.importorskip("cloudpickle")
    sent = {}

    def fake_send(header, globals_pickle, items, timeout, info, opener=None):
        sent["header"], sent["g"] = header, pickle.loads(globals_pickle)
        return [pickle.dumps(v) for v in range(len(items))]

    orig = pool._pool_send
    pool._pool_send = fake_send
    try:
        payload = pool_func._func_pickle(lambda v: v + 1)
        assert isinstance(payload["pickled"], bytes)
        pool._ClusterPool._remote_chunk(payload, [(0, 1), (1, 2)], False)
    finally:
        pool._pool_send = orig
    assert not sent["header"].get("func")
    assert pickle.loads(sent["g"][pool_pacing._FUNC_KEY])(1) == 2


def test_relayed_steps_keep_the_machine_that_ran_them(monkeypatch):
    monkeypatch.setattr(pool_pacing, "_RELAYED_STEPS", [])
    receipt = {"node": "origin-id", "parts": [
        {"where": "peer:10.0.0.2:4242", "items": 3, "ms": 5, "steps": [
            {"machine": "peer-id", "node": "peer-id", "chunk": 0, "step": "compute",
             "start_ns": 1, "dur_ns": 2}]},
        {"where": "peer:10.0.0.3:4242", "items": 1, "ms": 5, "steps": [
            {"step": "queue", "start_ns": 1, "dur_ns": 2}]},
    ]}
    pool_pacing._relay_steps(receipt)
    assert [s["machine"] for s in pool_pacing._RELAYED_STEPS] == ["peer-id", "10.0.0.3:4242"]
