"""Pool scheduler: FAC2 chunk sizes, per-node depth, cancelled losers, the
lost-race limit. run_remote is stubbed; no daemon (pacing is patched)."""
import threading
import time

import pytest

from pipedpeer_shim import pool, stats


@pytest.fixture
def pacing(monkeypatch):
    def set_(inflight=2, part_s=0.5, cores=0, peers=1):
        v = {"inflight": inflight, "part_s": part_s, "cores": cores, "peers": peers}
        monkeypatch.setattr(pool, "_cluster_pacing", lambda: v)
    set_()
    return set_


def local(vals):
    time.sleep(0.002 * len(vals))
    return [v * 2 for v in vals]


def test_fac2_chunks_shrink_toward_the_tail():
    left, sizes = 1000, []
    while left:
        k = pool._fac2_size(left, 3, 10)
        sizes.append(k)
        left -= k
    assert sum(sizes) == 1000
    assert sizes == sorted(sizes, reverse=True), sizes
    assert sizes[0] >= 150 and sizes[-1] <= 10, sizes
    assert pool._fac2_size(0, 3, 10) == 0
    assert pool._fac2_size(5, 3, 10) == 2      # fair share caps the floor


def test_chunk_floor_fills_the_receiving_cores():
    assert pool._chunk_floor(0.01, 2, {"part_s": 0.5, "cores": 0}) == 100
    assert pool._chunk_floor(0.01, 2, {"part_s": 0.5, "cores": 8}) == 400
    assert pool._chunk_floor(0, 4, {"part_s": 0.5, "cores": 8}) == 64


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

    r = pool._race_items(list(range(400)), 0.002, 1, local, remote)
    assert r == [v * 2 for v in range(400)]
    assert 1 <= peak[0] <= 3, "%d chunks in flight, daemon allowed 3" % peak[0]


def test_losing_copy_is_cancelled(pacing):
    pacing(inflight=1, peers=1)
    seen = []

    def remote(chunk):
        f = pool._TLS.flight
        seen.append(f)
        deadline = time.monotonic() + 10
        while not f.cancelled and time.monotonic() < deadline:
            time.sleep(0.01)
        return None            # the request died when its socket was shut

    t0 = time.monotonic()
    r = pool._race_items(list(range(20)), 0.001, 2, local, remote)
    assert r == [v * 2 for v in range(20)]
    time.sleep(0.2)
    assert seen and all(f.cancelled for f in seen), "a losing remote copy was left running"
    assert time.monotonic() - t0 < 5


def test_three_lost_races_pause_sending(pacing, monkeypatch):
    pacing(inflight=1, peers=1)
    monkeypatch.setitem(stats._STATS, "races_lost", 0)
    monkeypatch.setitem(pool._LOST, "n", 0)
    monkeypatch.setitem(pool._LOST, "until", 0.0)
    calls = []

    def remote(chunk):          # always beaten: holds its chunk until cut off
        calls.append(len(chunk))
        f = pool._TLS.flight
        while not f.cancelled:
            time.sleep(0.005)
        return None

    for _ in range(10):
        if pool._remote_paused():
            break
        r = pool._race_items(list(range(40)), 0.0001, 1, local, remote)
        assert r == [v * 2 for v in range(40)]
        time.sleep(0.1)         # the cancelled thread counts its loss
    assert pool._remote_paused(), "still sending after %d lost races" % stats._STATS["races_lost"]
    assert stats._STATS["races_lost"] >= pool._LOST_LIMIT
    assert stats._STATS["wasted_peer_s"] > 0
    n = len(calls)
    pool._race_items(list(range(40)), 0.0001, 1, local, remote)
    assert len(calls) == n, "sent a chunk while paused"
