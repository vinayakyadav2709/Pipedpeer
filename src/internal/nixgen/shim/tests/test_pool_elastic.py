"""A map uses the peers that hold its closure while it runs, not only those
present when it started: a peer still importing when the job began (the
first job after a join) and a machine that joins mid-map both get work.

The daemon is a stub HTTP server answering /v1/pool/stats, so the shim's
own pacing request, cache and re-read are what is tested; run_remote is a
stub too."""
import http.server
import json
import threading
import time

import pytest

from pipedpeer_shim import core, pool_pacing, pool_race, stats

NO_PEER = {"peers": 0, "pending": 1, "inflight": 1, "part_ms": 500, "cores": 0}
ONE_PEER = {"peers": 1, "pending": 0, "inflight": 2, "part_ms": 250, "cores": 2}
TWO_PEERS = {"peers": 2, "pending": 0, "inflight": 4, "part_ms": 250, "cores": 4}


@pytest.fixture
def daemon(monkeypatch):
    """A daemon whose /v1/pool/stats answer the test changes as it goes."""
    state = {"pacing": dict(NO_PEER), "asked": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            state["asked"] += 1
            body = json.dumps(state["pacing"]).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    monkeypatch.setattr(core, "_URL", "http://127.0.0.1:%d" % srv.server_address[1])
    monkeypatch.setattr(core, "_ENABLED", True)
    monkeypatch.setattr(core, "_NUM_SHARDS", "1")      # nobody held it at exec
    monkeypatch.setitem(pool_pacing._PACING, "v", None)
    monkeypatch.setitem(pool_pacing._PACING, "t", -1e9)
    monkeypatch.setitem(pool_pacing._LOST, "n", 0)
    monkeypatch.setitem(pool_pacing._LOST, "until", 0.0)
    monkeypatch.setattr(pool_race, "_RACE_REPACE_S", 0.05)
    yield state
    srv.shutdown()


def local(vals):
    time.sleep(0.002 * len(vals))
    return [v * 2 for v in vals]


class Remote:
    """run_remote: records each chunk it is handed and how many run at once."""

    def __init__(self, secs_per_item=0.0005):
        self.lock = threading.Lock()
        self.chunks, self.live, self.peak = [], 0, 0
        self.per_item = secs_per_item

    def __call__(self, chunk):
        with self.lock:
            self.chunks.append((time.monotonic(), len(chunk)))
            self.live += 1
            self.peak = max(self.peak, self.live)
        time.sleep(self.per_item * len(chunk))
        with self.lock:
            self.live -= 1
        return [(i, v * 2) for i, v in chunk]

    def items(self, after=0.0):
        return sum(k for t, k in self.chunks if t >= after)


def later(secs, fn):
    t = threading.Timer(secs, fn)
    t.daemon = True
    t.start()


def test_cluster_ready_waits_for_a_peer_still_importing(daemon):
    # PIPEDPEER_CLOSURE_NODES=1 at exec used to mean local for the whole job.
    assert core._cluster_ready(), "a peer importing the closure is a cluster to come"
    daemon["pacing"] = {"peers": 0, "pending": 0, "inflight": 1}
    pool_pacing._PACING["v"] = None
    assert not core._cluster_ready(), "alone, with nobody getting the closure: stay local"


def test_cluster_ready_does_not_ask_when_peers_held_it_at_exec(daemon, monkeypatch):
    monkeypatch.setattr(core, "_NUM_SHARDS", "3")
    assert core._cluster_ready()
    assert daemon["asked"] == 0


def test_no_chunk_leaves_while_no_peer_holds_the_closure(daemon):
    remote = Remote()
    r = pool_race._race_items(list(range(300)), 0.002, 2, local, remote)
    assert r == [v * 2 for v in range(300)]
    assert remote.chunks == [], "sent work while only our own daemon could run it"


def test_a_peer_ready_after_the_map_started_gets_work(daemon):
    # The first job after a join: no peer holds the closure when the map
    # starts; one finishes importing 0.3 s in.
    remote = Remote()
    later(0.3, lambda: daemon.update(pacing=dict(ONE_PEER)))
    t0 = time.monotonic()
    r = pool_race._race_items(list(range(3000)), 0.002, 2, local, remote)
    assert r == [v * 2 for v in range(3000)]
    assert remote.chunks, "the peer that became ready mid-map got nothing"
    assert remote.chunks[0][0] - t0 >= 0.25, "sent before any peer held the closure"
    # Alone, local needs 6 s; the cap kept most of the map queued for the peer.
    assert remote.items() >= 600, "the peer got %d of 3000 items" % remote.items()


def test_a_machine_joining_mid_map_adds_chunks_in_flight(daemon):
    daemon["pacing"] = dict(ONE_PEER)
    remote = Remote(secs_per_item=0.001)
    joined = []

    def join():
        joined.append(time.monotonic())
        daemon["pacing"] = dict(TWO_PEERS)

    later(0.5, join)
    r = pool_race._race_items(list(range(10000)), 0.002, 2, local, remote)
    assert r == [v * 2 for v in range(10000)]
    assert joined
    assert remote.peak > ONE_PEER["inflight"], "the join raised no concurrency (peak %d)" % remote.peak
    after = remote.items(after=joined[0])
    assert after >= 10000 // 4, "only %d items left for the cluster after the join" % after


def test_chunks_are_capped_so_the_map_stays_queued(daemon):
    daemon["pacing"] = dict(ONE_PEER)
    sizes = []

    def remote(chunk):
        sizes.append(len(chunk))
        time.sleep(0.0001 * len(chunk))
        return [(i, v * 2) for i, v in chunk]

    def local_sized(vals):
        sizes.append(len(vals))
        time.sleep(0.0001 * len(vals))
        return [v * 2 for v in vals]

    r = pool_race._race_items(list(range(20000)), 0.001, 2, local_sized, remote)
    assert r == [v * 2 for v in range(20000)]
    # 2 s of work on max(width, the peer's cores) = 2 lanes at 1 ms an item.
    cap = pool_pacing._chunk_cap(0.001, 2, {"part_s": 0.25})
    assert cap == 4000
    assert max(sizes) <= cap, "a %d-item chunk; FAC2 alone hands out %d" % (max(sizes), 20000 // 6)


def test_a_failing_cluster_is_not_retried_every_repace(daemon):
    daemon["pacing"] = dict(ONE_PEER)
    calls = []

    def remote(chunk):
        calls.append(len(chunk))
        return None                     # the daemon could not take it

    r = pool_race._race_items(list(range(400)), 0.002, 2, local, remote)
    assert r == [v * 2 for v in range(400)]
    assert len(calls) <= ONE_PEER["inflight"], "%d chunks sent to a failing cluster" % len(calls)


def test_receipt_counts_each_item_once_when_a_peer_arrives(daemon, monkeypatch):
    monkeypatch.setitem(stats._STATS, "remote_used", 0)
    monkeypatch.setitem(stats._STATS, "local_items", 0)
    remote = Remote()
    later(0.2, lambda: daemon.update(pacing=dict(ONE_PEER)))
    pool_race._race_items(list(range(2000)), 0.002, 2, local, remote)
    assert stats._STATS["remote_used"] + stats._STATS["local_items"] == 2000
