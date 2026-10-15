"""Unit tests for the shim's pure helpers. Run by scripts/shim-check.sh with a
bare pytest: no numpy, pandas or torch, which also proves every module
imports without them (they are imported lazily)."""
import importlib
import pickle


from pipedpeer_shim import core, numpy_ops, pool_func, transfer


def test_modules_import_without_heavy_deps():
    for name in ("core", "stats", "transfer", "numpy_ops", "pandas_ops", "pandas_ooc", "pandas_combine", "pandas_frame", "pool", "pool_pacing", "pool_race", "pool_resources", "pool_func", "ddp"):
        importlib.import_module("pipedpeer_shim." + name)
    importlib.import_module("sitecustomize")


def test_adaptive_chunk_bounds():
    assert pool_func._adaptive_chunk(0) == 64
    assert pool_func._adaptive_chunk(-1) == 64
    assert pool_func._adaptive_chunk(1e-9) == 256      # cheap items: capped large
    assert pool_func._adaptive_chunk(10.0) == 1        # costly items: one per chunk
    assert pool_func._adaptive_chunk(0.01) == 50       # ~0.5 s of work per chunk


def test_chunk_covers_every_item_in_order():
    seq = list(range(10))
    parts = pool_func._chunk(seq, 3)
    assert parts == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert pool_func._chunk(seq, 0) == [seq]           # 0 falls back to 64


def test_frames_body_layout():
    import json
    import struct
    body = transfer._frames_body({"items_frames": 2}, b"G", [b"ab", b"c"])
    head, rest = body.split(b"\n", 1)
    assert json.loads(head) == {"items_frames": 2}
    assert rest == struct.pack(">I", 1) + b"G" + struct.pack(">I", 2) + b"ab" + struct.pack(">I", 1) + b"c"


def test_frames_body_is_linear():
    # The old body += loop copied the body so far per item: 20k x 1 KiB items
    # meant ~200 GB of copying. A single join does it in milliseconds.
    import time
    items = [b"x" * 1024] * 20000
    t0 = time.monotonic()
    body = transfer._frames_body({"items_frames": len(items)}, None, items)
    assert time.monotonic() - t0 < 5
    assert len(body) > 20000 * 1028


def test_dumps_loads_plain_and_out_of_band():
    assert transfer._loads(transfer._dumps({"a": [1, 2]})) == {"a": [1, 2]}
    raw = bytearray(b"z" * 4096)
    blob = transfer._dumps(pickle.PickleBuffer(raw))
    assert blob[:4] == transfer._OOB_MAGIC
    back = transfer._loads(bytearray(blob))
    assert bytes(back) == bytes(raw)


def test_pool_required():
    assert transfer._pool_required(b"g" * 10, []) == 0
    assert transfer._pool_required(b"g" * 10, [b"a" * 5, b"b" * 20], "f" * 3) == 2 * (10 + 3 + 20)


def test_per_peer_bw(monkeypatch):
    monkeypatch.setattr(core, "_NUM_SHARDS", "5")     # 4 peers
    assert numpy_ops._per_peer_bw(400.0) == 100.0
    monkeypatch.setattr(core, "_NUM_SHARDS", "20")    # capped at 8 links
    assert numpy_ops._per_peer_bw(800.0) == 100.0
    monkeypatch.setattr(core, "_NUM_SHARDS", "1")     # nobody else: whole link
    assert numpy_ops._per_peer_bw(800.0) == 800.0


def test_offload_wins_split():
    # 1 GB at 100 flops/byte on a 1 GFLOP/s machine: 100 s here. Over 1 GB/s
    # with 2 round trips: 2 s transfer + 100/4*1.3 = 32.5 s compute on 4 nodes.
    assert numpy_ops._offload_wins(1e9, 100, 2, True, 1e9, 4, 1e9) is True
    # Same work over a 10 MB/s link: 200 s of transfer alone, stay here.
    assert numpy_ops._offload_wins(1e9, 100, 2, True, 1e9, 4, 1e7) is False


def test_offload_wins_single_worker_needs_transfer_under_half_local():
    # 100 s here; transfer 40 s is under half: relocate.
    assert numpy_ops._offload_wins(1e9, 100, 2, False, 1e9, 1, 5e7) is True
    # transfer 66 s is over half: stay.
    assert numpy_ops._offload_wins(1e9, 100, 2, False, 1e9, 1, 3e7) is False
