"""Unit tests for the shim's pure helpers. Run by scripts/shim-check.sh with a
bare pytest: no numpy, pandas or torch, which also proves every module
imports without them (they are imported lazily)."""
import importlib
import pickle

import pytest

from pipedpeer_shim import core, ddp, numpy_ops, pool, transfer


def test_modules_import_without_heavy_deps():
    for name in ("core", "stats", "transfer", "numpy_ops", "pandas_ops", "pool", "ddp"):
        importlib.import_module("pipedpeer_shim." + name)
    importlib.import_module("sitecustomize")


@pytest.mark.parametrize("weights,base,world", [
    ([0.5, 0.5], 32, 2),
    ([0.7, 0.3], 32, 2),
    ([0.2, 0.3, 0.5], 10, 3),
    ([0.34, 0.33, 0.33], 7, 3),
])
def test_weighted_batches_sum_to_ring_batch(weights, base, world):
    b = ddp._weighted_batches(weights, base, world)
    assert sum(b) == base * world
    assert all(x >= 1 for x in b)


def test_weighted_batches_left_rank_gets_nothing():
    b = ddp._weighted_batches([0.5, 0.0, 0.5], 16, 3)
    assert b[1] == 0
    assert sum(b) == 48


def test_adaptive_chunk_bounds():
    assert pool._adaptive_chunk(0) == 64
    assert pool._adaptive_chunk(-1) == 64
    assert pool._adaptive_chunk(1e-9) == 256      # cheap items: capped large
    assert pool._adaptive_chunk(10.0) == 1        # costly items: one per chunk
    assert pool._adaptive_chunk(0.01) == 50       # ~0.5 s of work per chunk


def test_chunk_covers_every_item_in_order():
    seq = list(range(10))
    parts = pool._chunk(seq, 3)
    assert parts == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert pool._chunk(seq, 0) == [seq]           # 0 falls back to 64


def test_maybe_compress_small_payload_untouched():
    items = [b"x" * 100]
    header = {}
    assert transfer._maybe_compress(items, header) is items
    assert "items_zlib" not in header


def test_maybe_compress_repetitive_payload_compressed():
    import zlib
    items = [pickle.dumps(b"abcdefgh" * 400000) for _ in range(2)]
    header = {}
    out = transfer._maybe_compress(list(items), header)
    assert header.get("items_zlib") is True
    assert [zlib.decompress(p) for p in out] == items


def test_maybe_compress_random_payload_untouched():
    import os
    items = [os.urandom(2 << 20)]
    header = {}
    assert transfer._maybe_compress(items, header) is items
    assert "items_zlib" not in header


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
