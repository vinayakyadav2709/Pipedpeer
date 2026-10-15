"""Unit tests for ddp.py's pure helpers: the batch split and the int8 codec.
The batch tests need nothing; the codec tests skip without numpy."""
import pytest

from pipedpeer_shim import ddp


@pytest.mark.parametrize("weights,base,world", [
    ([0.5, 0.5], 32, 2),
    ([0.7, 0.3], 32, 2),
    ([0.2, 0.3, 0.5], 10, 3),
    ([0.34, 0.33, 0.33], 7, 3),
    ([0.62, 0.31, 0.07], 512, 3),
])
def test_weighted_batches_sum_to_the_scripts_batch(weights, base, world):
    # The ring's batch is the script's batch, split by share: the step
    # averages over what one machine would have, at the script's LR.
    b = ddp._weighted_batches(weights, base, world)
    assert sum(b) == base
    assert all(x >= 1 for x in b)


def test_weighted_batches_left_rank_gets_nothing():
    b = ddp._weighted_batches([0.5, 0.0, 0.5], 16, 3)
    assert b == [8, 0, 8]


def test_a_batch_smaller_than_the_ring_gives_each_rank_one():
    assert ddp._weighted_batches([0.5, 0.5], 1, 2) == [1, 1]


def test_equal_shares_keep_the_scripts_step_count():
    # 64 samples at the script's batch of 8 is 8 steps an epoch on one
    # machine; split across two ranks it is still 8, each rank taking 4.
    for rank in range(2):
        s = ddp._WeightedShardSampler(64, [0.5, 0.5], rank, False, 8, 2)
        assert s.batch == 4
        assert s.steps == 8
        assert len(s) == 32


def test_int8_blocks_never_span_tensors():
    assert ddp._int8_blocks([10, 0, 4096, 5000], block=4096) == [10, 4096, 4096, 904]
    assert sum(ddp._int8_blocks([123457, 3])) == 123460


def test_int8_round_trip_within_half_a_step_per_block():
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(0)
    # A big layer next to a small one, magnitudes 300x apart.
    flat = np.concatenate([rng.normal(0, 0.3, 9000), rng.normal(0, 1e-3, 3000)]).astype(np.float32)
    counts = ddp._int8_blocks([9000, 3000])
    q, scales, dequant = ddp._int8_encode(flat, counts)
    assert q.dtype == np.int8 and len(scales) == len(counts)
    back = ddp._int8_decode(q, scales, counts)
    assert np.array_equal(back, dequant)
    per = np.repeat(np.asarray(scales), counts)
    assert np.all(np.abs(flat - back) <= per / 2 * (1 + 1e-5) + 1e-9)
    # The small tensor keeps its own resolution, not the big one's.
    assert np.abs(flat[9000:] - back[9000:]).max() < 1e-4


def test_int8_error_feedback_delivers_small_gradients():
    # A component far below one quantisation step rounds to zero every time;
    # carried forward it arrives in full over many steps.
    np = pytest.importorskip("numpy")
    g = np.array([1.0, 0.001], dtype=np.float32)
    counts = ddp._int8_blocks([2])
    err = np.zeros_like(g)
    sent = np.zeros(2, dtype=np.float64)
    for _ in range(1000):
        x = g + err
        q, scales, dequant = ddp._int8_encode(x, counts)
        err = x - dequant
        sent += ddp._int8_decode(q, scales, counts)
    assert abs(sent[1] - 1.0) < 1.0 / 127
