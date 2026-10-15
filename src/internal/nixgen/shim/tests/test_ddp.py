"""Unit tests for ddp.py's pure helpers: the batch split and the int8 codec.
The batch tests need nothing; the codec tests skip without numpy."""
import pytest

from pipedpeer_shim import ddp_wire, ddp_sampler, ddp_diloco


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
    b = ddp_sampler._weighted_batches(weights, base, world)
    assert sum(b) == base
    assert all(x >= 1 for x in b)


def test_weighted_batches_left_rank_gets_nothing():
    b = ddp_sampler._weighted_batches([0.5, 0.0, 0.5], 16, 3)
    assert b == [8, 0, 8]


def test_a_batch_smaller_than_the_ring_gives_each_rank_one():
    assert ddp_sampler._weighted_batches([0.5, 0.5], 1, 2) == [1, 1]


def test_equal_shares_keep_the_scripts_step_count():
    # 64 samples at the script's batch of 8 is 8 steps an epoch on one
    # machine; split across two ranks it is still 8, each rank taking 4.
    for rank in range(2):
        s = ddp_sampler._WeightedShardSampler(64, [0.5, 0.5], rank, False, 8, 2)
        assert s.batch == 4
        assert s.steps == 8
        assert len(s) == 32


def test_int8_blocks_never_span_tensors():
    assert ddp_wire._int8_blocks([10, 0, 4096, 5000], block=4096) == [10, 4096, 4096, 904]
    assert sum(ddp_wire._int8_blocks([123457, 3])) == 123460


def test_int8_round_trip_within_half_a_step_per_block():
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(0)
    # A big layer next to a small one, magnitudes 300x apart.
    flat = np.concatenate([rng.normal(0, 0.3, 9000), rng.normal(0, 1e-3, 3000)]).astype(np.float32)
    counts = ddp_wire._int8_blocks([9000, 3000])
    q, scales, dequant = ddp_wire._int8_encode(flat, counts)
    assert q.dtype == np.int8 and len(scales) == len(counts)
    back = ddp_wire._int8_decode(q, scales, counts)
    assert np.array_equal(back, dequant)
    per = np.repeat(np.asarray(scales), counts)
    assert np.all(np.abs(flat - back) <= per / 2 * (1 + 1e-5) + 1e-9)
    # The small tensor keeps its own resolution, not the big one's.
    assert np.abs(flat[9000:] - back[9000:]).max() < 1e-4


def test_int8_subnormal_block_gets_a_positive_scale():
    # peak/127 underflows to 0 for a subnormal peak; the daemon refuses a
    # non-positive scale (ddpreduce.go), so the block must fall back to 1.
    np = pytest.importorskip("numpy")
    flat = np.array([1e-44, 0.0, 0.5, -0.25], dtype=np.float32)
    counts = [2, 2]
    q, scales, dequant = ddp_wire._int8_encode(flat, counts)
    assert all(s > 0 for s in scales) and np.all(np.isfinite(dequant))


def test_int8_error_feedback_delivers_small_gradients():
    # A component far below one quantisation step rounds to zero every time;
    # carried forward it arrives in full over many steps.
    np = pytest.importorskip("numpy")
    g = np.array([1.0, 0.001], dtype=np.float32)
    counts = ddp_wire._int8_blocks([2])
    err = np.zeros_like(g)
    sent = np.zeros(2, dtype=np.float64)
    for _ in range(1000):
        x = g + err
        q, scales, dequant = ddp_wire._int8_encode(x, counts)
        err = x - dequant
        sent += ddp_wire._int8_decode(q, scales, counts)
    assert abs(sent[1] - 1.0) < 1.0 / 127


def test_read_frames_round_trip_and_truncation():
    import struct
    frames = [b"", b"a", b"xyz" * 1000]
    buf = b"".join(struct.pack(">I", len(f)) + f for f in frames)
    assert [bytes(m) for m in ddp_wire._read_frames(buf, 3)] == frames
    assert [bytes(m) for m in ddp_wire._read_frames(memoryview(buf), 2)] == frames[:2]
    with pytest.raises(RuntimeError):
        ddp_wire._read_frames(buf[:-1], 3)
    with pytest.raises(RuntimeError):
        ddp_wire._read_frames(buf, 4)


# DiLoCo schedule (--ddp-sync fast).

def test_fixed_interval_ignores_drift_and_may_pass_the_adaptive_ceiling(monkeypatch):
    assert ddp_diloco._DILOCO_FIXED_H == 64      # the default (DILOCO-TUNE)
    assert ddp_diloco._diloco_next_interval(64, 9.0, 1.0, 100, None) == 64
    monkeypatch.setattr(ddp_diloco, "_DILOCO_FIXED_H", 256)
    assert ddp_diloco._diloco_next_interval(2, 0.0, 1.0, 100, None) == 256
    assert ddp_diloco._diloco_next_interval(256, 0.0, 1.0, 150, 200) == 50   # never past the end


def test_interval_follows_the_ring_size(monkeypatch):
    # DILOCO-MANY: each row's H up to its rank count, the last H past it
    # (not validated), never a longer interval for a bigger ring.
    monkeypatch.setattr(ddp_diloco, "_DILOCO_ENV_H", None)
    for up_to, h in ddp_diloco._DILOCO_RANKS:
        assert ddp_diloco._diloco_every(up_to) == (h, True)
    last_n, last_h = ddp_diloco._DILOCO_RANKS[-1]
    assert ddp_diloco._diloco_every(2) == (ddp_diloco._DILOCO_RANKS[0][1], True)
    assert ddp_diloco._diloco_every(last_n + 1) == (last_h, False)
    hs = [h for _, h in ddp_diloco._DILOCO_RANKS]
    assert hs == sorted(hs, reverse=True)
    monkeypatch.setattr(ddp_diloco, "_DILOCO_ENV_H", 256)   # PIPEDPEER_DILOCO_H wins
    assert ddp_diloco._diloco_every(last_n + 5) == (256, True)
    monkeypatch.setattr(ddp_diloco, "_DILOCO_FIXED_H", 64)
    monkeypatch.setattr(ddp_diloco, "_DILOCO_H_START", 64)
    ddp_diloco._diloco_use_every(8)
    assert ddp_diloco._DILOCO_H_START == 8
    assert ddp_diloco._diloco_next_interval(2, 0.0, 1.0, 100, None) == 8
    ddp_diloco._diloco_use_every(0)                          # adaptive starts at 2
    assert ddp_diloco._DILOCO_H_START == 2


def test_interval_grows_on_low_drift_and_shrinks_on_high(monkeypatch):
    monkeypatch.setattr(ddp_diloco, "_DILOCO_FIXED_H", 0)    # PIPEDPEER_DILOCO_H=0: adaptive
    assert ddp_diloco._diloco_next_interval(4, 0.1, 1.0, 100, None) == 8
    assert ddp_diloco._diloco_next_interval(4, 2.0, 1.0, 100, None) == 2
    assert ddp_diloco._diloco_next_interval(4, 0.85, 1.0, 100, None) == 4   # dead band
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 100, None) == ddp_diloco._DILOCO_H_MAX
    assert ddp_diloco._diloco_next_interval(2, 9.0, 1.0, 100, None) == ddp_diloco._DILOCO_H_FLOOR
    # Not measured (a state round, an older daemon): unchanged.
    assert ddp_diloco._diloco_next_interval(8, None, 1.0, 100, None) == 8
    assert ddp_diloco._diloco_next_interval(8, 0.1, None, 100, None) == 8


def test_interval_tightens_over_the_last_tenth():
    total = 1000
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 800, total) == 64
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 950, total) == 32
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 995, total) == 3
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 999, total) == 1
    assert ddp_diloco._diloco_cap(999, total) == 1


def test_interval_never_runs_past_the_end():
    # A short run whose tenth is under 64 steps: H stops at the last step.
    assert ddp_diloco._diloco_next_interval(64, 0.0, 1.0, 150, 200) == 50
    n, h = 150, 64
    while ddp_diloco._diloco_cap(n, 200) > 1:
        h = ddp_diloco._diloco_next_interval(h, 0.0, 1.0, n, 200)
        n += h
        assert n <= 200


def test_no_tightening_without_a_run_length_is_said():
    assert ddp_diloco._diloco_cap(10 ** 6, None) is None
    assert "no tightening" in ddp_diloco._diloco_note(None)
    assert "tightens" in ddp_diloco._diloco_note(1000)
    assert ddp_diloco._diloco_warmup(None) == 10
    assert ddp_diloco._diloco_warmup(1000) == 100
    assert ddp_diloco._diloco_warmup(30) == 10


def test_outer_step_matches_torch_nesterov():
    np = pytest.importorskip("numpy")
    torch = pytest.importorskip("torch")
    rng = np.random.default_rng(0)
    theta = rng.standard_normal(5).astype(np.float32)
    p = torch.nn.Parameter(torch.from_numpy(theta.copy()))
    ref = torch.optim.SGD([p], lr=0.7, momentum=0.9, nesterov=True)
    buf = None
    for _ in range(4):
        delta = rng.standard_normal(5).astype(np.float32)
        theta, buf = ddp_diloco._outer_nesterov(theta, buf, delta, lr=0.7, mu=0.9)
        p.grad = torch.from_numpy(delta.copy())
        ref.step()
        np.testing.assert_allclose(theta, p.detach().numpy(), rtol=1e-5, atol=1e-6)


def _adam_ring():
    torch = pytest.importorskip("torch")
    pytest.importorskip("numpy")
    torch.manual_seed(0)
    model = torch.nn.Linear(3, 2)
    opt = torch.optim.Adam(model.parameters(), lr=0.01)
    params = [p for g in opt.param_groups for p in g["params"]]
    return torch, model, opt, params


def _local_step(torch, model, opt):
    x = torch.randn(4, 3)
    model(x).pow(2).mean().backward()
    opt.step()
    opt.zero_grad()


def test_round_applies_the_outer_step_and_keeps_the_lr():
    torch, model, opt, params = _adam_ring()
    import numpy as np
    st = ddp_diloco._DiLoCo()
    st.begin(params, 2, 0)
    start = [t.copy() for t in st.theta]
    _local_step(torch, model, opt)
    st.samples = 4
    seen = []

    def reduce(deltas, extras, samples):
        seen.append(samples)
        return [d.copy() for d in deltas + extras], 0.5   # one rank: its own mean

    drift = ddp_diloco._diloco_round(st, opt, params, reduce)
    assert drift == 0.5 and seen == [4] and st.samples == 0
    for p, t0 in zip(params, start):
        local = p.detach().numpy()
        # The snapshot did not move with training, and theta was applied.
        assert not np.allclose(t0, local)
    for p, t0, b in zip(params, start, st.buf):
        delta = b  # first outer step: buf = delta
        want = t0 - ddp_diloco._DILOCO_OUTER_LR * (delta + ddp_diloco._DILOCO_MOMENTUM * delta)
        np.testing.assert_allclose(p.detach().numpy(), want, rtol=1e-5, atol=1e-6)
    assert opt.param_groups[0]["lr"] == 0.01


def test_optimizer_state_rides_with_the_weights_every_k():
    torch, model, opt, params = _adam_ring()
    st = ddp_diloco._DiLoCo()
    st.begin(params, 2, 0)
    calls = []

    def reduce(deltas, extras, samples):
        calls.append((len(deltas), len(extras)))
        return [d.copy() for d in deltas + extras], None

    k = ddp_diloco._DILOCO_STATE_EVERY
    for _ in range(2 * k):
        _local_step(torch, model, opt)
        ddp_diloco._diloco_round(st, opt, params, reduce)
    assert len(calls) == 2 * k                  # one round per outer step
    with_state = [i + 1 for i, (_, e) in enumerate(calls) if e]
    assert with_state == [k, 2 * k]
    # Adam: exp_avg and exp_avg_sq for each parameter, in the same call as
    # the pseudo-gradients.
    assert calls[k - 1] == (len(params), 2 * len(params))
    # Forced (the end of the run, a ring change): state now.
    calls.clear()
    ddp_diloco._diloco_round(st, opt, params, reduce, force_state=True)
    assert calls == [(len(params), 2 * len(params))]
    assert opt.param_groups[0]["lr"] == 0.01


def test_averaged_moments_are_written_back():
    torch, model, opt, params = _adam_ring()
    st = ddp_diloco._DiLoCo()
    st.begin(params, 2, 0)
    _local_step(torch, model, opt)

    def reduce(deltas, extras, samples):
        return [d.copy() for d in deltas] + [e * 0 + 0.25 for e in extras], None

    ddp_diloco._diloco_round(st, opt, params, reduce, force_state=True)
    for p in params:
        assert torch.all(opt.state[p]["exp_avg"] == 0.25)
        assert torch.all(opt.state[p]["exp_avg_sq"] == 0.25)


# U26: butterfly shards, the background sync thread, delayed gradients.

@pytest.mark.parametrize("sizes,n", [([10000, 5, 9000], 3), ([4096 * 7], 4), ([1, 1, 1], 3)])
def test_shard_cuts_cover_the_vector_on_block_boundaries(sizes, n):
    counts = ddp_wire._int8_blocks(sizes)
    cuts = ddp_wire._shard_cuts(counts, n)
    assert len(cuts) == n
    assert cuts[0][0] == 0 and cuts[-1][1] == sum(sizes)
    for (lo, hi, blo, bhi), nxt in zip(cuts, cuts[1:] + [None]):
        assert hi > lo and bhi > blo                      # never an empty shard
        assert hi - lo == sum(counts[blo:bhi])            # whole blocks only
        if nxt is not None:
            assert nxt[0] == hi and nxt[2] == bhi


def test_shard_cuts_refuse_fewer_blocks_than_shards():
    assert ddp_wire._shard_cuts(ddp_wire._int8_blocks([5]), 3) is None
    assert ddp_wire._shard_cuts(ddp_wire._int8_blocks([5, 5, 5]), 1) is None


def test_sync_thread_runs_in_order_and_drain_keeps_results():
    import threading
    th = ddp_wire._SyncThread()
    seen, gate = [], threading.Event()
    first = th.submit(lambda: (gate.wait(5), seen.append(1), "a")[2])
    second = th.submit(lambda: (seen.append(2), threading.current_thread().name)[1])
    gate.set()
    th.drain()
    assert seen == [1, 2]
    assert first.result() == "a"
    assert second.result().startswith(ddp_wire._SyncThread.NAME)
    # drain on the thread itself must not wait on itself
    assert th.submit(th.drain).result(timeout=5) is None


def test_delayed_grads_apply_step_t_at_t_plus_1_exactly_once():
    th = ddp_wire._SyncThread()
    d = ddp_wire._DelayedGrads(th)
    applied = []
    for t in range(5):
        prev = d.swap(["p"], ["g%d" % t], lambda gs: [g + "-avg" for g in gs])
        if prev is not None:
            applied.append(prev[1][0])
    last = d.take()                     # the pass ends: flush
    applied.append(last[1][0])
    assert applied == ["g%d-avg" % t for t in range(5)]
    assert d.take() is None             # nothing applied twice


def test_delayed_grads_surface_a_sync_error_at_the_next_step():
    d = ddp_wire._DelayedGrads(ddp_wire._SyncThread())

    def boom(_):
        raise RuntimeError("ddp sync refused")
    d.swap(["p"], ["g"], boom)
    with pytest.raises(RuntimeError, match="refused"):
        d.take()


def test_pass_end_fires_once_and_passes_through():
    fired = []
    it = ddp_wire._PassEnd(iter([1, 2]), lambda: fired.append(1))
    assert list(it) == [1, 2]
    with pytest.raises(StopIteration):
        next(it)
    assert fired == [1]


@pytest.mark.parametrize("n,weights,base", [
    (70, [0.5, 0.5], 8),        # 6 left: 3 + 3
    (65, [0.5, 0.5], 8),        # 1 left: one rank pads
    (4001, [1 / 3] * 3, 32),    # 1 left over three ranks: two pad
    (1003, [0.7, 0.2, 0.1], 32),
    (5, [0.5, 0.5], 8),         # less than one step: a single partial one
])
def test_every_sample_once_including_the_last_partial_batch(n, weights, base):
    pytest.importorskip("numpy")   # the sampler's assignment is vectorised
    seen, total, pads = [], set(), 0
    for rank in range(len(weights)):
        s = ddp_sampler._WeightedShardSampler(n, weights, rank, False, base, len(weights))
        idx = list(s)
        assert len(idx) == len(s)
        if s.pad:
            pads += 1
            idx = idx[:-1]          # the repeat, weighed as nothing
        assert s.tail <= s.batch    # the remainder stays one batch
        seen.extend(idx)
        total.add(s.total_steps)
    assert sorted(seen) == list(range(n)), "not every sample exactly once"
    assert total == {-(-n // base)}, "steps differ from one machine's %d" % -(-n // base)
    left = n % base
    assert pads == max(0, len(weights) - left) if left else pads == 0


def test_drop_last_drops_like_the_script():
    s = ddp_sampler._WeightedShardSampler(70, [0.5, 0.5], 0, False, 8, 2, drop_last=True)
    assert s.total_steps == 8 and s.tail == 0 and not s.pad and len(s) == 32


def test_syncs_land_on_the_epoch_end():
    # 29 steps an epoch: after step 20 of it at most 9 more, so the epoch's
    # last step (and with it the run's) is a sync even with no run length.
    assert ddp_diloco._diloco_epoch_cap(16, 20, 29) == 9
    assert ddp_diloco._diloco_epoch_cap(4, 20, 29) == 4
    assert ddp_diloco._diloco_epoch_cap(64, 29, 29) == 29   # the step that ended one
    assert ddp_diloco._diloco_epoch_cap(8, 28, 29) == 1
    assert ddp_diloco._diloco_epoch_cap(8, 3, None) == 8
    n, h, ends = 0, 2, []
    while n < 29 * 3:
        h = ddp_diloco._diloco_epoch_cap(min(64, h * 2), n % 29 if n % 29 else (29 if n else 0), 29)
        n += h
        ends.append(n)
    assert 29 in ends and 58 in ends and 87 in ends


@pytest.mark.parametrize("env,overlap,want", [
    (None, False, "auto"), ("", False, "auto"),     # --ddp-sync unset
    ("exact", False, "exact"), (" FAST ", False, "fast"),
    ("", True, "exact"),                            # overlap never switches
    ("fast", True, "fast"),
])
def test_only_an_unset_mode_lets_the_lead_switch_to_diloco(env, overlap, want):
    assert ddp_diloco._sync_mode(env, overlap) == want
