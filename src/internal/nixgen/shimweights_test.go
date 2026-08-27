package nixgen

import "testing"

// TestWeightedShardsPartitionTheDataset.
//
// Contiguous, not strided. Striding by rank only gives disjoint, exhaustive
// shards when every rank takes the same stride - which is exactly what stops
// being true with measured shares. Rounding must lose nor duplicate a sample:
// a sample trained on twice per epoch, or never, is a silently different
// dataset.
func TestWeightedShardsPartitionTheDataset(t *testing.T) {
	runShimPython(t, "wshard", `
import sitecustomize as shim

n = 1000
weights = [0.5, 0.3, 0.2]
seen = []
sizes = []
steps = []
for rank in range(3):
    s = shim._WeightedShardSampler(n, weights, rank, False, 100, 3)
    idx = list(s)
    sizes.append(len(idx))
    steps.append(s.steps)
    assert len(idx) == len(s), "len() disagrees with what it yields"
    seen.extend(idx)

# Disjoint, and covering everything but the sub-global-batch remainder that
# drop_last discards in any DDP setup.
assert len(set(seen)) == len(seen), "shards overlap"
assert len(seen) > n - sum(shim._weighted_batches(weights, 100, 3)), (
    "more than one global batch was dropped: %d of %d" % (len(seen), n))
assert len(set(steps)) == 1, "ranks run different step counts: %s" % steps
# Shares are followed: 5:3:2 within a sample of rounding.
assert sizes[0] > sizes[1] > sizes[2], "sizes %s do not follow the shares" % sizes
assert abs(sizes[0] / sizes[2] - 2.5) < 0.15, "ratio off: %s" % sizes
print("WSHARD-OK")
`, "WSHARD-OK")
}

// TestEqualStepsAcrossUnevenShares.
//
// The invariant that a live run caught being broken: measured 62/31/7 shares
// gave 40, 40 and 39 steps, because shard and batch were each rounded on
// their own. Every step ends at a barrier, so the short rank leaves the
// others averaging without it — the daemon tolerates that, which is why this
// does not hang and instead makes the last step of every epoch quietly short
// a rank.
func TestEqualStepsAcrossUnevenShares(t *testing.T) {
	runShimPython(t, "wround", `
import sitecustomize as shim

# The first entry is the case a live run failed on, verbatim: 60000 samples,
# a 512 batch, and the shares placement measured for a 16/8/2-core ring. It
# produced 40, 40 and 39 steps.
cases = [(60000, [0.62, 0.31, 0.07], 512)]
for n in (500, 1000, 4001, 60000):
    for weights in ([1/3, 1/3, 1/3], [0.7, 0.2, 0.1], [0.45, 0.45, 0.10]):
        cases.append((n, weights, 32))

for n, weights, base in cases:
    if True:
        seen = []
        steps = []
        for rank in range(3):
            s = shim._WeightedShardSampler(n, weights, rank, False, base, 3)
            seen.extend(list(s))
            steps.append(s.steps)
        assert len(set(seen)) == len(seen), (
            "n=%d weights=%s duplicated samples" % (n, weights))
        assert len(set(steps)) == 1, (
            "n=%d weights=%s gave step counts %s - every step ends at a "
            "barrier, so ranks that finish early are left waiting"
            % (n, weights, steps))
        dropped = n - len(seen)
        globalbatch = sum(shim._weighted_batches(weights, base, 3))
        assert dropped < globalbatch, (
            "n=%d weights=%s dropped %d samples, more than one global batch "
            "of %d" % (n, weights, dropped, globalbatch))
print("WROUND-OK")
`, "WROUND-OK")
}

// TestShuffledShardsStayDisjoint. Every rank permutes the dataset the same
// way and then takes its own slice; a per-rank seed would give two ranks the
// same sample in one epoch and drop another entirely.
func TestShuffledShardsStayDisjoint(t *testing.T) {
	runShimPython(t, "wshuffle", `
import sitecustomize as shim

n = 500
weights = [0.6, 0.4]
for epoch in (0, 1, 7):
    seen = []
    for rank in range(2):
        s = shim._WeightedShardSampler(n, weights, rank, True, 50, 2)
        s.set_epoch(epoch)
        seen.extend(list(s))
    assert len(set(seen)) == len(seen), "epoch %d shards overlap" % epoch

# A different epoch must actually reshuffle, or set_epoch is decoration.
a = shim._WeightedShardSampler(n, weights, 0, True, 50, 2); a.set_epoch(0)
b = shim._WeightedShardSampler(n, weights, 0, True, 50, 2); b.set_epoch(1)
assert list(a) != list(b), "set_epoch did not change the order"
print("WSHUFFLE-OK")
`, "WSHUFFLE-OK")
}

// TestEqualSharesAreTreatedAsNoShares, so every existing ring keeps the code
// path it has always run rather than arithmetic that happens to reduce to it.
func TestEqualSharesAreTreatedAsNoShares(t *testing.T) {
	runShimPython(t, "wparse", `
import sitecustomize as shim

assert shim._parse_weights("", 2) is None
assert shim._parse_weights("0.5,0.5", 2) is None, "exactly even should be no-op"
assert shim._parse_weights("0.505,0.495", 2) is None, "within noise should be no-op"
assert shim._parse_weights("0.5,0.5", 1) is None, "a ring of one has no shares"

w = shim._parse_weights("0.75,0.25", 2)
assert w is not None and abs(w[0] - 0.75) < 1e-9, w

# Unnormalised input is normalised rather than rejected.
w = shim._parse_weights("3,1", 2)
assert w is not None and abs(w[0] - 0.75) < 1e-9, w

# Nonsense is refused, never guessed at.
assert shim._parse_weights("0.5", 2) is None, "wrong count accepted"
assert shim._parse_weights("0.5,0", 2) is None, "zero share accepted"
assert shim._parse_weights("0.5,-0.5", 2) is None, "negative share accepted"
assert shim._parse_weights("half,rest", 2) is None, "non-numeric accepted"
print("WPARSE-OK")
`, "WPARSE-OK")
}

// TestAShareTooSmallForOneSampleIsRefused rather than producing an empty
// shard, which would have that rank contribute a gradient over nothing.
func TestAShareTooSmallForOneSampleIsRefused(t *testing.T) {
	runShimPython(t, "wtiny", `
import sitecustomize as shim

try:
    shim._WeightedShardSampler(10, [0.99, 0.01], 1, False, 512, 2)
except ValueError as e:
    assert "do not make one step" in str(e), e
else:
    raise AssertionError("a dataset too small for one ring step was accepted")
print("WTINY-OK")
`, "WTINY-OK")
}
