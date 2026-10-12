package nixgen

import "testing"

// TestHashOwnersMatchesTheGreedyLoop pins the vectorised _hash_owners to the
// plain greedy loop it replaced (kept here as the reference), so the speed-up
// cannot change which rank owns a sample - every rank computes owners on its
// own and they must agree, and a leaving rank must still move only its
// samples (TestALeavingRankMovesOnlyItsOwnSamples). It also bounds the cost:
// a million samples used to build a per-sample preference list in Python.
func TestHashOwnersMatchesTheGreedyLoop(t *testing.T) {
	runShimPython(t, "hashowners", `
import random, time
import numpy as np
import sitecustomize as shim

def reference(n, weights, quotas, order=None):
    live = [r for r, w in enumerate(weights) if w > 0 and quotas[r] > 0]
    owners = [[] for _ in weights]
    if not live:
        return owners
    idx = np.arange(n, dtype=np.uint64)
    scores = np.stack([shim._hash_score(idx, r, weights[r]) for r in live])
    prefs = np.argsort(-scores, axis=0).T.tolist()
    room = [quotas[r] for r in live]
    left = sum(room)
    for s in (order if order is not None else range(n)):
        if left == 0:
            break
        for j in prefs[s]:
            if room[j]:
                room[j] -= 1
                left -= 1
                owners[live[j]].append(s)
                break
    return owners

cases = [
    (10007, [1/2, 1/2], 64),
    (10007, [1/3, 1/3, 1/3], 50),
    (9000, [0.25] * 4, 32),
    (12345, [0.5, 0.3, 0.2], 40),
    (8000, [0.5, 0.0, 0.5], 30),
    (5000, [0.7, 0.3], 1),
    (100, [0.5, 0.5], 50),
]
for n, weights, base in cases:
    batches = shim._weighted_batches(weights, base, len(weights))
    steps = n // sum(batches)
    quotas = [steps * b for b in batches]
    for shuffled in (False, True):
        order = None
        if shuffled:
            order = list(range(n))
            random.Random(n).shuffle(order)
        got = shim._hash_owners(n, weights, quotas, order)
        want = reference(n, weights, quotas, order)
        assert got == want, (n, weights, base, shuffled)
        assert [len(o) for o in got] == [q if w > 0 else 0 for q, w in zip(quotas, weights)]

n = 1_000_000
w = [0.25] * 4
quotas = [n // 4 - 100] * 4
order = np.random.default_rng(1).permutation(n).tolist()
t0 = time.monotonic()
got = shim._hash_owners(n, w, quotas, order)
took = time.monotonic() - t0
assert [len(o) for o in got] == quotas
print("1M samples x 4 ranks: %.2fs" % took)
assert took < 15, took
print("HASHOWNERS-OK")
`, "HASHOWNERS-OK")
}
