package nixgen

import (
	"strings"
	"testing"
)

// Per-item submission: executor.submit and Pool.apply_async used to run
// local only, so a loop of them never left the machine. They now queue and
// go out as one starmap; these tests hold them to the same provenance bar as
// Pool.map (assertDistributed), plus the futures semantics a batch must keep.

// TestShimExecutorSubmitLoopSpreads: a submit loop is distributed and
// as_completed yields every future with its own value.
func TestShimExecutorSubmitLoopSpreads(t *testing.T) {
	script := `
import concurrent.futures as cf
import time

def work(x):
    time.sleep(0.02)
    return x * x + 1

if __name__ == "__main__":
    with cf.ProcessPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(work, i): i for i in range(64)}
        seen = {}
        for f in cf.as_completed(futs, timeout=120):
            seen[futs[f]] = f.result()
    assert seen == {i: i * i + 1 for i in range(64)}, sorted(seen.items())[:8]
    print("SUBMIT-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	assertDistributed(t, res, "SUBMIT-OK")
}

// TestShimExecutorSubmitErrorStaysOnItsFuture: one call raising must fail
// only its own future, as with the stock executor - not the whole batch.
func TestShimExecutorSubmitErrorStaysOnItsFuture(t *testing.T) {
	script := `
import concurrent.futures as cf
import time

def work(x):
    time.sleep(0.02)
    if x == 13:
        raise ValueError("bad item %d" % x)
    return x * 2

if __name__ == "__main__":
    with cf.ProcessPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(work, i) for i in range(48)]
        cf.wait(futs, timeout=120)
    for i, f in enumerate(futs):
        if i == 13:
            e = f.exception()
            assert isinstance(e, ValueError) and "bad item 13" in str(e), repr(e)
        else:
            assert f.exception() is None, (i, f.exception())
            assert f.result() == i * 2, (i, f.result())
    print("SUBMIT-ERR-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	if !strings.Contains(res.stdout, "SUBMIT-ERR-OK") {
		t.Fatalf("missing sentinel\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
}

// TestShimPoolApplyAsyncSpreadsWithCallbacks: apply_async in a loop is
// distributed, callback and error_callback fire once per call before get()
// returns, and the close/join/get pattern still completes.
func TestShimPoolApplyAsyncSpreadsWithCallbacks(t *testing.T) {
	script := `
import multiprocessing, time

def work(x):
    time.sleep(0.02)
    return x + 100

if __name__ == "__main__":
    got = []
    pool = multiprocessing.Pool(4)
    rs = [pool.apply_async(work, (i,), callback=got.append) for i in range(64)]
    pool.close()
    pool.join()
    assert [r.get(timeout=120) for r in rs] == [i + 100 for i in range(64)]
    assert sorted(got) == [i + 100 for i in range(64)], sorted(got)[:8]
    assert all(r.ready() and r.successful() for r in rs)
    assert pool.__class__.__name__ == "_ClusterPool"
    print("APPLY-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	assertDistributed(t, res, "APPLY-OK")

	errScript := `
import multiprocessing, time

def work(x):
    if x == 3:
        raise KeyError(x)
    return x

if __name__ == "__main__":
    ok, bad = [], []
    with multiprocessing.Pool(2) as pool:
        rs = [pool.apply_async(work, (i,), callback=ok.append, error_callback=bad.append)
              for i in range(8)]
        for r in rs:
            r.wait(60)
        assert pool.apply(work, (5,)) == 5
    assert sorted(ok) == [0, 1, 2, 4, 5, 6, 7], ok
    assert len(bad) == 1 and isinstance(bad[0], KeyError), bad
    assert not rs[3].successful()
    try:
        rs[3].get()
        raise SystemExit("get() did not raise")
    except KeyError:
        pass
    print("APPLY-ERR-OK")
`
	res = runPoolScript(t, map[string]string{"job.py": errScript}, "job.py")
	if !strings.Contains(res.stdout, "APPLY-ERR-OK") {
		t.Fatalf("missing sentinel\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
}

// TestShimCategoricalGroupbyMatchesLocal: categorical keys used to keep a
// groupby local. They now shuffle, with partials run observed=True and the
// unobserved categories restored when the call's observed is False.
func TestShimCategoricalGroupbyMatchesLocal(t *testing.T) {
	runShimPython(t, "catgb", shimFakeDaemon+`
import pandas as pd
import numpy as np
os.environ["PIPEDPEER_SPILL_MIN"] = "1"
shim._FORCE = True
n = 3000
rng = np.random.default_rng(7)
cats = pd.Categorical(rng.choice(list("abcde"), n), categories=list("abcdefg"))
df = pd.DataFrame({"c": cats, "k": rng.integers(0, 4, n), "a": rng.standard_normal(n),
                   "b": rng.integers(0, 100, n)})
assert shim._groupby_gate(df.groupby("c", observed=False)), "gate refused a categorical key"
for obs in (True, False):
    for spec in ("sum", "mean", "count", {"a": ["sum", "mean"], "b": "max"}):
        gb = df.groupby("c", observed=obs)
        pd.testing.assert_frame_equal(shim._groupby_shuffle(gb, spec, (), {}), gb.agg(spec))
    for keys in ("c", ["c", "k"], ["k", "c"]):
        gb = df.groupby(keys, observed=obs)
        pd.testing.assert_frame_equal(shim._groupby_shuffle(gb, "sum", (), {}), gb.agg("sum"))
        pd.testing.assert_series_equal(shim._groupby_shuffle(gb, "size", (), {}), gb.agg("size"))
        gb = df.groupby(keys, observed=obs, as_index=False)
        pd.testing.assert_frame_equal(shim._groupby_shuffle(gb, "sum", (), {}), gb.agg("sum"))
# The call's own observed wins; unset means the pandas default.
assert shim._groupby_observed(df.groupby("c", observed=False)) is False
assert shim._groupby_observed(df.groupby("c", observed=True)) is True
print("CATGB-OK")
`, "CATGB-OK")
}
