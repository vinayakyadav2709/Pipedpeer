package nixgen

import (
	"testing"
)

// TestShimJoblibBackendDistributes (T7) runs an UNMODIFIED sklearn
// RandomForestClassifier(n_jobs=-1).fit() plus a plain joblib.Parallel call
// and requires the batches to reach the cluster (no multiprocessing.Pool
// anywhere in the user script). Skipped when joblib/sklearn are missing.
func TestShimJoblibBackendDistributes(t *testing.T) {
	runShimPythonEnv(t, "t7", shimFakeDaemon+`
import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib

core._ENABLED = True
core._URL = "http://127.0.0.1:%d" % PORT
core._STORE = ""

rng = np.random.RandomState(0)
X = rng.rand(200, 8)
y = (X[:, 0] * 2 + X[:, 1] > 1).astype(int)
# Two phases, asserted separately. They used to share one "MAX_CHUNK > 0" at
# the end, which is a running maximum - so the sklearn phase satisfied it and
# the lambda phase's result was never examined at all.
rf = RandomForestClassifier(n_estimators=16, n_jobs=-1, random_state=0)
rf.fit(X, y)
p = rf.predict(X[:5])
assert p.shape == (5,)
sklearn_calls = POOL_CALLS[0]
assert sklearn_calls > 0, "sklearn's joblib batches never reached the cluster"

# A lambda cannot be shipped - it has no importable definition and no source
# the shim can send - so this phase is expected to stay local. What matters is
# that it stays *correct*, and that the shim does not claim to have distributed
# it. The fake daemon ran the forest's tasks in this process, leaving the
# runner's nesting marker set, which would skip interception and pass this
# phase without testing it.
import os
os.environ.pop("PIPEDPEER_JB_NESTED", None)
before = POOL_CALLS[0]
res = joblib.Parallel(n_jobs=-1)(joblib.delayed(lambda a, b: a + b)(i, i) for i in range(12))
assert res == [2 * i for i in range(12)], res
lambda_calls = POOL_CALLS[0] - before
assert lambda_calls == 0, (
    "a lambda was shipped to the cluster (%d call(s)); it has no definition the "
    "worker could resolve, so this would have failed remotely and fallen back "
    "silently" % lambda_calls)
assert any(d["op"] == "joblib" and not d["shipped"] and "pickled" in d["why"]
           for d in stats._STATS["decisions"]), stats._STATS["decisions"]

assert MAX_CHUNK[0] > 0, "no joblib batch reached the cluster"
print("JOBLIB-OK")
`, "JOBLIB-OK",
		// Sixteen trees on 200 rows is milliseconds of work, which the cost
		// model rightly keeps local; a zero floor makes it ship anyway.
		"PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1",
		"PIPEDPEER_POOL_MIN_WORK=0")
}

// TestShimJoblibRacesInChunks pins the fix for a seven-fold slowdown: the
// old joblib backend sent one task per round trip and waited for each before
// the next, so RandomForest(n_jobs=-1) fitted one tree at a time across the
// whole cluster. A call with real work must now go out as multi-task chunks,
// come back in order, and leave a decision in the receipt; a trivial one must
// stay local.
func TestShimJoblibRacesInChunks(t *testing.T) {
	runShimPythonEnv(t, "t7b", shimFakeDaemon+`
import time
import joblib

def slow(i):
    time.sleep(0.05)
    return i * 3

before = POOL_CALLS[0]
res = joblib.Parallel(n_jobs=-1)(joblib.delayed(slow)(i) for i in range(60))
assert res == [i * 3 for i in range(60)], res
calls = POOL_CALLS[0] - before
assert calls > 0, "60 x 50ms of joblib work never reached the cluster"
assert calls < 30, "%d round trips for 60 tasks: still dispatching one at a time" % calls
assert any(d["op"] == "joblib" and d["shipped"] for d in stats._STATS["decisions"]), stats._STATS["decisions"]

# A large array every task shares travels once per chunk, in the globals
# frame, not inside each task.
os.environ.pop("PIPEDPEER_JB_NESTED", None)
import numpy as np
big = np.arange(512 * 1024, dtype=np.float64)          # 4 MB
def slow_sum(arr, i):
    time.sleep(0.05)
    return float(arr[i])
MAX_CHUNK[0] = 0
before = POOL_CALLS[0]
res = joblib.Parallel(n_jobs=-1)(joblib.delayed(slow_sum)(big, i) for i in range(60))
assert res == [float(i) for i in range(60)], res
assert POOL_CALLS[0] > before, "the shared-array call never reached the cluster"
assert 0 < MAX_CHUNK[0] < big.nbytes // 4, "a task carried the array itself: %d bytes" % MAX_CHUNK[0]

# The fake daemon runs tasks in this process, so the remote runner's nesting
# marker lands here; on a real peer it lands in that peer's worker.
os.environ.pop("PIPEDPEER_JB_NESTED", None)
# Judged by this call's own decisions, not POOL_CALLS: this fake daemon serves
# one request at a time, so chunks the previous call sent and then lost the
# race to are still being counted here, seconds later, on a loaded machine.
before = len(stats._STATS["decisions"])
res = joblib.Parallel(n_jobs=-1)(joblib.delayed(abs)(-i) for i in range(24))
assert res == list(range(24)), res
mine = stats._STATS["decisions"][before:]
assert mine and all(d["op"] == "joblib" and not d["shipped"] for d in mine), \
    "microseconds of work were shipped: %r" % mine
print("JOBLIB-RACE-OK")
`, "JOBLIB-RACE-OK", "PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}
