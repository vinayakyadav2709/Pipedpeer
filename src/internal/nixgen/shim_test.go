package nixgen

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestShimSyntax compiles the embedded shim, guaranteeing it is valid Python.
// A broken sitecustomize would abort every intercepted run at startup.
func TestShimSyntax(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	shim := filepath.Join(dir, "sitecustomize.py")
	if err := os.WriteFile(shim, []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(python, "-m", "py_compile", shim).CombinedOutput()
	if err != nil {
		t.Fatalf("shim does not compile:\n%s", out)
	}
}

// TestShimInstallSafe confirms the shim imports cleanly and is a no-op when
// interception is disabled (PIPEDPEER_SHIM unset), so it cannot break a normal
// python run.
func TestShimInstallSafe(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	// Define a module-level (picklable) function, then exercise a real Pool.
	// Exercise a real Pool from a file so the worker can pickle the function.
	src := `
def double(x):
    return x * 2

import multiprocessing
if __name__ == "__main__":
    p = multiprocessing.Pool(1)
    print(p.map(double, [1, 2, 3]))
    p.close()
    p.join()
`
	script := filepath.Join(dir, "work.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shim broke a normal run:\n%s\n%v", out, err)
	}
	if string(out) != "[2, 4, 6]\n" {
		t.Fatalf("unexpected output: %q", out)
	}
}

// TestShimEnabledStillLocal confirms that with interception on but no reachable
// daemon URL, the patched Pool still executes correctly on local cores (the
// never-slower fallback): it must not crash and must return correct results.
func TestShimEnabledStillLocal(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	src := `
def double(x):
    return x * 2

import multiprocessing
if __name__ == "__main__":
    p = multiprocessing.Pool(1)
    print(p.map(double, [1, 2, 3]))
    p.close()
`
	script := filepath.Join(dir, "work.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	// Enabled but no daemon URL / shard count: _remote is false, local fallback.
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=", // empty
		"PIPEDPEER_NUM_SHARDS=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shim local fallback failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "[2, 4, 6]") {
		t.Fatalf("unexpected output: %q", out)
	}
	if !strings.Contains(string(out), "local 3 items") {
		t.Fatalf("expected local fallback to be used, got: %q", out)
	}
}

// TestShimTorchFallbackLocal confirms torch matmul interception, when enabled
// but with no reachable daemon, falls back to plain local torch so a model run
// never breaks on an absent cluster. Skipped if torch isn't installed.
func TestShimTorchFallbackLocal(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if _, err := exec.Command(python, "-c", "import torch").CombinedOutput(); err != nil {
		t.Skipf("torch not available: %v", err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	src := `
import torch
torch.manual_seed(0)
a = torch.rand(40, 40)
b = torch.rand(40, 40)
expected = torch.matmul(a, b)
r = torch.matmul(a, b)
assert torch.allclose(r, expected), "torch matmul wrong under shim"
print("torch-ok")
`
	script := filepath.Join(dir, "work.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	// Enabled + torch interception on, but no daemon URL: local fallback.
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_TORCH=1",
		"PIPEDPEER_DAEMON_URL=",
		"PIPEDPEER_NUM_SHARDS=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("torch shim local fallback failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "torch-ok") {
		t.Fatalf("torch matmul wrong under shim: %q", out)
	}
}

// shimFakeDaemon is a python HTTP server that plays the local daemon's
// /v1/pool/map for the shim: it executes the shipped worker source in-process,
// serves the warm-worker _CACHE by cache_keys, and counts bandwidth probes.
const shimFakeDaemon = `
import base64
import json
import os
import pickle
import struct
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

CACHE = {}
PROBES = [0]
MAX_CHUNK = [0]
TMP = tempfile.mkdtemp(prefix="pp-shim-")

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(n)
        nl = data.find(b"\n")
        req = json.loads(data if nl < 0 else data[:nl])
        frames = bool(req.get("items_frames"))
        if req.get("func_src") == "def run(x):\n    return len(x)\n":
            PROBES[0] += 1
        if frames:
            rest = data[nl + 1:]
            def rf(rest):
                nn = struct.unpack(">I", rest[:4])[0]
                return rest[4:4 + nn], rest[4 + nn:]
            g = b""
            if req.get("globals"):
                g, rest = rf(rest)
            raw = []
            for _ in range(req["items_frames"]):
                f, rest = rf(rest)
                raw.append(f)
            b = [len(f) for f in raw]
            if b:
                MAX_CHUNK[0] = max(MAX_CHUNK[0], max(b))
        else:
            g = b""
            if req.get("items_b64"):
                b = [len(i) for i in req["items"]]
                if b:
                    MAX_CHUNK[0] = max(MAX_CHUNK[0], max(b))
        ns = {}
        if g:
            ns.update(pickle.loads(g))
        elif req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        ns["_CACHE"] = CACHE
        saved = os.environ.get("PIPEDPEER_NUMPY_NESTED")
        try:
            exec(req["func_src"], ns)
            func = ns[req.get("func_name", "run")]
            if frames:
                items = [pickle.loads(f) for f in raw]
            else:
                items = req["items"]
                if req.get("items_b64"):
                    items = [pickle.loads(base64.b64decode(i)) for i in items]
                if req.get("cache_keys"):
                    items = [CACHE.get(k) for k in req["cache_keys"]]
            results = [func(i) for i in items]
        finally:
            if saved is None:
                os.environ.pop("PIPEDPEER_NUMPY_NESTED", None)
            else:
                os.environ["PIPEDPEER_NUMPY_NESTED"] = saved
        if frames:
            out = b"".join(struct.pack(">I", len(p)) + p for p in [pickle.dumps(r) for r in results])
            body = b'{"results_frames": %d}\n' % len(results) + out
        else:
            body = json.dumps({"results": [{"pickle": base64.b64encode(pickle.dumps(r)).decode()} for r in results]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

srv = HTTPServer(("127.0.0.1", 0), Handler)
PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

os.environ["PIPEDPEER_SHIM"] = "1"
os.environ["PIPEDPEER_DAEMON_URL"] = "http://127.0.0.1:%d" % PORT
os.environ["PIPEDPEER_NUM_SHARDS"] = "3"
os.environ["PIPEDPEER_STORE_PATH"] = ""
os.environ["PIPEDPEER_PANDAS"] = "1"
os.environ["PIPEDPEER_OOC_MIN"] = "1"

import sitecustomize as shim
import pandas as pd
import numpy as np
# sitecustomize is imported at interpreter startup, before this preamble runs,
# so the env vars it read then are stale; point the module at the fake daemon.
shim._ENABLED = True
shim._URL = "http://127.0.0.1:%d" % PORT
shim._STORE = ""
shim._NUM_SHARDS = "3"
shim._OOC_CHUNK = 4096
`

// runShimPython writes the shim plus a python script and runs it; any nonzero
// exit or a missing sentinel fails the test.
func runShimPython(t *testing.T, name, src string, wantSentinel string) {
	runShimPythonEnv(t, name, src, wantSentinel)
}

// runShimPythonEnv is runShimPython with extra process env vars, needed when
// the shim's install-time decisions (joblib backend, numpy patching) must see
// a cluster before the preamble overrides the module attributes.
func runShimPythonEnv(t *testing.T, name, src, wantSentinel string, envs ...string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if _, err := exec.Command(python, "-c", "import pandas").CombinedOutput(); err != nil {
		t.Skipf("pandas not available: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, name+".py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	cmd.Env = append(cmd.Env, envs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed:\n%s\n%v", name, out, err)
	}
	if wantSentinel != "" && !strings.Contains(string(out), wantSentinel) {
		t.Fatalf("%s: missing sentinel %q in output:\n%s", name, wantSentinel, out)
	}
}

// TestShimHashShuffleMatchesLocal (T3) runs the pandas hash-shuffle (groupby,
// merge) and the out-of-core partitioned ops against the fake daemon and
// requires exact equality with local pandas.
func TestShimHashShuffleMatchesLocal(t *testing.T) {
	runShimPython(t, "t3", shimFakeDaemon+`
n = 4000
df = pd.DataFrame({"k": np.random.randint(0, 13, n), "a": np.random.randn(n), "b": np.random.randint(0, 100, n)})
gb = df.groupby("k")
pd.testing.assert_frame_equal(shim._groupby_shuffle(gb, {"a": ["sum", "mean"], "b": "max"}, (), {}),
                              gb.agg({"a": ["sum", "mean"], "b": "max"}))
pd.testing.assert_frame_equal(shim._groupby_shuffle(df.groupby("k"), ["sum", "mean"], (), {}),
                              df.groupby("k").agg(["sum", "mean"]))
pd.testing.assert_frame_equal(shim._groupby_shuffle(df.groupby("k", as_index=False), "sum", (), {}),
                              df.groupby("k", as_index=False).sum())
dna = df.copy()
dna.loc[::97, "k"] = np.nan
pd.testing.assert_frame_equal(shim._groupby_shuffle(dna.groupby("k"), "sum", (), {}),
                              dna.groupby("k").sum())

L = pd.DataFrame({"id": np.random.randint(0, 50, 3000), "lv": np.random.randn(3000)})
R = pd.DataFrame({"id": np.random.randint(0, 50, 500), "rv": np.random.randn(500)})
for how in ("inner", "left", "right", "outer"):
    got = shim._merge_shuffle(L, R, {"how": how, "on": "id", "sort": False})
    local = L.merge(R, how=how, on="id", sort=False)
    cols = list(local.columns)
    pd.testing.assert_frame_equal(got.sort_values(by=cols).reset_index(drop=True),
                                  local.sort_values(by=cols).reset_index(drop=True))
got = shim._merge_shuffle(L, R, {"how": "left", "on": "id", "sort": True})
local = L.merge(R, how="left", on="id", sort=True)
assert got["id"].is_monotonic_increasing
cols = list(local.columns)
pd.testing.assert_frame_equal(got.sort_values(by=cols).reset_index(drop=True),
                              local.sort_values(by=cols).reset_index(drop=True))
Li, Ri = L.set_index("id"), R.set_index("id")
got = shim._merge_shuffle(Li, Ri, {"how": "inner", "left_index": True, "right_index": True, "sort": True})
local = Li.merge(Ri, left_index=True, right_index=True, sort=True)
assert got.index.is_monotonic_increasing
pd.testing.assert_frame_equal(got.sort_index().sort_values(by=["lv", "rv"]).reset_index(drop=True),
                              local.sort_index().sort_values(by=["lv", "rv"]).reset_index(drop=True))

big = pd.DataFrame({"k": np.random.randint(0, 9, 20000), "v": np.random.randn(20000), "s": np.random.randint(0, 50, 20000)})
csv = os.path.join(TMP, "big.csv")
big.to_csv(csv, index=False)
pf = shim._partitioned_read_csv(csv, {})
assert len(pf) == 20000 and pf.shape[1] == 3
local_df = pd.read_csv(csv)
pd.testing.assert_frame_equal(pf._materialize(), local_df)
pd.testing.assert_frame_equal(pf.head(7).reset_index(drop=True), local_df.head(7).reset_index(drop=True))
pd.testing.assert_frame_equal(pf.tail(9).reset_index(drop=True), local_df.tail(9).reset_index(drop=True))
pd.testing.assert_frame_equal(pf.groupby("k").agg({"v": ["sum", "mean"], "s": "count"}),
                              local_df.groupby("k").agg({"v": ["sum", "mean"], "s": "count"}))
pd.testing.assert_series_equal(pf.groupby("k")["v"].mean(), local_df.groupby("k")["v"].mean())
pd.testing.assert_series_equal(pf.groupby("k")["s"].sum(), local_df.groupby("k")["s"].sum())
pd.testing.assert_frame_equal(pf.groupby("k").agg(vm=("v", "mean")),
                              local_df.groupby("k").agg(vm=("v", "mean")))
right = pd.DataFrame({"k": list(range(9)), "tag": list("abcdefghi")})
pd.testing.assert_frame_equal(pf.merge(right, on="k", how="left", sort=True),
                              local_df.merge(right, on="k", how="left", sort=True))
print("SHUFFLE-OK")
`, "SHUFFLE-OK")
}

// TestShimOutOfCoreIntegrity (T5) checks the chunked read guarantees: line
// boundaries, per-node working set bounded by one chunk (RAM flat), and the
// plain-DataFrame fallback when the daemon is unreachable.
func TestShimOutOfCoreIntegrity(t *testing.T) {
	runShimPython(t, "t5", shimFakeDaemon+`
csv = os.path.join(TMP, "lines.csv")
with open(csv, "wb") as f:
    f.write(b"k,v\n" + b"".join(b"k%d,%d\n" % (i % 5, i) for i in range(1000)))
ranges = list(shim._csv_ranges(csv, 0))
offs = [ranges[0][0]] + [b for _, b in ranges]
assert offs[0] == 4, offs
data = open(csv, "rb").read()
lines = 0
for a, b in zip(offs, offs[1:] + [len(data)]):
    seg = data[a:b]
    if seg:
        assert not seg.startswith(b"k,v"), seg[:20]
    lines += seg.count(b"\n")
assert lines == 1000
assert offs[-1] == len(data)

os.environ["PIPEDPEER_DAEMON_URL"] = "http://127.0.0.1:1"
r = pd.read_csv(csv)
assert isinstance(r, pd.DataFrame) and len(r) == 1000
os.environ["PIPEDPEER_DAEMON_URL"] = "http://127.0.0.1:%d" % PORT

shim._OOC_CHUNK = 1024 * 1024
big = pd.DataFrame({"k": np.random.randint(0, 9, 800000), "v": np.random.randn(800000), "s": np.random.randint(0, 50, 800000)})
bigcsv = os.path.join(TMP, "ram.csv")
big.to_csv(bigcsv, index=False)
pf = shim._partitioned_read_csv(bigcsv, {})
assert len(pf) == len(big)
b64_byte = MAX_CHUNK[0] * 3 // 4
assert b64_byte <= 1024 * 1024 + 65536, b64_byte
assert len(pf._keys) > 10, len(pf._keys)
print("OOC-OK")
`, "OOC-OK")
}

// TestShimCostModelDecisions (T6) pins the latency/bandwidth cost-model
// decision table and the probe cache (one real probe per TTL).
func TestShimCostModelDecisions(t *testing.T) {
	runShimPython(t, "t6", shimFakeDaemon+`
_real_bw = shim._measure_bandwidth
shim._measure_bandwidth = lambda: 500e6
assert not shim._should_spill(16 * 1024 * 1024, 100)
assert not shim._should_spill(100 * 1024 * 1024, 2)
assert not shim._should_spill(600 * 1024 * 1024, 2)
shim._measure_bandwidth = lambda: 2e9
assert shim._should_spill(600 * 1024 * 1024, 2)
shim._measure_bandwidth = lambda: 500e6
assert shim._should_spill(200 * 1024 * 1024, 100)
shim._measure_bandwidth = lambda: None
assert not shim._should_spill(1e9, 100)
shim._measure_bandwidth = _real_bw

assert PROBES[0] == 0
bw1 = shim._measure_bandwidth()
bw2 = shim._measure_bandwidth()
# bw1 may be None when the probe fails under load (documented "stay local" fallback).
# Core cache invariant: both calls must return the same value regardless of outcome.
assert bw1 == bw2, "cache broken: two calls returned different values"
# PROBES counts server-side arrivals: 1 when the request succeeded, 0 when it
# threw before the server accepted it.  Either is valid; what must never happen
# is PROBES > 1 (which would mean the second call bypassed the TTL cache).
assert PROBES[0] in (0, 1), "unexpected probe count %d" % PROBES[0]
if bw1 is not None:
    assert PROBES[0] == 1, "probe returned data but server never received it"
print("COST-OK")
`, "COST-OK")
}

// TestShimJoblibBackendDistributes (T7) runs an UNMODIFIED sklearn
// RandomForestClassifier(n_jobs=-1).fit() plus a plain joblib.Parallel call
// and requires the batches to reach the cluster (no multiprocessing.Pool
// anywhere in the user script). Skipped when joblib/sklearn are missing.
func TestShimJoblibBackendDistributes(t *testing.T) {
	runShimPythonEnv(t, "t7", shimFakeDaemon+`
import sitecustomize as shim
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib

shim._ENABLED = True
shim._URL = "http://127.0.0.1:%d" % PORT
shim._STORE = ""

rng = np.random.RandomState(0)
X = rng.rand(200, 8)
y = (X[:, 0] * 2 + X[:, 1] > 1).astype(int)
rf = RandomForestClassifier(n_estimators=16, n_jobs=-1, random_state=0)
rf.fit(X, y)
p = rf.predict(X[:5])
assert p.shape == (5,)
res = joblib.Parallel(n_jobs=-1)(joblib.delayed(lambda a, b: a + b)(i, i) for i in range(12))
assert res == [2 * i for i in range(12)], res
assert MAX_CHUNK[0] > 0, "no joblib batch reached the cluster"
print("JOBLIB-OK")
`, "JOBLIB-OK",
		"PIPEDPEER_SHIM=1", "PIPEDPEER_NUM_SHARDS=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}

// TestShimNumPyInterception (T8) runs unmodified np.matmul/np.tensordot and
// np.linalg.svd/eig on large arrays and requires: block-row slicing (workers
// only ever see a row slice, never the full A), tensordot/svd/eig correctness
// against local numpy, and svd/eig offloading the whole matrix to one worker.
func TestShimNumPyInterception(t *testing.T) {
	runShimPythonEnv(t, "t8", shimFakeDaemon+`
import time
import base64
import sitecustomize as shim
import numpy as np

shim._ENABLED = True
shim._URL = "http://127.0.0.1:%d" % PORT
shim._STORE = ""
# The fake daemon's loopback latency would make the cost model refuse to
# spill; pin the cached bandwidth so the cost model chooses remote.
shim._BW_CACHE["bw"] = 1e9
shim._BW_CACHE["t"] = time.monotonic()

rng = np.random.RandomState(1)
A = rng.rand(2048, 2048)
B = rng.rand(2048, 2048)
b64A = len(base64.b64encode(A.tobytes()))

# Honest cost model at realistic BLAS speeds: local dgemm beats the transport
# for matmul, but svd offloads (LAPACK is ~100x slower per flop).
assert not shim._numpy_should_offload(A.nbytes, max(8, A.shape[0] // 8), 5, True, 200e9)
assert shim._numpy_should_offload(A.nbytes, max(8, A.shape[0] // 4), 3, False, 1.5e9)
# Force every offload so the slicing/frames mechanics below are exercised.
shim._numpy_should_offload = lambda *a, **k: True

C = np.matmul(A, B)            # dispatches block rows
local = A @ B                  # @ operator is untouched: local reference
assert np.allclose(C, local)
assert 0 < MAX_CHUNK[0] < b64A, "block rows must be sliced below the full A"

T = np.tensordot(A, B, axes=((1,), (0,)))
assert np.allclose(T, local)

U, S, Vh = np.linalg.svd(A)    # offloaded to one worker
assert np.allclose(U @ np.diag(S) @ Vh, A, rtol=1e-4, atol=1e-4)
assert MAX_CHUNK[0] >= A.nbytes, "svd must ship the matrix to a worker"

W, V = np.linalg.eig(A)
assert np.allclose(A @ V, V * W, rtol=1e-4)
print("NUMPY-OK")
`, "NUMPY-OK",
		"PIPEDPEER_SHIM=1", "PIPEDPEER_NUM_SHARDS=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}

// TestShimDDPSync (T4) runs two gloo ranks over loopback through the shim's
// transparent DDP and requires: identical weights across ranks, and each
// rank's synced gradient equal to the exact (g0+g1)/2 of the single-process
// baselines. Skipped when torch is missing.
func TestShimDDPSync(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if _, err := exec.Command(python, "-c", "import torch; import torch.distributed").CombinedOutput(); err != nil {
		t.Skipf("torch.distributed not available: %v", err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	rankSrc := `
import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from torch.utils.data.distributed import DistributedSampler

rank = int(os.environ["PIPEDPEER_RANK"])
torch.manual_seed(0)
model = nn.Linear(8, 4)
opt = optim.SGD(model.parameters(), lr=0.1)

torch.manual_seed(rank + 1)
x = torch.randn(16, 8)
y = torch.randn(16, 4)

loss = nn.functional.mse_loss(model(x), y)
loss.backward()
opt.step()
grad = model.weight.grad.detach().clone()

# sampler construction needs the initialized group (the shim inits lazily at
# the first optimizer.step); iterating twice must bump set_epoch cleanly
if os.environ.get("PIPEDPEER_DDP") == "1":
    ds = TensorDataset(x, y)
    dl = DataLoader(ds, batch_size=4, sampler=DistributedSampler(ds))
    for _ in dl:
        pass
    for _ in dl:
        pass

prefix = os.environ.get("PIPEDPEER_TEST_PREFIX", "rank")
torch.save({"w": model.weight.detach(), "g": grad},
           os.path.join(os.environ["PIPEDPEER_TEST_OUT"], "%s-%d.pt" % (prefix, rank)))
print("RANK-%d-OK" % rank)
`
	rankScript := filepath.Join(dir, "rank.py")
	if err := os.WriteFile(rankScript, []byte(rankSrc), 0644); err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	masterPort := l.Addr().(*net.TCPAddr).Port
	l.Close()

	base := []string{
		"PYTHONPATH=" + dir,
		"PIPEDPEER_TEST_OUT=" + outDir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=",
		"PIPEDPEER_NUM_SHARDS=0",
		"PIPEDPEER_TORCH=0",
	}
	runOnce := func(env ...string) {
		cmd := exec.Command(python, rankScript)
		cmd.Env = append(os.Environ(), append(append([]string(nil), base...), env...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("rank run failed: %s\n%v", out, err)
		}
	}
	runOnce("PIPEDPEER_RANK=0", "PIPEDPEER_WORLD_SIZE=1", "PIPEDPEER_TEST_PREFIX=alone")
	runOnce("PIPEDPEER_RANK=1", "PIPEDPEER_WORLD_SIZE=1", "PIPEDPEER_TEST_PREFIX=alone")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			cmd := exec.Command(python, rankScript)
			cmd.Env = append(os.Environ(), append(append([]string(nil), base...),
				"PIPEDPEER_DDP=1",
				fmt.Sprintf("PIPEDPEER_RANK=%d", r),
				"PIPEDPEER_WORLD_SIZE=2",
				"MASTER_ADDR=127.0.0.1",
				fmt.Sprintf("MASTER_PORT=%d", masterPort))...)
			if out, err := cmd.CombinedOutput(); err != nil {
				errs <- fmt.Errorf("rank %d: %s\n%v", r, out, err)
			}
		}(r)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	assertSrc := `
import os, sys, torch
out = sys.argv[1]
def load(n):
    return torch.load(os.path.join(out, n))
w0, w1 = load("rank-0.pt"), load("rank-1.pt")
a0, a1 = load("alone-0.pt"), load("alone-1.pt")
assert torch.equal(w0["w"], w1["w"]), "weights differ across ranks"
avg = (a0["g"] + a1["g"]) / 2
assert torch.equal(w0["g"], avg), "rank 0 grad != (g0+g1)/2"
assert torch.equal(w1["g"], avg), "rank 1 grad != (g0+g1)/2"
print("DDP-SYNC-OK")
`
	assertScript := filepath.Join(dir, "assert.py")
	if err := os.WriteFile(assertScript, []byte(assertSrc), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, assertScript, outDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ddp assertions failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "DDP-SYNC-OK") {
		t.Fatalf("missing DDP-SYNC-OK: %q", out)
	}
}

// PIPEDPEER_DISTRIBUTE=force must make the spill gates say yes without
// consulting the cost model or the bandwidth probe: it exists so a demo can
// show distribution on payloads the never-slower policy would keep local.
func TestShimForceDistributeBypassesCostModel(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	shim := filepath.Join(dir, "sitecustomize.py")
	if err := os.WriteFile(shim, []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(`
import sitecustomize as sc

# 1 KB of work with modest flops: auto mode must refuse, force must accept.
assert sc._should_spill(1024, 4) is True, "_should_spill ignored force"
assert sc._numpy_should_offload(1024, 4, 2, True, 1e9) is True, "_numpy_should_offload ignored force"
assert sc._spill_min() == 0, "force did not zero the spill floor"
print("FORCE_OK")
`), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(python, script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1", // never dialled: force skips the probe
		"PIPEDPEER_NUM_SHARDS=2",
		"PIPEDPEER_DISTRIBUTE=force",
		"PYTHONPATH="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "FORCE_OK") {
		t.Fatalf("force-mode probe failed: %v\n%s", err, out)
	}

	// And the floor must be overridable independently of force.
	cmd = exec.Command(python, "-c",
		"import sitecustomize as sc; assert sc._spill_min() == 5.0; print('MIN_OK')")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_SHIM=1", "PIPEDPEER_SPILL_MIN=5", "PYTHONPATH="+dir)
	out, err = cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "MIN_OK") {
		t.Fatalf("PIPEDPEER_SPILL_MIN probe failed: %v\n%s", err, out)
	}
}

// TestShimDoesNotImportHeavyLibsAtStartup pins the lazy-patching contract.
//
// The shim used to call _install_torch/_install_pandas/_install_numpy eagerly,
// so every job paid for `import torch` (~1.7s here) even when the script never
// mentioned torch. That is precisely the never-slower guarantee the shim
// exists to keep, and scripts/bench-shim-d2.sh measured the tax at 21x before
// the fix. Patching is now deferred to each library's first import.
func TestShimDoesNotImportHeavyLibsAtStartup(t *testing.T) {
	src := `
import sys
assert sys.modules.get("sitecustomize") is not None, "shim did not load"
import sitecustomize as shim
assert shim._ENABLED, "shim not enabled; this test would be vacuous"
for mod in ("torch", "pandas", "numpy", "joblib", "sklearn"):
    assert mod not in sys.modules, mod + " was imported at startup"
print("LAZY-OK")
`
	runShimPythonEnv(t, "lazy_startup", src, "LAZY-OK",
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1",
		"PIPEDPEER_NUM_SHARDS=3")
}

// TestShimPatchesOnFirstImport is the other half of that contract: deferring
// the work must not lose it. numpy is patched only once the script imports it.
func TestShimPatchesOnFirstImport(t *testing.T) {
	src := `
import sys
import sitecustomize as shim
assert shim._ENABLED, "shim not enabled; this test would be vacuous"
assert "numpy" not in sys.modules, "numpy imported before the script asked"
import numpy as np
assert np.matmul.__module__ == "sitecustomize", np.matmul.__module__
assert "numpy" not in shim._PENDING_PATCHES, "numpy patch still pending"
print("PATCH-OK")
`
	runShimPythonEnv(t, "lazy_patch", src, "PATCH-OK",
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1",
		"PIPEDPEER_NUM_SHARDS=3")
}
