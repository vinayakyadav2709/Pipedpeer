package nixgen

import (
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
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(python, "-m", "compileall", "-q", dir).CombinedOutput()
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
	if err := WriteShimDir(dir); err != nil {
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
	if err := WriteShimDir(dir); err != nil {
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
		"PIPEDPEER_CLOSURE_NODES=0",
		"PIPEDPEER_VERBOSE=1", // the "local 3 items" line is a -v detail
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CACHE = {}
PROBES = [0]
MAX_CHUNK = [0]
# How many pool requests arrived. MAX_CHUNK alone cannot separate two phases of
# a test: it is a running maximum, so a phase that distributed nothing leaves it
# untouched and the assertion still passes on the earlier phase's work.
POOL_CALLS = [0]
FRAMES = []
COUNT_LOCK = threading.Lock()
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
            with COUNT_LOCK:
                POOL_CALLS[0] += 1
            # Sizes are measured on the wire, before decompression: the point
            # of MAX_CHUNK is what actually crossed.
            b = [len(f) for f in raw]
            if b:
                MAX_CHUNK[0] = max(MAX_CHUNK[0], max(b))
            # The shim compresses a payload when it measures it as
            # compressible and says so in the header. This fake daemon did not
            # honour that flag, so every compressed chunk arrived here as a
            # zlib stream and pickle.loads failed with "invalid load key, 'x'"
            # - 0x78 being a zlib header. It went unnoticed because the tests
            # that send compressible payloads skip wherever pandas is missing,
            # which is most machines.
            if req.get("items_zlib"):
                import zlib
                raw = [zlib.decompress(r) for r in raw]
            # Every frames request as it crossed, for tests that load one in
            # another process (where this script's __main__ is absent).
            FRAMES.append((req, g, raw))
        else:
            with COUNT_LOCK:
                POOL_CALLS[0] += 1
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

# Threaded: one request at a time made a call's
# requests wait behind the previous call's lost-race chunks, so on a loaded
# machine they were counted during the next call instead of their own.
srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
srv.daemon_threads = True
PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

os.environ["PIPEDPEER_SHIM"] = "1"
os.environ["PIPEDPEER_DAEMON_URL"] = "http://127.0.0.1:%d" % PORT
os.environ["PIPEDPEER_CLOSURE_NODES"] = "3"
os.environ["PIPEDPEER_STORE_PATH"] = ""
os.environ["PIPEDPEER_OOC_MIN"] = "1"

import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler
# pandas and numpy are imported by the scripts that use them, not here. This
# preamble is shared by every test that needs a fake daemon, and importing them
# unconditionally made the cost-model and numpy tests skip for want of pandas
# they never touch - a skip for the wrong reason, which hides that the machine
# could have run the test.
# sitecustomize is imported at interpreter startup, before this preamble runs,
# so the env vars it read then are stale; point the module at the fake daemon.
core._ENABLED = True
core._URL = "http://127.0.0.1:%d" % PORT
core._STORE = ""
core._NUM_SHARDS = "3"
pandas_ooc._OOC_CHUNK = 4096
`

// runShimPython writes the shim plus a python script and runs it; any nonzero
// exit or a missing sentinel fails the test.
func runShimPython(t *testing.T, name, src string, wantSentinel string) {
	runShimPythonEnv(t, name, src, wantSentinel)
}

// runShimPythonEnv is runShimPython with extra process env vars, needed when
// the shim's install-time decisions (joblib backend, numpy patching) must see
// a cluster before the preamble overrides the module attributes.
// requiredPython reports which third-party modules a shim test script needs,
// by looking at what it uses. Deliberately conservative on both sides: a
// module named here that is not needed causes a skip that could have run, and
// one missed causes an import error inside the script that reads as a test
// failure.
func requiredPython(src string) []string {
	var deps []string
	add := func(mod string, markers ...string) {
		for _, m := range markers {
			if strings.Contains(src, m) {
				deps = append(deps, mod)
				return
			}
		}
	}
	add("pandas", "import pandas", "pd.")
	add("numpy", "import numpy", "np.")
	add("sklearn", "import sklearn", "from sklearn")
	add("joblib", "import joblib", "joblib.")
	// The weighted sampler imports torch inside __iter__, so a script that
	// only names the sampler needs torch too.
	add("torch", "import torch", "torch.", "_WeightedShardSampler", "_weighted_batches")
	return deps
}

// pythonImportChecks caches pythonImports per interpreter and import list.
var pythonImportChecks sync.Map

// pythonImports reports whether python can import mods ("torch, numpy"),
// asking once per test run: each check is a fresh interpreter importing the
// module (torch alone ~2-4 s), and ~35 tests here used to repeat it.
func pythonImports(python, mods string) ([]byte, error) {
	type result struct {
		out []byte
		err error
	}
	f, _ := pythonImportChecks.LoadOrStore(python+"\x00"+mods, sync.OnceValue(func() result {
		out, err := exec.Command(python, "-c", "import "+mods).CombinedOutput()
		return result{out, err}
	}))
	r := f.(func() result)()
	return r.out, r.err
}

func runShimPythonEnv(t *testing.T, name, src, wantSentinel string, envs ...string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	// Gate on what this particular script actually imports.
	//
	// The gate used to fire on the fake daemon being present at all, which
	// meant the numpy tests and the cost-model test skipped for want of
	// pandas that they never touch. A skip is coverage that is not running,
	// and skipping for the wrong reason hides that a machine could have run
	// it: three of the six skips here needed nothing that was missing.
	for _, dep := range requiredPython(src) {
		if _, err := pythonImports(python, dep); err != nil {
			t.Skipf("%s not available, which this script imports: %v", dep, err)
		}
	}
	dir := t.TempDir()
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, name+".py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script)
	// Verbose: several tests assert the shim's progress lines (run -v detail).
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "PIPEDPEER_VERBOSE=1")
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
import pandas as pd
import numpy as np
n = 4000
df = pd.DataFrame({"k": np.random.randint(0, 13, n), "a": np.random.randn(n), "b": np.random.randint(0, 100, n)})
gb = df.groupby("k")
pd.testing.assert_frame_equal(pandas_ops._groupby_shuffle(gb, {"a": ["sum", "mean"], "b": "max"}, (), {}),
                              gb.agg({"a": ["sum", "mean"], "b": "max"}))
pd.testing.assert_frame_equal(pandas_ops._groupby_shuffle(df.groupby("k"), ["sum", "mean"], (), {}),
                              df.groupby("k").agg(["sum", "mean"]))
pd.testing.assert_frame_equal(pandas_ops._groupby_shuffle(df.groupby("k", as_index=False), "sum", (), {}),
                              df.groupby("k", as_index=False).sum())
dna = df.copy()
dna.loc[::97, "k"] = np.nan
pd.testing.assert_frame_equal(pandas_ops._groupby_shuffle(dna.groupby("k"), "sum", (), {}),
                              dna.groupby("k").sum())

L = pd.DataFrame({"id": np.random.randint(0, 50, 3000), "lv": np.random.randn(3000)})
R = pd.DataFrame({"id": np.random.randint(0, 50, 500), "rv": np.random.randn(500)})
for how in ("inner", "left", "right", "outer"):
    got = pandas_ops._merge_shuffle(L, R, {"how": how, "on": "id", "sort": False})
    local = L.merge(R, how=how, on="id", sort=False)
    cols = list(local.columns)
    pd.testing.assert_frame_equal(got.sort_values(by=cols).reset_index(drop=True),
                                  local.sort_values(by=cols).reset_index(drop=True))
got = pandas_ops._merge_shuffle(L, R, {"how": "left", "on": "id", "sort": True})
local = L.merge(R, how="left", on="id", sort=True)
assert got["id"].is_monotonic_increasing
cols = list(local.columns)
pd.testing.assert_frame_equal(got.sort_values(by=cols).reset_index(drop=True),
                              local.sort_values(by=cols).reset_index(drop=True))
Li, Ri = L.set_index("id"), R.set_index("id")
got = pandas_ops._merge_shuffle(Li, Ri, {"how": "inner", "left_index": True, "right_index": True, "sort": True})
local = Li.merge(Ri, left_index=True, right_index=True, sort=True)
assert got.index.is_monotonic_increasing
pd.testing.assert_frame_equal(got.sort_index().sort_values(by=["lv", "rv"]).reset_index(drop=True),
                              local.sort_index().sort_values(by=["lv", "rv"]).reset_index(drop=True))

big = pd.DataFrame({"k": np.random.randint(0, 9, 20000), "v": np.random.randn(20000), "s": np.random.randint(0, 50, 20000)})
csv = os.path.join(TMP, "big.csv")
big.to_csv(csv, index=False)
pf = pandas_ooc._partitioned_read_csv(csv, {})
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

// TestShimPartitionedFrameFallsBackWhenTheClusterLosesItsChunks: the finale's
// pandas job died on "daemon HTTP 500: pool run failed" when the node asked for
// a chunk had no copy of it. Every partitioned op must then re-parse the
// chunks here, say so, and give the local answer.
func TestShimPartitionedFrameFallsBackWhenTheClusterLosesItsChunks(t *testing.T) {
	runShimPython(t, "oocfallback", shimFakeDaemon+`
import io, contextlib
import pandas as pd
import numpy as np
big = pd.DataFrame({"k": np.random.randint(0, 9, 20000), "v": np.random.randn(20000), "s": np.random.randint(0, 50, 20000)})
csv = os.path.join(TMP, "lost.csv")
big.to_csv(csv, index=False)
pf = pandas_ooc._partitioned_read_csv(csv, {})
assert len(pf._keys) > 3
local_df = pd.read_csv(csv)
class _Lost(dict):
    def get(self, k, d=None):
        raise KeyError("chunk cache miss: %s" % k)
CACHE = _Lost()  # every node lost its chunks: each map now fails, as the real daemon's does
err = io.StringIO()
with contextlib.redirect_stderr(err):
    pd.testing.assert_frame_equal(pf.groupby("k").mean(), local_df.groupby("k").mean())
    pd.testing.assert_frame_equal(pf.groupby("k").agg({"v": ["sum", "mean"], "s": "count"}),
                                  local_df.groupby("k").agg({"v": ["sum", "mean"], "s": "count"}))
    pd.testing.assert_frame_equal(pf._materialize(), local_df)
    pd.testing.assert_frame_equal(pf.head(7).reset_index(drop=True), local_df.head(7).reset_index(drop=True))
    right = pd.DataFrame({"k": list(range(9)), "tag": list("abcdefghi")})
    pd.testing.assert_frame_equal(pf.merge(right, on="k", how="left", sort=True),
                                  local_df.merge(right, on="k", how="left", sort=True))
out = err.getvalue()
for what in ("groupby", "materialize", "chunk fetch", "merge"):
    assert ("WARNING: %s on the cluster failed" % what) in out, (what, out)
assert stats._STATS["remote_failures"] >= 5, stats._STATS["remote_failures"]
print("OOC-FALLBACK-OK")
`, "OOC-FALLBACK-OK")
}

// TestShimOutOfCoreIntegrity (T5) checks the chunked read guarantees: line
// boundaries, per-node working set bounded by one chunk (RAM flat), and the
// plain-DataFrame fallback when the daemon is unreachable.
func TestShimOutOfCoreIntegrity(t *testing.T) {
	runShimPython(t, "t5", shimFakeDaemon+`
import pandas as pd
import numpy as np
csv = os.path.join(TMP, "lines.csv")
with open(csv, "wb") as f:
    f.write(b"k,v\n" + b"".join(b"k%d,%d\n" % (i % 5, i) for i in range(1000)))
ranges = list(pandas_ooc._csv_ranges(csv, 0))
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

pandas_ooc._OOC_CHUNK = 1024 * 1024
big = pd.DataFrame({"k": np.random.randint(0, 9, 800000), "v": np.random.randn(800000), "s": np.random.randint(0, 50, 800000)})
bigcsv = os.path.join(TMP, "ram.csv")
big.to_csv(bigcsv, index=False)
pf = pandas_ooc._partitioned_read_csv(bigcsv, {})
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
_real_bw = transfer._measure_bandwidth
transfer._measure_bandwidth = lambda: 500e6
assert not transfer._should_spill(16 * 1024 * 1024, 100)
assert not transfer._should_spill(100 * 1024 * 1024, 2)
assert not transfer._should_spill(600 * 1024 * 1024, 2)
transfer._measure_bandwidth = lambda: 2e9
assert transfer._should_spill(600 * 1024 * 1024, 2)
transfer._measure_bandwidth = lambda: 500e6
assert transfer._should_spill(200 * 1024 * 1024, 100)
transfer._measure_bandwidth = lambda: None
assert not transfer._should_spill(1e9, 100)
transfer._measure_bandwidth = _real_bw

assert PROBES[0] == 0
bw1 = transfer._measure_bandwidth()
bw2 = transfer._measure_bandwidth()
# Both calls must return the same value: that is the TTL cache doing its job.
assert bw1 == bw2, "cache broken: two calls returned different values"
# Exactly one probe reached the server. This used to accept 0 or 1, on the
# grounds that the probe might fail under load - which also accepted a
# _measure_bandwidth that never contacted anything at all, and the cost model
# is built entirely on what it returns. The daemon here is a local fake that
# answers every time; measured stable over repeated runs, so the tolerance was
# caution rather than a real failure mode. If this ever does flake, the fix is
# to make the fake answer reliably, not to widen the assertion back out.
assert PROBES[0] == 1, "expected exactly one probe, got %d" % PROBES[0]
assert bw1 is not None, "the probe reached the server but produced no bandwidth"
if bw1 is not None:
    assert PROBES[0] == 1, "probe returned data but server never received it"
print("COST-OK")
`, "COST-OK")
}

// TestShimDoesNotProbeWhenNoLinkWouldChangeTheAnswer.
//
// The bandwidth probe ships 64 MB. A script doing one 33 MB matmul spent 2.5 s
// on a LAN measuring the link only to conclude it should not use it, and over
// the phone-tethered link this also runs on, 64 MB is half a minute - the
// probe costing far more than the work it was declining.
//
// Bandwidth only ever divides est_transfer, so measuring can only make
// shipping look worse. If an offload loses at a speed no real link reaches, it
// loses at the real speed, and there is nothing to find out.
func TestShimDoesNotProbeWhenNoLinkWouldChangeTheAnswer(t *testing.T) {
	runShimPython(t, "t6b", shimFakeDaemon+`
import numpy as np
A = np.zeros((2048, 2048))

assert PROBES[0] == 0, "something probed before the test started"

# matmul at real BLAS speed: local wins by a wide margin at any bandwidth.
assert not numpy_ops._numpy_should_offload(A.nbytes, max(8, A.shape[0] // 8), 5, True, 200e9)
assert PROBES[0] == 0, (
    "the link was measured to answer a question the measurement could not "
    "change: %d probe(s), 64 MB each" % PROBES[0])

# svd at LAPACK speed is the other way round - shipping plausibly wins, so the
# real bandwidth genuinely decides and the probe is worth its cost.
numpy_ops._numpy_should_offload(A.nbytes, max(8, A.shape[0] // 4), 3, False, 1.5e9)
assert PROBES[0] == 1, (
    "expected the probe when the answer actually depends on it, got %d"
    % PROBES[0])
print("NOPROBE-OK")
`, "NOPROBE-OK")
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
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler
import numpy as np

core._ENABLED = True
core._URL = "http://127.0.0.1:%d" % PORT
core._STORE = ""
# The fake daemon's loopback latency would make the cost model refuse to
# spill; pin the cached bandwidth so the cost model chooses remote.
transfer._BW_CACHE["bw"] = 1e9
transfer._BW_CACHE["t"] = time.monotonic()

# Honest cost model at realistic BLAS speeds, for a 2048x2048 float64 matrix
# (pure arithmetic, above the 32 MB spill floor): local dgemm beats the
# transport for matmul, but svd offloads (LAPACK is ~100x slower per flop).
N = 2048
assert not numpy_ops._numpy_should_offload(N * N * 8, max(8, N // 8), 5, True, 200e9)
assert numpy_ops._numpy_should_offload(N * N * 8, max(8, N // 4), 3, False, 1.5e9)

# The mechanics run on 512x512: forced offload skips the size floor, and the
# path is the same at any size, while 2048 made svd+eig take ~2 minutes.
rng = np.random.RandomState(1)
A = rng.rand(512, 512)
B = rng.rand(512, 512)
b64A = len(base64.b64encode(A.tobytes()))

# Force every offload so the slicing/frames mechanics below are exercised.
numpy_ops._numpy_should_offload = lambda *a, **k: True

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
		"PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}

// TestShimNumPyRespectsTheCostModelsNo is the other half of T8.
//
// That test checks the cost model's arithmetic, and then overrides it to
// always-true so the slicing and framing mechanics get exercised. What neither
// covered is whether the interception actually consults the model: an
// implementation that ignored it and shipped everything would satisfy both,
// and would send a matrix across the wire to do work that is faster locally.
func TestShimNumPyRespectsTheCostModelsNo(t *testing.T) {
	runShimPythonEnv(t, "t8b", shimFakeDaemon+`
import time
import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler
import numpy as np

core._ENABLED = True
core._URL = "http://127.0.0.1:%d" % PORT
core._STORE = ""
transfer._BW_CACHE["bw"] = 1e9
transfer._BW_CACHE["t"] = time.monotonic()

rng = np.random.RandomState(1)
A = rng.rand(512, 512)
B = rng.rand(512, 512)

# The model says no to everything. Nothing may cross the wire, and the answers
# must still be right - "stayed local" is only acceptable if it also computed.
numpy_ops._numpy_should_offload = lambda *a, **k: False

before = POOL_CALLS[0]
C = np.matmul(A, B)
U, S, Vh = np.linalg.svd(A)
W, V = np.linalg.eig(A)
after = POOL_CALLS[0]

assert after == before, (
    "%d pool call(s) were made while the cost model refused every offload; the "
    "interception is not consulting it" % (after - before))
assert np.allclose(C, A @ B), "matmul returned the wrong answer while local"
assert np.allclose(U @ np.diag(S) @ Vh, A, rtol=1e-4, atol=1e-4), "svd wrong while local"
assert np.allclose(A @ V, V * W, rtol=1e-4), "eig wrong while local"
print("NUMPY-LOCAL-OK")
`, "NUMPY-LOCAL-OK",
		"PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
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
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}

	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(`
import sitecustomize as sc
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler

# 1 KB of work with modest flops: auto mode must refuse, force must accept.
assert transfer._should_spill(1024, 4) is True, "_should_spill ignored force"
assert numpy_ops._numpy_should_offload(1024, 4, 2, True, 1e9) is True, "_numpy_should_offload ignored force"
assert core._spill_min() == 0, "force did not zero the spill floor"
print("FORCE_OK")
`), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(python, script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1", // never dialled: force skips the probe
		"PIPEDPEER_CLOSURE_NODES=2",
		"PIPEDPEER_DISTRIBUTE=force",
		"PYTHONPATH="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "FORCE_OK") {
		t.Fatalf("force-mode probe failed: %v\n%s", err, out)
	}

	// And the floor must be overridable independently of force.
	cmd = exec.Command(python, "-c",
		"from pipedpeer_shim import core; assert core._spill_min() == 5.0; print('MIN_OK')")
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
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler
assert core._ENABLED, "shim not enabled; this test would be vacuous"
for mod in ("torch", "pandas", "numpy", "joblib", "sklearn"):
    assert mod not in sys.modules, mod + " was imported at startup"
print("LAZY-OK")
`
	runShimPythonEnv(t, "lazy_startup", src, "LAZY-OK",
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1",
		"PIPEDPEER_CLOSURE_NODES=3")
}

// TestShimPatchesOnFirstImport is the other half of that contract: deferring
// the work must not lose it. numpy is patched only once the script imports it.
func TestShimPatchesOnFirstImport(t *testing.T) {
	src := `
import sys
import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pandas_ooc, ddp_sampler
assert core._ENABLED, "shim not enabled; this test would be vacuous"
assert "numpy" not in sys.modules, "numpy imported before the script asked"
import numpy as np
assert np.matmul.__module__ == "pipedpeer_shim.numpy_ops", np.matmul.__module__
assert "numpy" not in ddp._PENDING_PATCHES, "numpy patch still pending"
print("PATCH-OK")
`
	runShimPythonEnv(t, "lazy_patch", src, "PATCH-OK",
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1",
		"PIPEDPEER_CLOSURE_NODES=3")
}

func TestRequiredPythonSeesTheShimsTorchHelpers(t *testing.T) {
	for _, src := range []string{"ddp._WeightedShardSampler(x)", "ddp._weighted_batches(x)"} {
		found := false
		for _, d := range requiredPython(src) {
			found = found || d == "torch"
		}
		if !found {
			t.Errorf("requiredPython(%q) misses torch", src)
		}
	}
}
