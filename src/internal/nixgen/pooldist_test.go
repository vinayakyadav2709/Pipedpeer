package nixgen

// Regression tests for cluster Pool distribution.
//
// These exist because Pool.map interception shipped for months without ever
// distributing anything: the callable travelled as a by-reference pickle
// (__main__.work), the worker runs in a different process with neither the
// job's workspace nor __main__ on its path, every chunk 500'd, and the shim's
// local-fallback ladder produced correct results with zero speedup. Nothing
// failed, because every test asserted results rather than provenance.
//
// So the rules here are deliberately different from the older shim tests:
//
//   - the fake daemon runs OUT OF PROCESS. An in-process daemon shares
//     __main__ with the script under test, which makes a by-reference pickle
//     resolve and silently manufactures the one condition where the broken
//     protocol works.
//   - the daemon counts what it actually executed and reports errors, and the
//     tests assert those counters. Result correctness is never sufficient
//     evidence: local fallback guarantees it.
//   - a fallback log line on stderr fails the test outright.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// poolFakeDaemon plays the daemon's /v1/pool/map for the shim, out of process.
// It understands both wire formats and, like the real warm worker, has only
// the closure's python on its path: no workspace, no user modules, and a
// __main__ of its own. Whatever it manages to execute, it executed for real.
const poolFakeDaemon = `
import base64, json, os, pickle, struct, sys, threading, traceback
from http.server import BaseHTTPRequestHandler, HTTPServer

STATE = {"requests": 0, "errors": 0, "items": 0, "last_error": "", "by_src": 0, "by_ref": 0}
LOCK = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path != "/stats":
            self.send_response(404); self.end_headers(); return
        with LOCK:
            body = json.dumps(STATE).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(n)
        with LOCK:
            STATE["requests"] += 1
        try:
            body = self.handle_map(data)
        except Exception as e:
            with LOCK:
                STATE["errors"] += 1
                STATE["last_error"] = "%s: %s" % (type(e).__name__, e)
            msg = traceback.format_exc().encode()
            self.send_response(500)
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_map(self, data):
        nl = data.find(b"\n")
        req = json.loads(data if nl < 0 else data[:nl])
        frames = bool(req.get("items_frames"))
        g = b""
        if frames:
            rest = data[nl + 1:]
            def rf(r):
                nn = struct.unpack(">I", r[:4])[0]
                return r[4:4 + nn], r[4 + nn:]
            if req.get("globals"):
                g, rest = rf(rest)
            raw = []
            for _ in range(req["items_frames"]):
                f, rest = rf(rest)
                raw.append(f)
            items = [pickle.loads(r) for r in raw]
        else:
            items = req["items"]
            if req.get("items_b64"):
                items = [pickle.loads(base64.b64decode(i)) for i in items]

        ns = {}
        if g:
            ns.update(pickle.loads(g))
        elif req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        ns["_CACHE"] = {}
        ns["_CHUNK_DIR"] = ""
        if req.get("func_src"):
            with LOCK:
                STATE["by_src"] += 1
            exec(req["func_src"], ns)
            func = ns[req.get("func_name", "run")]
        else:
            # The legacy by-reference protocol. Faithfully hostile: this
            # process has no user code, so __main__/workspace lookups fail
            # exactly as the real warm worker's would.
            with LOCK:
                STATE["by_ref"] += 1
            func = pickle.loads(base64.b64decode(req["func"]))

        starmap = req.get("starmap", False)
        results = []
        for item in items:
            if starmap:
                args = item if isinstance(item, (list, tuple)) else (item,)
                results.append(func(*args))
            else:
                results.append(func(item))
        with LOCK:
            STATE["items"] += len(results)

        if frames:
            out = b"".join(struct.pack(">I", len(p)) + p for p in [pickle.dumps(r) for r in results])
            # Stand in for a daemon that fanned the whole chunk to a peer, so
            # the shim's "ran elsewhere" accounting is exercised.
            receipt = json.dumps({"node": "fake-daemon", "parts": [
                {"where": "peer:198.51.100.7:38080", "items": len(results), "ms": 1}]})
            head = '{"results_frames": %d, "receipt": %s}\n' % (len(results), receipt)
            return head.encode() + out
        return json.dumps({"results": [{"pickle": base64.b64encode(pickle.dumps(r)).decode()} for r in results]}).encode()

srv = HTTPServer(("127.0.0.1", 0), Handler)
sys.stdout.write("PORT %d\n" % srv.server_address[1])
sys.stdout.flush()
srv.serve_forever()
`

type fakeDaemonStats struct {
	Requests  int    `json:"requests"`
	Errors    int    `json:"errors"`
	Items     int    `json:"items"`
	LastError string `json:"last_error"`
	BySrc     int    `json:"by_src"`
	ByRef     int    `json:"by_ref"`
}

// startPoolFakeDaemon runs poolFakeDaemon in its own process, from its own
// directory, so it cannot see the calling test's files or __main__.
func startPoolFakeDaemon(t *testing.T, python string) (url string, stats func(*testing.T) fakeDaemonStats) {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "fake_daemon.py")
	if err := os.WriteFile(script, []byte(poolFakeDaemon), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	portCh := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := stdout.Read(buf)
		line := strings.TrimSpace(string(buf[:n]))
		portCh <- strings.TrimPrefix(line, "PORT ")
	}()
	var port string
	select {
	case port = <-portCh:
	case <-time.After(20 * time.Second):
		t.Fatal("fake daemon did not report a port")
	}
	if port == "" {
		t.Fatal("fake daemon reported an empty port")
	}
	url = "http://127.0.0.1:" + port

	return url, func(t *testing.T) fakeDaemonStats {
		t.Helper()
		out, err := exec.Command(python, "-c", fmt.Sprintf(
			"import urllib.request;print(urllib.request.urlopen(%q).read().decode())", url+"/stats")).Output()
		if err != nil {
			t.Fatalf("reading fake daemon stats: %v", err)
		}
		var s fakeDaemonStats
		if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &s); err != nil {
			t.Fatalf("decoding fake daemon stats %q: %v", out, err)
		}
		return s
	}
}

type poolRunResult struct {
	stdout  string
	stderr  string
	receipt shimReceipt
	stats   fakeDaemonStats
}

type shimReceipt struct {
	RemoteItems     int `json:"remote_items"`
	LocalItems      int `json:"local_items"`
	DispatchedItems int `json:"dispatched_items"`
	RemoteFailures  int `json:"remote_failures"`
	Unshippable     int `json:"unshippable"`
	Parts           []struct {
		Kind         string `json:"kind"`
		Items        int    `json:"items"`
		OK           bool   `json:"ok"`
		Error        string `json:"error"`
		RanElsewhere int    `json:"ran_elsewhere"`
		Via          string `json:"via"`
	} `json:"parts"`
}

// runPoolScript runs src under the shim against an out-of-process fake daemon.
// Unlike the older shim harness it sets the environment before the interpreter
// starts, so sitecustomize reads the real values rather than being patched
// after the fact.
func runPoolScript(t *testing.T, files map[string]string, main string) poolRunResult {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	url, stats := startPoolFakeDaemon(t, python)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	receiptPath := filepath.Join(dir, "receipt.json")
	cmd := exec.Command(python, filepath.Join(dir, main))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL="+url,
		"PIPEDPEER_NUM_SHARDS=3",
		"PIPEDPEER_STORE_PATH=/nix/store/fake",
		"PIPEDPEER_RECEIPT="+receiptPath,
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("script failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	res := poolRunResult{stdout: stdout.String(), stderr: stderr.String(), stats: stats(t)}
	if raw, err := os.ReadFile(receiptPath); err == nil {
		if err := json.Unmarshal(raw, &res.receipt); err != nil {
			t.Fatalf("decoding receipt %q: %v", raw, err)
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("reading receipt: %v", err)
	}
	return res
}

// assertDistributed is the assertion set that the old tests were missing.
func assertDistributed(t *testing.T, res poolRunResult, wantSentinel string) {
	t.Helper()
	if !strings.Contains(res.stdout, wantSentinel) {
		t.Fatalf("missing sentinel %q\nstdout:\n%s\nstderr:\n%s", wantSentinel, res.stdout, res.stderr)
	}
	for _, bad := range []string{"remote failed", "local covers it", "Traceback"} {
		if strings.Contains(res.stderr, bad) {
			t.Errorf("stderr reports a fallback/crash (%q):\n%s", bad, res.stderr)
		}
	}
	if res.stats.Requests == 0 {
		t.Errorf("daemon received no pool/map requests: work never left the process\nstderr:\n%s", res.stderr)
	}
	if res.stats.Errors > 0 {
		t.Errorf("daemon failed %d/%d requests (last: %s)", res.stats.Errors, res.stats.Requests, res.stats.LastError)
	}
	if res.stats.Items == 0 {
		t.Errorf("daemon executed no items")
	}
	if res.receipt.DispatchedItems == 0 {
		t.Errorf("receipt records nothing dispatched to the cluster: %+v\nstderr:\n%s", res.receipt, res.stderr)
	}
	// The stronger claim: the daemon reported the work ran on another machine,
	// not merely that it accepted the request.
	if res.receipt.RemoteItems == 0 {
		t.Errorf("receipt records no items executed off this node: %+v\nstderr:\n%s", res.receipt, res.stderr)
	}
	if res.receipt.RemoteFailures > 0 {
		t.Errorf("receipt records %d remote failures", res.receipt.RemoteFailures)
	}
}

// poolScript is a Pool.map workload whose per-item cost is high enough that
// the shim's adaptive chunker produces several chunks (a single chunk would
// be dealt entirely to the local side and nothing would ever ship).
const poolScript = `
import multiprocessing, time

OFFSET = 7

def work(x):
    time.sleep(0.02)
    return x * x + OFFSET

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(work, list(range(64)))
    want = [x * x + OFFSET for x in range(64)]
    assert got == want, "wrong results: %r" % (got[:8],)
    print("POOL-OK")
`

// TestShimPoolDistributesMainModuleFunc is the regression test for the bug
// this whole effort started from: a kernel defined in the script's own
// __main__, which the by-reference protocol can never resolve on a worker.
func TestShimPoolDistributesMainModuleFunc(t *testing.T) {
	res := runPoolScript(t, map[string]string{"job.py": poolScript}, "job.py")
	assertDistributed(t, res, "POOL-OK")
	if res.stats.ByRef > 0 {
		t.Errorf("shim still shipped %d chunks by reference; source shipping is the contract", res.stats.ByRef)
	}
}

// TestShimPoolDistributesAdjacentModuleFunc covers the second half of the
// report: a kernel imported from a sibling file, which the worker cannot
// import because the workspace is never on its path.
func TestShimPoolDistributesAdjacentModuleFunc(t *testing.T) {
	files := map[string]string{
		"kernel.py": `
import time

SCALE = 3

def helper(x):
    return x * SCALE

def work(x):
    time.sleep(0.02)
    return helper(x) + 1
`,
		"job.py": `
import multiprocessing
from kernel import work

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(work, list(range(64)))
    want = [x * 3 + 1 for x in range(64)]
    assert got == want, "wrong results: %r" % (got[:8],)
    print("POOL-OK")
`,
	}
	res := runPoolScript(t, files, "job.py")
	assertDistributed(t, res, "POOL-OK")
}

// TestShimPoolStarmapDistributes covers the starmap path, which ships items
// as argument tuples.
func TestShimPoolStarmapDistributes(t *testing.T) {
	script := `
import multiprocessing, time

def work(a, b):
    time.sleep(0.02)
    return a * b

if __name__ == "__main__":
    pairs = [(i, i + 1) for i in range(64)]
    with multiprocessing.Pool(4) as pool:
        got = pool.starmap(work, pairs)
    want = [a * b for a, b in pairs]
    assert got == want, "wrong results: %r" % (got[:8],)
    print("STARMAP-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	assertDistributed(t, res, "STARMAP-OK")
}

// TestShimPoolUnshippableStaysLocalQuietly pins the other half of the
// contract: a kernel that stock multiprocessing runs happily but that cannot
// be rebuilt from source on a worker. functools.partial is the everyday case
// — it pickles locally by reference to a module function, but has no source
// of its own. Interception must decline it, stay correct, stay quiet, and say
// so in the receipt. Before serialisation moved inside the try, a callable
// the shim could not ship raised out of the dispatch thread and printed a
// traceback the user never asked for.
func TestShimPoolUnshippableStaysLocalQuietly(t *testing.T) {
	script := `
import functools, multiprocessing, time

def scaled(factor, x):
    time.sleep(0.02)
    return x * factor

if __name__ == "__main__":
    kernel = functools.partial(scaled, 2)
    with multiprocessing.Pool(4) as pool:
        got = pool.map(kernel, list(range(64)))
    assert got == [x * 2 for x in range(64)], "wrong results"
    print("LAMBDA-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	if !strings.Contains(res.stdout, "LAMBDA-OK") {
		t.Fatalf("missing sentinel\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if strings.Contains(res.stderr, "Traceback") {
		t.Errorf("unshippable kernel leaked a traceback:\n%s", res.stderr)
	}
	if res.stats.Errors > 0 {
		t.Errorf("daemon saw %d errors for a kernel that should never have been sent (last: %s)",
			res.stats.Errors, res.stats.LastError)
	}
	if res.receipt.Unshippable == 0 {
		t.Errorf("receipt does not account for the unshippable kernel: %+v", res.receipt)
	}
}

// TestShimExecutorAPISurvivesInterception covers the futures API the shim
// replaces. Binding _ClusterPool straight onto ProcessPoolExecutor left it
// without submit() or shutdown() and with the wrong constructor keyword, so
// any job using the executor for anything but a single-iterable map() failed
// outright — a crash, not a slowdown, and no test noticed.
func TestShimExecutorAPISurvivesInterception(t *testing.T) {
	script := `
import concurrent.futures as cf
import time

def work(x):
    time.sleep(0.02)
    return x * x

def add(a, b):
    return a + b

if __name__ == "__main__":
    with cf.ProcessPoolExecutor(max_workers=4) as ex:
        fut = ex.submit(work, 7)
        assert fut.result() == 49, fut.result()
        assert list(ex.map(work, range(64))) == [x * x for x in range(64)]
        assert list(ex.map(add, [1, 2, 3], [10, 20, 30])) == [11, 22, 33]
        done, _ = cf.wait([ex.submit(work, 3)])
        assert [f.result() for f in done] == [9]
    print("EXEC-OK")
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	assertDistributed(t, res, "EXEC-OK")
}

// TestShimPoolSingleNodeDoesNotDialItself guards the wasteful path: with one
// shard (peers + 1 == 1, i.e. nobody but us) the shim must not post work to
// its own daemon, wait for it, and then re-run it locally.
func TestShimPoolSingleNodeDoesNotDialItself(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	url, stats := startPoolFakeDaemon(t, python)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(poolScript), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(dir, "job.py"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL="+url,
		"PIPEDPEER_NUM_SHARDS=1", // no peers: this node is the whole cluster
		"PIPEDPEER_STORE_PATH=/nix/store/fake",
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("script failed: %v\nstderr:\n%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "POOL-OK") {
		t.Fatalf("missing sentinel\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	if got := stats(t).Requests; got != 0 {
		t.Errorf("single-node run dialled the daemon %d times; it has nowhere to spill to", got)
	}
}

// TestShimPoolSizesItselfNotAsAsked covers the promise that nobody should
// have to pick a worker count. A hand-typed number is a guess about the
// author's machine; the shim uses what this one can actually run. The escape
// hatch matters just as much: some counts encode correctness, not speed, and
// those break rather than slow down when overridden.
func TestShimPoolSizesItselfNotAsAsked(t *testing.T) {
	script := `
import multiprocessing, os, time

def work(x):
    time.sleep(0.01)
    return x

if __name__ == "__main__":
    with multiprocessing.Pool(2) as pool:
        got = pool.map(work, list(range(32)))
        width = pool._procs
    assert got == list(range(32)), "wrong results"
    print("WIDTH %d CORES %d" % (width, os.cpu_count()))
`
	res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
	var width, cores int
	if _, err := fmt.Sscanf(strings.TrimSpace(res.stdout), "WIDTH %d CORES %d", &width, &cores); err != nil {
		t.Fatalf("unexpected output %q (stderr: %s)", res.stdout, res.stderr)
	}
	if cores > 2 && width <= 2 {
		t.Errorf("pool honoured the requested 2 workers on a %d-core box; the point is not having to pick", cores)
	}
	if width < 1 {
		t.Errorf("nonsensical width %d", width)
	}
}

// TestShimPoolRespectsSizeWhenAsked pins the opt-out.
func TestShimPoolRespectsSizeWhenAsked(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	script := `
import multiprocessing

def work(x):
    return x * 2

if __name__ == "__main__":
    with multiprocessing.Pool(2) as pool:
        assert pool.map(work, [1, 2, 3]) == [2, 4, 6]
        print("WIDTH %d" % pool._procs)
`
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(dir, "job.py"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_RESPECT_POOL_SIZE=1",
		"PIPEDPEER_NUM_SHARDS=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "WIDTH 2") {
		t.Errorf("opt-out ignored; wanted WIDTH 2, got %q", out)
	}
}

// TestShimImapStaysLazy pins imap's contract. The shim used to materialise
// the whole iterable before returning anything, which holds every item and
// result in memory at once and turns an unbounded generator — which imap
// explicitly supports — into a hang. Taking a few results from an infinite
// source must return, not spin forever.
func TestShimImapStaysLazy(t *testing.T) {
	script := `
import itertools, multiprocessing

def work(x):
    return x * 2

if __name__ == "__main__":
    seen = []
    with multiprocessing.Pool(4) as pool:
        # An endless source: materialising it never finishes.
        for v in pool.imap(work, itertools.count()):
            seen.append(v)
            if len(seen) >= 8:
                break
        assert seen == [x * 2 for x in range(8)], seen
        # A finite source still returns everything, in order.
        assert list(pool.imap(work, range(40))) == [x * 2 for x in range(40)]
    print("IMAP-OK")
`
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(dir, "job.py"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir, "PIPEDPEER_SHIM=1", "PIPEDPEER_NUM_SHARDS=0",
		"PIPEDPEER_IMAP_BATCH=4",
	)
	done := make(chan struct{})
	var out []byte
	go func() { out, _ = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("imap over an endless source never returned: it is still materialising")
	}
	if !strings.Contains(string(out), "IMAP-OK") {
		t.Fatalf("imap test failed:\n%s", out)
	}
}
