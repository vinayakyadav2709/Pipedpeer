package nixgen

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestShimLinalgOffloadTimesOut pins the V1 rehearsal hang: 02_numpy_heavy's
// svd went to the cluster and waited 15 minutes, because urllib's timeout is
// per socket read and the daemon tried four slow peers in turn. The offload
// must give up at its deadline, close the request (so the daemon drops it)
// and compute locally.
func TestShimLinalgOffloadTimesOut(t *testing.T) {
	runShimPythonEnv(t, "linalg_deadline", `
import os, select, socket, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CLOSED = threading.Event()

class Hang(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # Never answer; notice when the client goes away.
        end = time.monotonic() + 60
        while time.monotonic() < end:
            r, _, _ = select.select([self.connection], [], [], 0.1)
            if r and not self.connection.recv(1, socket.MSG_PEEK):
                CLOSED.set()
                return

srv = ThreadingHTTPServer(("127.0.0.1", 0), Hang)
srv.daemon_threads = True
threading.Thread(target=srv.serve_forever, daemon=True).start()

import sitecustomize as shim
import numpy as np
shim._ENABLED = True
shim._URL = "http://127.0.0.1:%d" % srv.server_address[1]
shim._STORE = ""
shim._BW_CACHE["bw"] = 1e9
shim._BW_CACHE["t"] = time.monotonic()
shim._LINALG_MIN_DEADLINE = 1.0
shim._numpy_should_offload = lambda *a, **k: True

A = np.random.RandomState(0).rand(256, 256)
t0 = time.monotonic()
U, S, Vh = np.linalg.svd(A)
took = time.monotonic() - t0
assert np.allclose((U * S) @ Vh, A), "svd answer wrong after the fallback"
assert took < 10, "svd waited %.1fs for a peer that never answered" % took
assert CLOSED.wait(5), "the timed-out request was left open; the daemon would keep the peer busy"
print("LINALG-DEADLINE-OK")
`, "LINALG-DEADLINE-OK", "PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}

// TestShimSingleWorkerOffloadUsesPerPeerBandwidth: the bandwidth probe
// spreads over every peer, so it measures the aggregate; a single-worker svd
// gets one link. The rehearsal's 4 peers measured 12.8 MB/s together and the
// 134 MB svd was shipped as if one peer had all of it.
func TestShimSingleWorkerOffloadUsesPerPeerBandwidth(t *testing.T) {
	runShimPythonEnv(t, "perpeer_bw", shimFakeDaemon+`
import time
nbytes = 4096 * 4096 * 8
shim._BW_CACHE["bw"] = 12.8e6
shim._BW_CACHE["t"] = time.monotonic()
shim._NUM_SHARDS = "5"   # four peers share the 12.8 MB/s
assert not shim._numpy_should_offload(nbytes, 1024, 3, False, 1.5e9), "svd shipped on the aggregate bandwidth"
shim._NUM_SHARDS = "2"   # one peer has all of it
shim._BW_CACHE["t"] = time.monotonic()
assert shim._numpy_should_offload(nbytes, 1024, 3, False, 1.5e9), "svd refused although one peer has the full link"
print("PERPEER-OK")
`, "PERPEER-OK", "PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=3", "PIPEDPEER_DAEMON_URL=http://127.0.0.1:1")
}

// TestShimSIGTERMAsPID1ExitsQuietly pins the ~50-line traceback after
// "cancelled": a cancel SIGTERMs the sandbox, the script is pid 1 there and
// ignores it, its Pool workers die and are respawned with tracebacks until
// SIGKILL. With the shim the script exits 143 at once and prints nothing.
func TestShimSIGTERMAsPID1ExitsQuietly(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if err := exec.Command("unshare", "-rpf", "--mount-proc", "true").Run(); err != nil {
		t.Skipf("no user+pid namespaces to make python pid 1: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "pool_sigterm.py")
	if err := os.WriteFile(script, []byte(`
import os, sys, time
from multiprocessing import Pool
def work(i):
    time.sleep(30)
    return i
if __name__ == "__main__":
    assert os.getpid() == 1, os.getpid()
    with Pool(2) as p:
        r = p.map_async(work, range(4))
        print("READY", flush=True)
        r.get()
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("unshare", "-rpf", "--mount-proc", python, script)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	sc := bufio.NewScanner(out)
	if !sc.Scan() || sc.Text() != "READY" {
		t.Fatalf("script did not start: %q, stderr %s", sc.Text(), stderr.String())
	}
	// The python is unshare's child; signal it from this (the parent)
	// namespace, as the daemon does.
	kids, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/task/" + strconv.Itoa(cmd.Process.Pid) + "/children")
	if err != nil {
		t.Skipf("cannot find the namespaced python: %v", err)
	}
	pid, err := strconv.Atoi(strings.Fields(string(kids))[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for sc.Scan() {
		}
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 143 {
			t.Errorf("exit = %v, want code 143", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("pid 1 ignored SIGTERM for 10 s; stderr so far:\n%s", stderr.String())
	}
	if n := strings.Count(strings.TrimSpace(stderr.String()), "\n"); n > 0 || strings.Contains(stderr.String(), "Traceback") {
		t.Errorf("SIGTERM printed %d+ lines:\n%s", n+1, stderr.String())
	}
}
