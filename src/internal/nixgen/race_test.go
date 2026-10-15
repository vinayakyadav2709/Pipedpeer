package nixgen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runRaceProbe drives the shim's _ClusterPool._race directly, stubbing
// _remote_chunk, and asserts results equal a plain local map. mode controls the
// remote stub: "ok" (all chunks succeed) or "dead" (every chunk fails).
//
// The stub tags the values it produces, so the two modes are distinguishable.
// They were not before: the "ok" stub computed results in-process and the test
// asserted only that the numbers were right, which the local side guarantees
// on its own — so the passing test said nothing about whether a remote result
// had ever reached the caller.
func runRaceProbe(t *testing.T, mode string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}

	src := `
import sys, time
sys.path.insert(0, sys.argv[1])
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_func, pool_race

def double(x):
    time.sleep(0.01)
    return x * 2

mode = sys.argv[2]

def main():
    items = list(range(40))

    class StubPool(pool._ClusterPool):
        def _remote_chunk(self, payload, chunk, starmap):
            if mode == "dead":
                return None
            # Tagged, and instant: the local side is sleeping, so these should
            # win their slots and still be there at the end.
            return [(i, ("R", v * 2)) for i, v in chunk]

    payload = pool_func._func_payload(double)
    assert payload is not None, "a module-level function must be shippable"

    p = StubPool(processes=2)
    p._remote = True
    r = p._race(double, items, False, 0.01, payload)
    p.close()

    plain = [v[1] if isinstance(v, tuple) else v for v in r]
    assert plain == list(map(lambda x: x * 2, items)), (plain[:8],)
    remote_filled = [i for i, v in enumerate(r) if isinstance(v, tuple)]
    if mode == "dead":
        assert not remote_filled, "dead remote still filled slots %r" % (remote_filled,)
    else:
        assert remote_filled, "no remote result survived into the output"
    print("race-ok")

if __name__ == "__main__":
    main()
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script, dir, mode)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("race probe (mode=%s) failed:\n%s\n%v", mode, out, err)
	}
	if !strings.Contains(string(out), "race-ok") {
		t.Fatalf("race probe (mode=%s) unexpected output: %q", mode, out)
	}
}

func TestShimRaceCorrectWithRemote(t *testing.T) {
	runRaceProbe(t, "ok")
}

func TestShimRaceCorrectWithDeadRemote(t *testing.T) {
	// A dead remote must not change the result: local re-runs every straggler.
	runRaceProbe(t, "dead")
}

// TestShimRaceAdaptiveChunk sanity-checks the adaptive chunk sizing bounds.
func TestShimRaceAdaptiveChunk(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	src := `
import sys
sys.path.insert(0, sys.argv[1])
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_func, pool_race
# cheap item -> big chunk, costly item -> small chunk, bounded
assert pool_func._adaptive_chunk(0.0001) == 256
assert pool_func._adaptive_chunk(1.0) == 1
assert pool_func._adaptive_chunk(0) == 64
assert pool_func._chunk(list(range(10)), 0) == [list(range(10))]
print("chunk-ok")
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(python, script, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("chunk probe failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "chunk-ok") {
		t.Fatalf("chunk probe unexpected output: %q", out)
	}
}

// A faster cluster must take more than half. Chunks used to be dealt
// alternately before anything ran, so this machine owned half the items
// whatever the cluster could do, and adding workers stopped helping once
// they had finished their half: 00_pool took 23 s on one, two and three
// workers alike.
func TestShimRaceSharesFollowSpeed(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	src := `
import sys, time
sys.path.insert(0, sys.argv[1])
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_func, pool_race

def slow(vals):                      # one local lane, 50 ms an item
    out = []
    for v in vals:
        time.sleep(0.05)
        out.append(("L", v))
    return out

def fast(chunk):                     # the cluster: 5 ms a chunk
    time.sleep(0.005)
    return [(i, ("R", v)) for i, v in chunk]

items = list(range(80))
r = pool_race._race_items(items, 0.05, 1, slow, fast)
assert [v for _, v in r] == items, r[:5]
remote = sum(1 for tag, _ in r if tag == "R")
assert remote > len(items) * 3 // 4, "the cluster took only %d of %d" % (remote, len(items))
print("shares-ok")
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(python, script, dir).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "shares-ok") {
		t.Fatalf("shares probe failed:\n%s\n%v", out, err)
	}
}

// TestARaceLeavesNoChunkBehind: once every item has its result, the race is
// over. A peer still holding a chunk is a loser: it must not hold the
// interpreter open at exit (its request can take up to the 600 s timeout),
// and no peer thread may take another chunk, or that request lands in the
// script's next call. Its counted result must not be counted as remote work
// either (S3): a chunk whose receipt says it ran on the origin is not the
// cluster's.
func TestARaceLeavesNoChunkBehind(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	src := `
import sys, threading, time
sys.path.insert(0, sys.argv[1])
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_func, pool_race

def run_local(vals):
    time.sleep(0.005 * len(vals))
    return [v * 2 for v in vals]

# A peer that answers instantly but ran the work on the origin node.
def on_origin(chunk):
    out = pool_race._RemotePairs((i, v * 2) for i, v in chunk)
    out.elsewhere = 0
    return out

r = pool_race._race_items(list(range(40)), 0.005, 2, run_local, on_origin)
assert r == [v * 2 for v in range(40)], r[:8]
assert stats._STATS["remote_used"] == 0, stats._STATS["remote_used"]

# A peer that never answers in time.
started = []
lock = threading.Lock()

def stuck(chunk):
    with lock:
        started.append(time.monotonic())
    time.sleep(60)
    return None

r = pool_race._race_items(list(range(400)), 0.005, 2, run_local, stuck)
assert r == [v * 2 for v in range(400)], r[:8]
n = len(started)
time.sleep(0.5)
assert len(started) == n, "a peer took %d chunk(s) after the race ended" % (len(started) - n)
print("leak-ok", flush=True)
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	out, err := exec.Command(python, script, dir).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "leak-ok") {
		t.Fatalf("leak probe failed:\n%s\n%v", out, err)
	}
	// The stuck peer sleeps 60 s; exit must not wait for it.
	if d := time.Since(t0); d > 40*time.Second {
		t.Errorf("the script took %s to exit: a losing peer chunk held the interpreter open", d)
	}
}
