package nixgen

// Pool sizing, laziness and receipt tests; helpers live in pooldist_test.go.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

# The harness keeps every other test's own pool size; this one is about
# not keeping it.
os.environ.pop("PIPEDPEER_RESPECT_POOL_SIZE", None)

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
	if err := WriteShimDir(dir); err != nil {
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
		"PIPEDPEER_CLOSURE_NODES=0",
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
	if err := WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(dir, "job.py"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir, "PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=0",
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

// TestTheReceiptAccountsForEveryItem: local_items + remote_items must cover
// the whole map. The race's local share (and the stragglers it re-runs for a
// failed chunk) went uncounted, so a lab run of 80 items reported 44. The
// receipt counts executions, so an item both sides ran counts twice: the sum
// may exceed the map but never fall short. The
// "remote fails" case stands in for a worker killed mid-run: the fake daemon
// has no PIPEDPEER_SHIM in its environment, so odd items raise only there.
// The "returns None" case pins the slot sentinel: None used to read as
// "not filled yet", and the straggler loop re-ran those items forever.
func TestTheReceiptAccountsForEveryItem(t *testing.T) {
	const tmpl = `
import multiprocessing, os, time

FAIL_REMOTE = %s

def work(x):
    time.sleep(0.02)
    if FAIL_REMOTE and x %% 2 and not os.environ.get("PIPEDPEER_SHIM"):
        raise RuntimeError("worker killed")
    return %s

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(work, list(range(64)))
    assert got == [work(x) for x in range(64)], "wrong results: %%r" %% (got[:8],)
    print("POOL-OK")
`
	for _, tc := range []struct {
		name, fail, ret string
		wantFailures    bool
	}{
		{"all remote ok", "False", "x * x", false},
		{"remote chunks fail", "True", "x * x", true},
		{"kernel returns None", "False", "None", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := fmt.Sprintf(tmpl, tc.fail, tc.ret)
			res := runPoolScript(t, map[string]string{"job.py": script}, "job.py")
			if !strings.Contains(res.stdout, "POOL-OK") {
				t.Fatalf("missing sentinel\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
			}
			if len(res.receipt.Parts) == 0 {
				t.Fatalf("no chunk was offered to the cluster, so the race never ran: %+v\nstderr:\n%s", res.receipt, res.stderr)
			}
			if got := res.receipt.LocalItems + res.receipt.RemoteItems; got < 64 {
				t.Errorf("receipt covers %d of 64 items (local %d, remote %d): %+v",
					got, res.receipt.LocalItems, res.receipt.RemoteItems, res.receipt)
			}
			if tc.wantFailures {
				// Every remote chunk failed: this machine ran all 64, exactly once.
				if res.receipt.RemoteFailures == 0 || res.receipt.RemoteItems != 0 || res.receipt.LocalItems != 64 {
					t.Errorf("want remote failures, remote 0, local 64: %+v", res.receipt)
				}
			} else if res.receipt.RemoteFailures > 0 || res.receipt.RemoteItems == 0 {
				t.Errorf("want remote work and no failures: %+v\nstderr:\n%s", res.receipt, res.stderr)
			}
		})
	}
}

// A pinned job must size its pool to the cores it may run on, not the
// machine's total: on a 12-core host pinned to one core, a 12-wide pool only
// time-slices that core twelve ways.
func TestPoolWidthFollowsCPUPinning(t *testing.T) {
	runShimPython(t, "pinned", `
import os
import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_resources

if not hasattr(os, "sched_setaffinity"):
    print("PINNED-OK")
    raise SystemExit(0)
allowed = sorted(os.sched_getaffinity(0))
assert core._usable_cores() <= len(allowed), core._usable_cores()
os.sched_setaffinity(0, {allowed[0]})
assert core._usable_cores() == 1, "pinned to one core, sized for %d" % core._usable_cores()
os.environ.pop("PIPEDPEER_RESPECT_POOL_SIZE", None)
assert pool_resources._pool_width(None) == 1, pool_resources._pool_width(None)
print("PINNED-OK")
`, "PINNED-OK")
}

// A pool's own workers must not count against it. _resize measures free memory
// with them running, so on a memory-bound machine every map found less room
// than the pool was built with and rebuilt it narrower, paying for a second
// pool start in the middle of the job.
func TestPoolResizeCountsItsOwnWorkersAsFree(t *testing.T) {
	runShimPython(t, "reclaim", `
import os
import sitecustomize as shim
from pipedpeer_shim import core, stats, transfer, numpy_ops, pandas_ops, pool, ddp, pool_resources

os.environ.pop("PIPEDPEER_RESPECT_POOL_SIZE", None)
core._usable_cores = lambda: 2
budget = int(2 * (256 << 20) / 0.4) + 1     # exactly room for two workers
pool_resources._avail_bytes = lambda: budget

def inc(x):
    return x + 1

if __name__ == "__main__":
    pool = pool._ClusterPool()
    assert pool._procs == 2, pool._procs
    held = pool._workers_memory()
    assert held > 0, "a running pool reported no memory held by its workers"
    # The workers now hold memory the machine no longer reports as free.
    pool._avail_bytes = lambda: budget - held
    pool._resize()
    assert pool._procs == 2, "shrank to %d over its own workers' memory" % pool._procs
    assert pool.map(inc, range(4)) == [1, 2, 3, 4]
    pool.close()
    print("RECLAIM-OK")
`, "RECLAIM-OK")
}

// The receipt says what the cost model decided and why, not only what ran
// where: when a run that should have spread out stays home, the reason is the
// first thing anyone asks for.
func TestReceiptRecordsWhyWorkShippedOrStayed(t *testing.T) {
	res := runPoolScript(t, map[string]string{"job.py": poolScript}, "job.py")
	assertDistributed(t, res, "POOL-OK")
	shipped := false
	for _, d := range res.receipt.Decisions {
		if d.Op == "pool.map" && d.Shipped && strings.Contains(d.Why, "worth a round trip") {
			shipped = true
		}
	}
	if !shipped {
		t.Errorf("a distributed map left no 'shipped' decision with its reason: %+v", res.receipt.Decisions)
	}
	// Every item is accounted for exactly once: those that ran remotely and
	// those whose local result was kept. Items the local cores computed while
	// racing the cluster were missing, and the total fell short of the map.
	// The fake daemon runs remote parts on the origin, so "local" includes them.
	if got := res.receipt.RemoteItems + res.receipt.LocalItems; got < 64 {
		t.Errorf("receipt accounts for %d items of a 64-item map: %+v", got, res.receipt)
	}

	tiny := runPoolScript(t, map[string]string{"job.py": `
import multiprocessing

def inc(x):
    return x + 1

if __name__ == "__main__":
    with multiprocessing.Pool(2) as pool:
        print("TINY-OK", sum(pool.map(inc, range(40))))
`}, "job.py")
	kept := false
	for _, d := range tiny.receipt.Decisions {
		if d.Op == "pool.map" && !d.Shipped && strings.Contains(d.Why, "under") {
			kept = true
		}
	}
	if !kept {
		t.Errorf("a map too small to ship left no 'kept local' decision: %+v\nstderr:\n%s",
			tiny.receipt.Decisions, tiny.stderr)
	}
}
