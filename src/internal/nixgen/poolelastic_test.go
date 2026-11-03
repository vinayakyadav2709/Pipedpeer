package nixgen

import (
	"os/exec"
	"strings"
	"testing"
)

// The first job after a join: PIPEDPEER_CLOSURE_NODES=1 at exec (every peer
// was still importing the closure), and a peer holds it a moment into the
// map. The shim used to read that 1 once, when the Pool was built, and run
// the whole job here (rehearsal-day E14). These run the real shim, Pool.map
// in a real interpreter, against the out-of-process fake daemon.

const elasticMapScript = `
import time
from multiprocessing import Pool

def work(x):
    time.sleep(0.02)
    return x * 3 + 1

if __name__ == "__main__":
    with Pool(2) as p:
        out = p.map(work, range(200))
    assert out == [x * 3 + 1 for x in range(200)], out[:8]
    print("ELASTIC-OK")
`

func runElastic(t *testing.T, readyAfter string) poolRunResult {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	return runPoolScriptEnv(t, python, map[string]string{"job.py": elasticMapScript}, "job.py",
		[]string{"FAKE_READY_AFTER=" + readyAfter}, []string{"PIPEDPEER_CLOSURE_NODES=1"})
}

// TestShimMapUsesAPeerThatBecomesReadyMidMap: 200 x 20 ms on 2 local
// processes is 2 s; the peer holds the closure from 0.8 s after the daemon
// started. Work reaches it, and nothing was sent before it was ready.
func TestShimMapUsesAPeerThatBecomesReadyMidMap(t *testing.T) {
	res := runElastic(t, "0.8")
	if !strings.Contains(res.stdout, "ELASTIC-OK") {
		t.Fatalf("map failed\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if res.stats.Pacings == 0 {
		t.Fatal("the shim never asked the daemon how to pace the map")
	}
	if res.stats.Early != 0 {
		t.Fatalf("%d chunk(s) sent while no peer held the closure", res.stats.Early)
	}
	if res.stats.Items == 0 || res.receipt.RemoteItems == 0 {
		t.Fatalf("the peer that became ready mid-map got nothing: daemon ran %d items, receipt %+v\nstderr:\n%s",
			res.stats.Items, res.receipt, res.stderr)
	}
	if got := res.receipt.LocalItems + res.receipt.RemoteItems; got < 200 {
		t.Fatalf("receipt accounts for %d of 200 items: %+v", got, res.receipt)
	}
}

// TestShimMapStaysLocalWhenNobodyIsComing: alone, with no peer getting the
// closure, the map is a plain local Pool.map: nothing is sent.
func TestShimMapStaysLocalWhenNobodyIsComing(t *testing.T) {
	res := runElastic(t, "never")
	if !strings.Contains(res.stdout, "ELASTIC-OK") {
		t.Fatalf("map failed\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if res.stats.Requests != 0 {
		t.Fatalf("sent %d chunk(s) with no peer in the cluster", res.stats.Requests)
	}
}
