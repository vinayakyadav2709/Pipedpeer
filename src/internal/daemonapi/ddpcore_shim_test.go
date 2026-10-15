package daemonapi

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// The rank script for the shim tests below: one SGD step on rank-specific
// data, saving the weight and the gradient it applied.
const ddpCoreScript = `
import os, torch
rank = int(os.environ["PIPEDPEER_RANK"])
width = int(os.environ.get("TEST_WIDTH", "4"))
torch.manual_seed(0)
model = torch.nn.Linear(width, 1)
opt = torch.optim.SGD(model.parameters(), lr=0.1)
torch.manual_seed(rank + 1)
x, y = torch.randn(16, width), torch.randn(16, 1)
torch.nn.functional.mse_loss(model(x), y).backward()
opt.step()
torch.save({"w": model.weight.detach(), "g": model.weight.grad.detach()},
           os.path.join(os.environ["TEST_OUT"], "%s-%d.pt" % (os.environ["TEST_PREFIX"], rank)))
print("RANK-OK")
`

// ddpCoreSetup writes the shim and the rank script, and returns the work dir
// and a runner for one rank.
func ddpCoreSetup(t *testing.T) (string, func(env ...string) *exec.Cmd) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if out, err := exec.Command(python, "-c", "import torch, numpy").CombinedOutput(); err != nil {
		t.Skipf("torch/numpy not available: %s", out)
	}
	dir := t.TempDir()
	if err := nixgen.WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(ddpCoreScript), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, func(env ...string) *exec.Cmd {
		cmd := exec.Command(python, filepath.Join(dir, "job.py"))
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), append([]string{"PYTHONPATH=" + dir, "PIPEDPEER_SHIM=1",
			"PIPEDPEER_CLOSURE_NODES=0", "TEST_OUT=" + dir}, env...)...)
		return cmd
	}
}

// ddpRing runs two ranks against a real daemon and returns their outputs
// and errors.
func ddpRing(t *testing.T, run func(env ...string) *exec.Cmd, extra ...[]string) ([2]string, [2]error) {
	t.Helper()
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	var outs [2]string
	var errs [2]error
	var wg sync.WaitGroup
	for rank := 0; rank < 2; rank++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			env := []string{"PIPEDPEER_DDP=1", fmt.Sprintf("PIPEDPEER_RANK=%d", rank), "PIPEDPEER_WORLD_SIZE=2",
				"PIPEDPEER_DDP_SYNC=" + ts.URL + "/v1/ddp/sync", "PIPEDPEER_DDP_GROUP=g",
				"PIPEDPEER_DDP_HOSTS=laptop,gpu1", "PIPEDPEER_DDP_START_WAIT=60", "TEST_PREFIX=ring"}
			if rank < len(extra) {
				env = append(env, extra[rank]...)
			}
			out, err := run(env...).CombinedOutput()
			outs[rank], errs[rank] = string(out), err
		}(rank)
	}
	wg.Wait()
	return outs, errs
}

// TestShimDDPAveragesThroughTheDaemon replaces the gloo test: two ranks sync
// through the lead daemon on the default wire (int8 with error feedback) and
// end with identical weights, each having applied the average of the two
// single-process gradients to within int8's resolution.
func TestShimDDPAveragesThroughTheDaemon(t *testing.T) {
	dir, run := ddpCoreSetup(t)
	for rank := 0; rank < 2; rank++ {
		cmd := run(fmt.Sprintf("PIPEDPEER_RANK=%d", rank), "TEST_PREFIX=alone")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("rank %d alone: %v\n%s", rank, err, out)
		}
	}
	outs, errs := ddpRing(t, run)
	for rank, err := range errs {
		if err != nil {
			t.Fatalf("rank %d: %v\n%s", rank, err, outs[rank])
		}
	}
	check := exec.Command("python3", "-c", `
import os, sys, torch
d = sys.argv[1]
load = lambda n: torch.load(os.path.join(d, n))
r0, r1, a0, a1 = load("ring-0.pt"), load("ring-1.pt"), load("alone-0.pt"), load("alone-1.pt")
assert torch.equal(r0["w"], r1["w"]), "weights differ across ranks"
avg = (a0["g"] + a1["g"]) / 2
tol = 2 * max(a0["g"].abs().max(), a1["g"].abs().max()).item() / 127
for r in (r0, r1):
    err = (r["g"] - avg).abs().max().item()
    assert err <= tol, "grad off the average by %g (tolerance %g)" % (err, tol)
print("AVG-OK")
`, dir)
	if out, err := check.CombinedOutput(); err != nil || !strings.Contains(string(out), "AVG-OK") {
		t.Fatalf("%v\n%s", err, out)
	}
}

// TestShimMismatchedModelsExit76: rank 1 builds a wider model (the demo's
// CPU-vs-GPU script). Both ranks stop with exit 76 and one line naming the
// difference; neither trains on alone.
func TestShimMismatchedModelsExit76(t *testing.T) {
	_, run := ddpCoreSetup(t)
	outs, errs := ddpRing(t, run, nil, []string{"TEST_WIDTH=8"})
	for rank, err := range errs {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 76 {
			t.Errorf("rank %d ended with %v, want exit 76\n%s", rank, err, outs[rank])
			continue
		}
		if !strings.Contains(outs[rank], "pipedpeer: training stopped: the ranks built different models") {
			t.Errorf("rank %d never said why it stopped:\n%s", rank, outs[rank])
		}
		if strings.Contains(outs[rank], "RANK-OK") {
			t.Errorf("rank %d finished its script after the ring was aborted:\n%s", rank, outs[rank])
		}
	}
}
