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

// pythonImportChecks caches pythonImports per interpreter and import list.
var pythonImportChecks sync.Map

// pythonImports reports whether python can import mods ("torch, numpy"),
// asking once per test run: each check is a fresh interpreter importing torch
// (~2-4 s), and every DDP shim test used to repeat it.
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

// ddpCoreSetup writes the shim and the rank script, and returns the work dir
// and a runner for one rank.
func ddpCoreSetup(t *testing.T) (string, func(env ...string) *exec.Cmd) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if out, err := pythonImports(python, "torch, numpy"); err != nil {
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

// ddpPartialScript runs one epoch at lr 0 over N samples in batches of 8 and
// prints the step count and sum over steps of (the step's applied gradient x
// the samples in that step): with the weights held still, that sum is the
// gradient summed over every sample once, the same on one machine as on the
// ring if and only if the ring trains every sample once and weighs each
// step's average by samples - the last partial batch included.
const ddpPartialScript = `
import os, torch
from torch.utils.data import DataLoader, TensorDataset
N, B = int(os.environ["TEST_N"]), 8
torch.manual_seed(0)
X, y = torch.randn(N, 4), torch.randn(N, 1)
model = torch.nn.Linear(4, 1)
opt = torch.optim.SGD(model.parameters(), lr=0.0)
acc, steps = torch.zeros_like(model.weight, dtype=torch.float64), 0
for k, (xb, yb) in enumerate(DataLoader(TensorDataset(X, y), batch_size=B)):
    opt.zero_grad()
    torch.nn.functional.mse_loss(model(xb), yb).backward()
    opt.step()
    acc += model.weight.grad.double() * min(B, N - k * B)
    steps += 1
print("STEPS %d" % steps)
print("ACC " + " ".join("%.12g" % v for v in acc.flatten().tolist()))
`

// TestShimRingTrainsTheLastPartialBatch is FIX-PARTIAL: U31B found exact sync
// 1.5% off one device on gpu.py because the ring dropped each epoch's last
// partial batch. N=70 leaves 6 samples past 8 full steps (split 3+3); N=65
// leaves 1, so one rank pads with a repeat that must weigh nothing.
func TestShimRingTrainsTheLastPartialBatch(t *testing.T) {
	dir, run := ddpCoreSetup(t)
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(ddpPartialScript), 0o644); err != nil {
		t.Fatal(err)
	}
	field := func(out, prefix string) string {
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, prefix) {
				return strings.TrimPrefix(line, prefix)
			}
		}
		return ""
	}
	for _, n := range []int{70, 65} {
		env := []string{fmt.Sprintf("TEST_N=%d", n), "PIPEDPEER_DDP_INT8=0", "PIPEDPEER_DDP_FP32=1"}
		alone, err := run(append([]string{"PIPEDPEER_RANK=0"}, env...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("N=%d alone: %v\n%s", n, err, alone)
		}
		outs, errs := ddpRing(t, run, env, env)
		for rank, err := range errs {
			if err != nil {
				t.Fatalf("N=%d rank %d: %v\n%s", n, rank, err, outs[rank])
			}
			if got, want := field(outs[rank], "STEPS "), field(string(alone), "STEPS "); got != want {
				t.Errorf("N=%d rank %d took %s steps, one machine %s", n, rank, got, want)
			}
			check := exec.Command("python3", "-c", `
import sys
a = [float(v) for v in sys.argv[1].split()]
b = [float(v) for v in sys.argv[2].split()]
assert len(a) == len(b) and a, (a, b)
for x, y in zip(a, b):
    assert abs(x - y) <= 1e-5 * max(1.0, abs(y)), (a, b)
print("MATCH")
`, field(outs[rank], "ACC "), field(string(alone), "ACC "))
			if out, err := check.CombinedOutput(); err != nil {
				t.Errorf("N=%d rank %d: the ring's summed gradient is not one machine's: %v\n%s\n%s",
					n, rank, err, out, outs[rank])
			}
		}
	}
}

// ddpTimedScript trains one epoch of 200 global steps (batch 8 split 4+4),
// sleeping TEST_SLOW seconds a step, and prints its step count and weights.
const ddpTimedScript = `
import os, time, torch
from torch.utils.data import DataLoader, TensorDataset
slow = float(os.environ.get("TEST_SLOW", "0"))
torch.manual_seed(0)
X, y = torch.randn(1600, 4), torch.randn(1600, 1)
model = torch.nn.Linear(4, 1)
opt = torch.optim.SGD(model.parameters(), lr=0.01)
steps = 0
for xb, yb in DataLoader(TensorDataset(X, y), batch_size=8, shuffle=True):
    opt.zero_grad()
    torch.nn.functional.mse_loss(model(xb), yb).backward()
    time.sleep(slow)
    opt.step()
    steps += 1
print("STEPS %d" % steps)
print("W " + " ".join("%.8g" % v for v in model.weight.flatten().tolist()))
`

// TestShimFastModeTimesRoundsForASlowRank is PHONE-DDP: under --ddp-sync
// fast, a rank 50x slower than the other runs out of its round budget,
// skips the round's remaining batches and syncs, so the fast rank is not
// held for the slow rank's whole interval; the ring stays in step (no
// aborted round) and ends the epoch with one model on both ranks.
func TestShimFastModeTimesRoundsForASlowRank(t *testing.T) {
	dir, run := ddpCoreSetup(t)
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(ddpTimedScript), 0o644); err != nil {
		t.Fatal(err)
	}
	// Slack 1.25: with two ranks the median is their mean, which the
	// default stall guard (4x) never trips on.
	common := []string{"PIPEDPEER_DDP_SYNC_MODE=fast", "PIPEDPEER_DILOCO_H=8", "PIPEDPEER_VERBOSE=1",
		"PIPEDPEER_DILOCO_ROUND_SLACK=1.25"}
	outs, errs := ddpRing(t, run, append([]string{"TEST_SLOW=0.001"}, common...),
		append([]string{"TEST_SLOW=0.05"}, common...))
	var weights [2]string
	for rank, err := range errs {
		if err != nil {
			t.Fatalf("rank %d: %v\n%s", rank, err, outs[rank])
		}
		for _, line := range strings.Split(outs[rank], "\n") {
			if strings.HasPrefix(line, "W ") {
				weights[rank] = line
			}
		}
	}
	if weights[0] == "" || weights[0] != weights[1] {
		t.Errorf("the ranks ended on different models: %q vs %q\n%s\n%s", weights[0], weights[1], outs[0], outs[1])
	}
	if !strings.Contains(outs[1], "over its") || !strings.Contains(outs[1], "skipped") {
		t.Errorf("the slow rank never cut a round short:\n%s", outs[1])
	}
	if strings.Contains(outs[0], "skipping") {
		t.Errorf("the fast rank skipped batches:\n%s", outs[0])
	}
}
