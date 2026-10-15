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
	if err := WriteShimDir(dir); err != nil {
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
		"PIPEDPEER_CLOSURE_NODES=0",
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
