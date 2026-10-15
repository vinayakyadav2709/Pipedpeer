package daemonapi

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// U32's rank script: a step that computes for ~2 ms, so a loopback sync of
// a few hundred microseconds or more makes DiLoCo (2 ms + sync/dilocoEvery(2)) beat both
// the exact split (2 ms + sync) and one rank alone (4 ms) by the margin.
const ddpAutoScript = `
import os, time, torch
torch.manual_seed(0)
model = torch.nn.Linear(8, 1)
opt = torch.optim.SGD(model.parameters(), lr=0.01)
torch.manual_seed(int(os.environ["PIPEDPEER_RANK"]) + 1)
x, y = torch.randn(64, 8), torch.randn(64, 1)
for _ in range(60):
    opt.zero_grad()
    torch.nn.functional.mse_loss(model(x), y).backward()
    time.sleep(0.002)
    opt.step()
print("RANK-OK")
`

var switchedAt = regexp.MustCompile(`switching to DiLoCo after (\d+) optimizer step`)

// TestShimAutoSwitchesToDiLoCoAtTheSameStep: with --ddp-sync unset, the
// lead's verdict reaches both ranks in the reply to the same sync; both
// switch after the same step and rank 0 prints the line. --ddp-sync exact
// pins the ring: no switch.
func TestShimAutoSwitchesToDiLoCoAtTheSameStep(t *testing.T) {
	old := rebalanceEvery
	rebalanceEvery = 5
	defer func() { rebalanceEvery = old }()
	dir, run := ddpCoreSetup(t)
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(ddpAutoScript), 0o644); err != nil {
		t.Fatal(err)
	}
	verbose := []string{"PIPEDPEER_VERBOSE=1"}

	outs, errs := ddpRing(t, run, verbose, verbose)
	var at [2]string
	for rank, out := range outs {
		if errs[rank] != nil || !strings.Contains(out, "RANK-OK") {
			t.Fatalf("rank %d: %v\n%s", rank, errs[rank], out)
		}
		m := switchedAt.FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("rank %d never switched to DiLoCo:\n%s", rank, out)
		}
		at[rank] = m[1]
		// The lead's interval for a ring of two (DILOCO-MANY), not the shim's own.
		if !strings.Contains(out, fmt.Sprintf("training locally, syncing every %d steps", dilocoEvery(2))) {
			t.Errorf("rank %d switched but never trained locally:\n%s", rank, out)
		}
	}
	if at[0] != at[1] {
		t.Errorf("ranks switched at different steps: %s and %s", at[0], at[1])
	}
	if !strings.Contains(outs[0], "pipedpeer: training with DiLoCo") ||
		!strings.Contains(outs[0], "--ddp-sync exact") {
		t.Errorf("rank 0 did not say it switched and how to undo it:\n%s", outs[0])
	}
	if strings.Count(outs[0], "pipedpeer: training with DiLoCo") != 1 {
		t.Errorf("the line was printed more than once:\n%s", outs[0])
	}

	pinned := []string{"PIPEDPEER_VERBOSE=1", "PIPEDPEER_DDP_SYNC_MODE=exact"}
	outs, errs = ddpRing(t, run, pinned, pinned)
	for rank, out := range outs {
		if errs[rank] != nil {
			t.Fatalf("pinned rank %d: %v\n%s", rank, errs[rank], out)
		}
		if switchedAt.MatchString(out) {
			t.Errorf("pinned rank %d switched:\n%s", rank, out)
		}
	}
}
