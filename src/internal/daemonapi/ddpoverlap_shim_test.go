package daemonapi

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ddpOverlapScript trains 3 DataLoader epochs at SGD lr TEST_LR, momentum
// 0.9, and prints its parameters.
const ddpOverlapScript = `
import os, torch
torch.manual_seed(0)
g = torch.Generator().manual_seed(11)
x = torch.randn(4096, 16, generator=g)
y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] * x[:, 3] > 0.5).float().unsqueeze(1)
dl = torch.utils.data.DataLoader(torch.utils.data.TensorDataset(x, y), batch_size=64, shuffle=True,
                                 generator=torch.Generator().manual_seed(5))
m = torch.nn.Sequential(torch.nn.Linear(16, 64), torch.nn.BatchNorm1d(64), torch.nn.ReLU(),
                        torch.nn.Linear(64, 1))
opt = torch.optim.SGD(m.parameters(), lr=float(os.environ["TEST_LR"]), momentum=0.9)
for ep in range(3):
    for xb, yb in dl:
        opt.zero_grad()
        torch.nn.functional.mse_loss(m(xb), yb).backward()
        opt.step()
print("PARAMS", " ".join("%.6f" % v for p in m.parameters() for v in p.detach().flatten().tolist()))
`

func ddpOverlapRing(t *testing.T, lr string) ([2]string, [2]error) {
	t.Helper()
	dir, run := ddpCoreSetup(t)
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(ddpOverlapScript), 0o644); err != nil {
		t.Fatal(err)
	}
	env := []string{"PIPEDPEER_DDP_OVERLAP=1", "PIPEDPEER_DDP_FIXED=1", "PIPEDPEER_VERBOSE=1", "TEST_LR=" + lr}
	return ddpRing(t, run, env, env)
}

// TestShimOverlapTrainsInLockstep: with PIPEDPEER_DDP_OVERLAP=1 every
// gradient sync runs on the sync thread, one step late. Both ranks finish a
// multi-epoch DataLoader run and end with identical parameters.
func TestShimOverlapTrainsInLockstep(t *testing.T) {
	outs, errs := ddpOverlapRing(t, "0.005")
	for rank, err := range errs {
		if err != nil {
			t.Fatalf("rank %d: %v\n%s", rank, err, outs[rank])
		}
	}
	if !strings.Contains(outs[0], "overlapping each gradient sync") {
		t.Errorf("rank 0 never overlapped a sync:\n%s", outs[0])
	}
	params := func(out string) string {
		for _, line := range strings.Split(out, "\n") {
			if p, ok := strings.CutPrefix(line, "PARAMS "); ok {
				return p
			}
		}
		return ""
	}
	if p0, p1 := params(outs[0]), params(outs[1]); p0 == "" || p0 != p1 {
		t.Errorf("ranks end apart: %q vs %q\n%s\n%s", p0, p1, outs[0], outs[1])
	}
}

// TestShimOverlapDivergenceStopsWithTheReason (FIX-OVERLAP): at lr 0.05 and
// momentum 0.9 a one-step-late gradient diverges (plain torch with the same
// delay does too; without it the script trains) until a gradient is inf.
// Its int8 scale used to reach the lead as JSON Infinity: HTTP 400, "invalid
// character 'I'". A rank now stops with exit 76 and says why and what to
// change.
func TestShimOverlapDivergenceStopsWithTheReason(t *testing.T) {
	outs, errs := ddpOverlapRing(t, "0.05")
	said := false
	for rank, err := range errs {
		if err == nil {
			t.Fatalf("rank %d finished a diverged run:\n%s", rank, outs[rank])
		}
		if strings.Contains(outs[rank], "HTTP 400") {
			t.Errorf("rank %d sent a non-finite gradient to the lead:\n%s", rank, outs[rank])
		}
		if !strings.Contains(outs[rank], "hold inf or NaN") {
			continue
		}
		said = true
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 76 {
			t.Errorf("rank %d stopped with %v, want exit 76", rank, err)
		}
		if !strings.Contains(outs[rank], "run without it") {
			t.Errorf("rank %d does not name the overlap as the cause:\n%s", rank, outs[rank])
		}
	}
	if !said {
		t.Errorf("no rank said why it stopped:\n%s\n%s", outs[0], outs[1])
	}
}
