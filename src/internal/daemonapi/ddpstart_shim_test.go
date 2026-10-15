package daemonapi

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// TestTheStartBarrierIsNotSyncTime is V4 80: rank 0 waited 146 s at the start
// barrier in silence, then counted the wait as sync - "36202 ms per sync",
// advice to average every 32 steps, "146.0s of sync". Rank 1 starts late here:
// rank 0 must say whom it waits for, report the wait on its own line, and
// keep it out of the sync total.
func TestTheStartBarrierIsNotSyncTime(t *testing.T) {
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
	script := `
import torch
torch.manual_seed(0)
model = torch.nn.Linear(4, 1)
opt = torch.optim.SGD(model.parameters(), lr=0.01)
X = torch.randn(64, 4)
for i in range(0, 64, 8):
    opt.zero_grad()
    torch.nn.functional.mse_loss(model(X[i:i+8]), X[i:i+8].sum(1, keepdim=True)).backward()
    opt.step()
print("DONE")
`
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	start := func(rank int, out *bytes.Buffer) *exec.Cmd {
		cmd := exec.Command(python, filepath.Join(dir, "job.py"))
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "PIPEDPEER_SHIM=1", "PIPEDPEER_CLOSURE_NODES=0",
			"PIPEDPEER_DDP=1", fmt.Sprintf("PIPEDPEER_RANK=%d", rank), "PIPEDPEER_WORLD_SIZE=2",
			"PIPEDPEER_VERBOSE=1", "PIPEDPEER_DDP_SYNC="+ts.URL+"/v1/ddp/sync",
			"PIPEDPEER_DDP_GROUP=g", "PIPEDPEER_DDP_HOSTS=laptop,gpu1", "PIPEDPEER_DDP_START_WAIT=120")
		cmd.Stdout, cmd.Stderr = out, out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	var out0, out1 bytes.Buffer
	r0 := start(0, &out0)
	defer func() { _ = r0.Process.Kill() }()
	time.Sleep(6 * time.Second) // rank 1's closure "still arriving"
	r1 := start(1, &out1)
	defer func() { _ = r1.Process.Kill() }()
	for i, c := range []*exec.Cmd{r0, r1} {
		done := make(chan error, 1)
		go func() { done <- c.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("rank %d: %v\n%s\n%s", i, err, out0.String(), out1.String())
			}
		case <-time.After(120 * time.Second):
			t.Fatalf("rank %d never finished:\n%s", i, out0.String())
		}
	}

	got := out0.String()
	if !regexp.MustCompile(`waiting for rank 1 on gpu1 to be ready .*up to 2 min`).MatchString(got) {
		t.Errorf("rank 0 never said whom it waited for:\n%s", got)
	}
	m := regexp.MustCompile(`waited (\d+) s at the start`).FindStringSubmatch(got)
	if m == nil {
		t.Fatalf("no line for the barrier wait:\n%s", got)
	}
	waited, _ := strconv.Atoi(m[1])
	sm := regexp.MustCompile(`(\d+) sync\(s\), ([\d.]+)s total`).FindStringSubmatch(got)
	if sm == nil {
		t.Fatalf("no sync summary:\n%s", got)
	}
	total, _ := strconv.ParseFloat(sm[2], 64)
	if waited < 3 || total >= float64(waited)/2 {
		t.Errorf("waited %d s, sync total %.1f s: the barrier is still counted as sync\n%s", waited, total, got)
	}
	if regexp.MustCompile(`averaged \d+ of \d+ ranks`).MatchString(got) {
		t.Errorf("a short round was reported though every rank made the barrier:\n%s", got)
	}
}
