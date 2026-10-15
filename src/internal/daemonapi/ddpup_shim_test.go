package daemonapi

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// TestARankJoinsARunningRingThroughTheShim runs the real shim against the
// real handlers: two ranks train a DataLoader script, a third starts once
// they are under way and joins. It must skip to the ring's epoch, load the
// ring's model and optimizer, and from then on train in lockstep - so all
// three end with the same parameters, and the last epoch's shards are
// disjoint across all three.
func TestARankJoinsARunningRingThroughTheShim(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if out, err := exec.Command(python, "-c", "import torch, numpy").CombinedOutput(); err != nil {
		t.Skipf("torch/numpy not available: %s", out)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(nixgen.ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}
	script := `
import sys, time, torch
from torch.utils.data import DataLoader, TensorDataset

if __name__ == "__main__":
    torch.manual_seed(0)
    X = torch.randn(240, 4)
    y = X.sum(1, keepdim=True)
    loader = DataLoader(TensorDataset(torch.arange(240), X, y), batch_size=8, shuffle=True)
    # BatchNorm: running stats the joiner must take from the ring, not from
    # its catch-up batches. GradScaler: asserts after a backward that did nothing.
    model = torch.nn.Sequential(torch.nn.BatchNorm1d(4), torch.nn.Linear(4, 1))
    scaler = torch.amp.GradScaler("cpu")
    opt = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.9)
    # Stepped once an epoch, the empty ones a joiner skips included: those
    # steps must not decay the rate the joiner took from the ring a second time.
    sched = torch.optim.lr_scheduler.ExponentialLR(opt, 0.8)
    for epoch in range(int(sys.argv[1])):
        seen = []
        for idx, xb, yb in loader:
            opt.zero_grad()
            scaler.scale(torch.nn.functional.mse_loss(model(xb), yb)).backward()
            scaler.step(opt)
            scaler.update()
            seen.extend(idx.tolist())
            time.sleep(0.1)
        sched.step()
        # The loop's variables, which an epoch a joiner skips must still bind.
        print("EPOCH", epoch, len(seen), len(xb), flush=True)
    print("SEEN", " ".join(str(i) for i in sorted(seen)))
    print("PARAMS", " ".join("%.6f" % v for p in model.parameters() for v in p.detach().flatten().tolist()))
`
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	type rankRun struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	start := func(rank, world int, join bool) *rankRun {
		r := &rankRun{cmd: exec.Command(python, filepath.Join(dir, "job.py"), "10")}
		r.cmd.Dir = dir
		r.cmd.Env = append(os.Environ(),
			"PYTHONPATH="+dir,
			"PIPEDPEER_SHIM=1",
			"PIPEDPEER_CLOSURE_NODES=0",
			"PIPEDPEER_DDP=1",
			fmt.Sprintf("PIPEDPEER_RANK=%d", rank),
			fmt.Sprintf("PIPEDPEER_WORLD_SIZE=%d", world),
			"PIPEDPEER_DDP_BACKEND=daemon",
			"PIPEDPEER_DDP_SYNC="+ts.URL+"/v1/ddp/sync",
			"PIPEDPEER_DDP_GROUP=g",
		)
		if join {
			r.cmd.Env = append(r.cmd.Env, "PIPEDPEER_DDP_JOIN=1")
		}
		r.cmd.Stdout = &r.out
		r.cmd.Stderr = &r.out
		if err := r.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return r
	}
	ranks := []*rankRun{start(0, 2, false), start(1, 2, false)}
	defer func() {
		for _, r := range ranks {
			_ = r.cmd.Process.Kill()
		}
	}()

	// The submitter starts a joiner only once the ring is syncing.
	deadline := time.Now().Add(60 * time.Second)
	for {
		s.ddp.mu.Lock()
		run := s.ddp.runs["g"]
		ok := run != nil && run.lastSeq > 0
		s.ddp.mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ring never synced:\n%s\n%s", ranks[0].out.String(), ranks[1].out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	ranks = append(ranks, start(2, 3, true))

	for i, r := range ranks {
		done := make(chan error, 1)
		go func() { done <- r.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("rank %d failed: %v\n%s", i, err, r.out.String())
			}
		case <-time.After(120 * time.Second):
			t.Fatalf("rank %d never finished:\n%s", i, r.out.String())
		}
	}

	field := func(out, prefix string) string {
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, prefix) {
				return strings.TrimPrefix(line, prefix)
			}
		}
		return ""
	}
	joiner := ranks[2].out.String()
	if !strings.Contains(joiner, "joins the ring of 3") {
		t.Fatalf("the joiner never joined:\n%s", joiner)
	}
	if !strings.Contains(ranks[0].out.String(), "ring grows from 2 to 3") {
		t.Errorf("rank 0 never grew the ring:\n%s", ranks[0].out.String())
	}
	if !strings.Contains(ranks[0].out.String(), "handed the model and optimizer to joining rank 2") {
		t.Errorf("rank 0 never handed over the model:\n%s", ranks[0].out.String())
	}
	// A skipped epoch runs one placeholder batch, so the script's loop
	// variables exist, and its output is tagged as not trained.
	if field(joiner, "EPOCH 0 ") != "" || !strings.Contains(joiner, "[rank 2 catch-up, not trained] EPOCH 0 8 8") {
		t.Errorf("the joiner's skipped first epoch is not a tagged placeholder batch:\n%s", joiner)
	}

	if !strings.Contains(joiner, "took the ring's 3 buffer(s)") {
		t.Errorf("the joiner kept its own BatchNorm stats:\n%s", joiner)
	}

	if !strings.Contains(joiner, "restored to the ring's") {
		t.Errorf("the joiner never put back the ring's learning rate:\n%s", joiner)
	}

	p0 := field(ranks[0].out.String(), "PARAMS ")
	for i, r := range ranks[1:] {
		if p := field(r.out.String(), "PARAMS "); p == "" || p != p0 {
			t.Errorf("rank %d ends with parameters %q, rank 0 with %q: not in lockstep", i+1, p, p0)
		}
	}

	seen := map[string]int{}
	for i, r := range ranks {
		samples := strings.Fields(field(r.out.String(), "SEEN "))
		if len(samples) == 0 {
			t.Errorf("rank %d trained nothing in the last epoch:\n%s", i, r.out.String())
		}
		for _, v := range samples {
			seen[v]++
			if seen[v] > 1 {
				t.Fatalf("sample %s trained by two ranks in the last epoch", v)
			}
		}
	}
	if len(seen) < 240-24 {
		t.Errorf("the ring of three trained %d of 240 samples in its last epoch", len(seen))
	}
}
