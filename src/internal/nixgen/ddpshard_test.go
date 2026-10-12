package nixgen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestDDPShardsDataAcrossRanks pins what data-parallel training has to mean.
//
// The shim only called set_epoch when the user had already built a
// DistributedSampler. Without one — the ordinary case, and what the bundled
// demo does — every rank iterated the entire dataset, so averaging gradients
// across ranks reproduced the single-process gradient exactly and N machines
// bought nothing but a slower step. It looked like working DDP: losses
// matched across ranks, the run finished, nothing failed.
//
// So the assertion is disjointness, not "it ran".
func TestDDPShardsDataAcrossRanks(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if out, err := exec.Command(python, "-c", "import torch").CombinedOutput(); err != nil {
		t.Skipf("torch not available: %s", out)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0o644); err != nil {
		t.Fatal(err)
	}

	script := `
import torch
from torch.utils.data import DataLoader, TensorDataset

if __name__ == "__main__":
    n = 40
    ds = TensorDataset(torch.arange(n).float().unsqueeze(1))
    loader = DataLoader(ds, batch_size=4)
    seen = []
    for (batch,) in loader:
        seen.extend(int(v.item()) for v in batch.flatten())
    print("SEEN " + " ".join(str(v) for v in sorted(seen)))
`
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}

	runRank := func(rank string) []string {
		t.Helper()
		cmd := exec.Command(python, filepath.Join(dir, "job.py"))
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"PYTHONPATH="+dir,
			"PIPEDPEER_SHIM=1",
			"PIPEDPEER_NUM_SHARDS=0",
			"PIPEDPEER_DDP=1",
			"PIPEDPEER_RANK="+rank,
			"PIPEDPEER_WORLD_SIZE=2",
			"PIPEDPEER_DDP_BACKEND=daemon",
			// No sync endpoint: this is about which samples a rank reads,
			// not about gradient exchange.
			"PIPEDPEER_DDP_SYNC=",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("rank %s failed: %v\n%s", rank, err, out)
		}
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "SEEN ") {
				return strings.Fields(strings.TrimPrefix(line, "SEEN "))
			}
		}
		t.Fatalf("rank %s printed no sample list:\n%s", rank, out)
		return nil
	}

	r0, r1 := runRank("0"), runRank("1")
	if len(r0) == 0 || len(r1) == 0 {
		t.Fatalf("a rank read nothing: %d and %d samples", len(r0), len(r1))
	}

	set0 := map[string]bool{}
	for _, v := range r0 {
		set0[v] = true
	}
	var overlap []string
	for _, v := range r1 {
		if set0[v] {
			overlap = append(overlap, v)
		}
	}
	if len(overlap) > 0 {
		show := overlap
		if len(show) > 5 {
			show = show[:5]
		}
		t.Errorf("ranks share %d of %d samples (e.g. %v): that is redundant computation, not data-parallel training",
			len(overlap), len(r1), show)
	}
	if len(r0)+len(r1) < 40 {
		t.Errorf("ranks saw %d+%d of 40 samples between them; data was dropped", len(r0), len(r1))
	}
}
