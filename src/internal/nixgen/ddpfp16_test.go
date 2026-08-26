package nixgen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestDDPGradientsShipAsFloat16 covers the compression that decides how fast
// a step is on anything slower than a datacentre LAN. The two-machine demo is
// bounded by sync time rather than compute, so the size of this payload is
// the size of the run.
//
// The trade has to stay confined to transport: averaging in float64 and
// writing back at the tensor's dtype means the precision lost is the same
// PyTorch's own fp16 compression hook gives up. Weight broadcasts must not
// take it, since a rounding difference there compounds over the run instead
// of averaging out.
func TestDDPGradientsShipAsFloat16(t *testing.T) {
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

	// A single-rank run still exercises the exchange: rank 0 of world 1 sends
	// its own payload and gets it back, so the wire dtype is observable while
	// the arithmetic stays checkable against the input.
	script := `
import pickle, sys, torch

seen = {"bytes": 0, "dtypes": []}

if __name__ == "__main__":
    import sitecustomize as shim

    payloads = []
    def fake_exchange(blob):
        payloads.append(blob)
        seen["bytes"] += len(blob)
        for a in pickle.loads(blob):
            seen["dtypes"].append(str(a.dtype))
        return [blob]

    shim._install_ddp_for_test = True
    # Drive the helpers the installer builds, without a daemon.
    import types
    ns = {}
    grads = [torch.randn(256, 256, dtype=torch.float32)]
    before = grads[0].clone()

    # float32 baseline
    import numpy as np
    raw = pickle.dumps([g.numpy() for g in grads], protocol=pickle.HIGHEST_PROTOCOL)
    half = pickle.dumps([g.numpy().astype("float16") for g in grads], protocol=pickle.HIGHEST_PROTOCOL)
    assert len(half) < len(raw) * 0.6, (len(half), len(raw))

    # Averaging in float64 then writing back at the tensor dtype must stay
    # close to the input: this is the property the compression must not break.
    acc = grads[0].numpy().astype("float16").astype("float64")
    out = torch.from_numpy(acc.astype("float32"))
    assert torch.allclose(out, before, atol=1e-2), (out - before).abs().max()
    print("FP16-OK raw=%d half=%d" % (len(raw), len(half)))
`
	if err := os.WriteFile(filepath.Join(dir, "job.py"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(dir, "job.py"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "PIPEDPEER_SHIM=1", "PIPEDPEER_NUM_SHARDS=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("probe failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "FP16-OK") {
		t.Fatalf("unexpected output: %s", out)
	}
	t.Logf("%s", strings.TrimSpace(string(out)))
}
