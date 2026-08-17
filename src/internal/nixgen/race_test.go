package nixgen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runRaceProbe drives the shim's _ClusterPool._race directly, stubbing
// _remote_chunk, and asserts results equal a plain local map. mode controls the
// remote stub: "ok" (all chunks succeed) or "dead" (every chunk fails).
func runRaceProbe(t *testing.T, mode string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "shim_mod.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	src := `
import sys
sys.path.insert(0, sys.argv[1])
import shim_mod

def double(x):
    return x * 2

mode = sys.argv[2]

def main():
    items = list(range(40))
    class StubPool(shim_mod._ClusterPool):
        def _remote_chunk(self, func, chunk, starmap):
            if mode == "dead":
                return None
            idxs = [p[0] for p in chunk]
            vals = [p[1] for p in chunk]
            return [(i, func(*v) if isinstance(v, tuple) else func(v)) for i, v in zip(idxs, vals)]

    p = StubPool(processes=2)
    p._remote = True
    p._STORE = "/nix/store/x"
    r = p._race(double, items, False, 0.5)
    assert r == list(map(double, items)), (r, list(map(double, items)))
    p.close()
    print("race-ok")

if __name__ == "__main__":
    main()
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script, dir, mode)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("race probe (mode=%s) failed:\n%s\n%v", mode, out, err)
	}
	if !strings.Contains(string(out), "race-ok") {
		t.Fatalf("race probe (mode=%s) unexpected output: %q", mode, out)
	}
}

func TestShimRaceCorrectWithRemote(t *testing.T) {
	runRaceProbe(t, "ok")
}

func TestShimRaceCorrectWithDeadRemote(t *testing.T) {
	// A dead remote must not change the result: local re-runs every straggler.
	runRaceProbe(t, "dead")
}

// TestShimRaceAdaptiveChunk sanity-checks the adaptive chunk sizing bounds.
func TestShimRaceAdaptiveChunk(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "shim_mod.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	src := `
import sys
sys.path.insert(0, sys.argv[1])
import shim_mod
# cheap item -> big chunk, costly item -> small chunk, bounded
assert shim_mod._adaptive_chunk(0.0001) == 256
assert shim_mod._adaptive_chunk(1.0) == 1
assert shim_mod._adaptive_chunk(0) == 64
assert shim_mod._chunk(list(range(10)), 0) == [list(range(10))]
print("chunk-ok")
`
	script := filepath.Join(dir, "probe.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(python, script, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("chunk probe failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "chunk-ok") {
		t.Fatalf("chunk probe unexpected output: %q", out)
	}
}
