package nixgen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestShimSyntax compiles the embedded shim, guaranteeing it is valid Python.
// A broken sitecustomize would abort every intercepted run at startup.
func TestShimSyntax(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	shim := filepath.Join(dir, "sitecustomize.py")
	if err := os.WriteFile(shim, []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(python, "-m", "py_compile", shim).CombinedOutput()
	if err != nil {
		t.Fatalf("shim does not compile:\n%s", out)
	}
}

// TestShimInstallSafe confirms the shim imports cleanly and is a no-op when
// interception is disabled (PIPEDPEER_SHIM unset), so it cannot break a normal
// python run.
func TestShimInstallSafe(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}

	// Define a module-level (picklable) function, then exercise a real Pool.
	// Exercise a real Pool from a file so the worker can pickle the function.
	src := `
def double(x):
    return x * 2

import multiprocessing
if __name__ == "__main__":
    p = multiprocessing.Pool(1)
    print(p.map(double, [1, 2, 3]))
    p.close()
    p.join()
`
	script := filepath.Join(dir, "work.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shim broke a normal run:\n%s\n%v", out, err)
	}
	if string(out) != "[2, 4, 6]\n" {
		t.Fatalf("unexpected output: %q", out)
	}
}

// TestShimEnabledStillLocal confirms that with interception on but no reachable
// daemon URL, the patched Pool still executes correctly on local cores (the
// never-slower fallback): it must not crash and must return correct results.
func TestShimEnabledStillLocal(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(ShimSitecustomize), 0644); err != nil {
		t.Fatal(err)
	}
	src := `
def double(x):
    return x * 2

import multiprocessing
if __name__ == "__main__":
    p = multiprocessing.Pool(1)
    print(p.map(double, [1, 2, 3]))
    p.close()
`
	script := filepath.Join(dir, "work.py")
	if err := os.WriteFile(script, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	// Enabled but no daemon URL / shard count: _remote is false, local fallback.
	cmd := exec.Command(python, script)
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"PIPEDPEER_SHIM=1",
		"PIPEDPEER_DAEMON_URL=", // empty
		"PIPEDPEER_NUM_SHARDS=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shim local fallback failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "[2, 4, 6]") {
		t.Fatalf("unexpected output: %q", out)
	}
	if !strings.Contains(string(out), "local 3 items") {
		t.Fatalf("expected local fallback to be used, got: %q", out)
	}
}
