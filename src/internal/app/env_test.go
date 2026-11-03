package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildUsesAPathFlakeRef(t *testing.T) {
	if got, want := flakeRef("x86_64-linux"), "path:.#packages.x86_64-linux.default"; got != want {
		t.Fatalf("flakeRef = %q, want %q", got, want)
	}
}

// With no cache port the build argv is the plain build, unchanged; the peer
// cache options are appended only when a port is given.
func TestNixBuildArgvUnchangedWithoutCachePort(t *testing.T) {
	want := []string{"nix", "build", "path:.#packages.x86_64-linux.default", "--option", "build-users-group", ""}
	got := nixBuildArgv(flakeRef("x86_64-linux"), 0)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("nixBuildArgv(port 0) = %q, want %q", got, want)
	}
}

// An environment that covers a script's packages is reused instead of built;
// one that adds a heavy package, differs in template or is gone is not.
func TestFindEnvReusesCoveringEnvironment(t *testing.T) {
	dir := t.TempDir()
	index := filepath.Join(dir, "envs.json")
	mk := func(name string) string {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Join(p, "bin"), 0o755)
		os.WriteFile(filepath.Join(p, "bin", "run"), nil, 0o755)
		return p
	}
	small, withTorch := mk("small"), mk("torch")
	recordEnv(index, "T", []string{"pandas", "numpy"}, small)
	recordEnv(index, "T", []string{"numpy", "torch"}, withTorch)
	recordEnv(index, "T", []string{"numpy"}, filepath.Join(dir, "gone"))

	if got, ok := findEnv(index, "T", []string{"numpy"}); !ok || got != small {
		t.Errorf("findEnv(numpy) = %q, %v; want the pandas+numpy env, not torch's or the gone one", got, ok)
	}
	if got, ok := findEnv(index, "T", []string{"numpy", "torch"}); !ok || got != withTorch {
		t.Errorf("findEnv(numpy, torch) = %q, %v", got, ok)
	}
	if _, ok := findEnv(index, "other", []string{"numpy"}); ok {
		t.Error("an environment from another template was reused")
	}
	if _, ok := findEnv(index, "T", []string{"scipy"}); ok {
		t.Error("an environment lacking a package was reused")
	}
}

// A CPU-only node must never reuse a recorded CUDA environment: the two
// flakes differ only in torch's build.
func TestEnvTemplateSeparatesGPUFromCPU(t *testing.T) {
	if envTemplate("python3", true) == envTemplate("python3", false) {
		t.Error("GPU and CPU environments share a template, so one is reused for the other")
	}
}
