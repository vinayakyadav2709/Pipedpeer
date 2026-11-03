package app

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// A 3 GiB (sparse) workspace: refused for a run, untouched for a build-only
// caller (check-only, warm-up).
func TestBuildOnlySkipsWorkspace(t *testing.T) {
	proj := t.TempDir()
	f, err := os.Create(filepath.Join(proj, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 << 30); err != nil {
		t.Fatal(err)
	}
	f.Close()
	script := filepath.Join(proj, "j.py")
	os.WriteFile(script, []byte("x"), 0o644)

	var big *daemonctl.WorkspaceTooLargeError
	if err := checkWorkspaceSize(EnvOptions{}, proj); !errors.As(err, &big) {
		t.Errorf("normal run: err = %v, want refusal by the cap", err)
	}
	if err := checkWorkspaceSize(EnvOptions{BuildOnly: true}, proj); err != nil {
		t.Errorf("build-only: size error %v", err)
	}

	tmp := t.TempDir()
	env := &Environment{tmpDir: tmp}
	if err := packWorkspace(t.Context(), env, EnvOptions{BuildOnly: true}, proj, script); err != nil {
		t.Fatal(err)
	}
	if env.WorkspaceTar != "" {
		t.Errorf("build-only set WorkspaceTar %q", env.WorkspaceTar)
	}
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Errorf("build-only wrote %d file(s)", len(ents))
	}
}

func TestPackWorkspaceNormalWritesTar(t *testing.T) {
	proj := t.TempDir()
	script := filepath.Join(proj, "j.py")
	os.WriteFile(script, []byte("x"), 0o644)
	env := &Environment{tmpDir: t.TempDir()}
	if err := packWorkspace(t.Context(), env, EnvOptions{}, proj, script); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(env.WorkspaceTar); err != nil || env.ScriptRel != "j.py" {
		t.Errorf("tar %v rel %q", err, env.ScriptRel)
	}
}
