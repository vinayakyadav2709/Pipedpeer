package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoTempDirInSource keeps pipedpeer's files off tmpfs.
//
// $TMPDIR is usually RAM-backed and sized to a fraction of memory, so
// anything large the daemon writes there competes with the jobs it is
// running: a workspace with a big dataset once filled it and the daemon
// started rejecting uploads it had plenty of disk for. It is also wiped on
// reboot, which loses the node identity and the closure cache, so a restarted
// machine re-downloads gigabytes it already had.
//
// internal/userdir is the one place allowed to name a temp dir, as the last
// resort when there is no home directory at all. Everything else asks it
// where files belong. This is a lint rather than a runtime check because the
// failure it prevents is silent: things work fine until the machine is busy.
func TestNoTempDirInSource(t *testing.T) {
	banned := []string{"os.TempDir()", `os.MkdirTemp("",`, `ioutil.TempDir("",`}
	allowed := map[string]bool{
		filepath.Join("internal", "userdir", "userdir.go"): true,
	}

	var offenders []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "vendor" || strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || allowed[path] {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(body), "\n") {
			code := line
			if i := strings.Index(code, "//"); i >= 0 {
				code = code[:i] // a comment may discuss it; only code counts
			}
			for _, b := range banned {
				if strings.Contains(code, b) {
					offenders = append(offenders, path+": "+strings.TrimSpace(line))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Errorf("these write to a temp dir instead of asking internal/userdir:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}
