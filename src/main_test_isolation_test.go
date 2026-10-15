package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// binDir holds the CLI binary the tests build once and share.
var binDir string

// TestMain isolates the token store. The CLI tests start real daemons, and
// authtoken reads the operator's token from XDG_DATA_HOME; on a machine where
// one is configured those daemons start refusing the tests' own requests.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "pipedpeer-test-xdg-*")
	if err != nil {
		panic(err)
	}
	if binDir, err = os.MkdirTemp("", "pipedpeer-test-bin-*"); err != nil {
		panic(err)
	}
	os.Setenv("XDG_DATA_HOME", dir)
	os.Unsetenv("PIPEDPEER_TOKEN")
	code := m.Run()
	os.RemoveAll(dir)
	os.RemoveAll(binDir)
	os.Exit(code)
}

// buildCLI builds the binary once per test run. Every CLI test used to `go
// run .`, which links the binary again for each of ~30 invocations.
var buildCLI = sync.OnceValues(func() (string, error) {
	bin := filepath.Join(binDir, "pipedpeer")
	out, err := exec.Command("go", "build", "-buildvcs=false", "-o", bin, ".").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("binary build failed: %v\n%s", err, out)
	}
	return bin, nil
})

// cliBinary is the shared CLI binary; a failed build fails the calling test.
func cliBinary(t *testing.T) string {
	t.Helper()
	bin, err := buildCLI()
	if err != nil {
		t.Fatal(err)
	}
	return bin
}
