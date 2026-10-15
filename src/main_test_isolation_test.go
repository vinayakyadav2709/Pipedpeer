package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/service"
)

// binDir holds the CLI binary the tests build once and share.
var binDir string

// guardLog collects every systemctl/loginctl call a test (or a CLI it runs)
// made; the fake tools on PATH write it.
var guardLog string

// TestMain isolates the token store and the user's service. The CLI tests
// start real daemons, and authtoken reads the operator's token from
// XDG_DATA_HOME; on a machine where one is configured those daemons start
// refusing the tests' own requests. daemonctl.Start/Stop hand the daemon to
// `systemctl --user` once ~/.config/systemd/user/pipedpeer.service exists, so
// with the real HOME and session bus the tests stopped and restarted the
// live daemon (VERIFY-5, 2026-10-16). HOME and XDG_CONFIG_HOME move to a temp
// dir, the bus variables go (cgroups.ScopePrefix then skips systemd-run, its
// no-bus path), and fake systemctl/loginctl fail the run if anything calls
// them.
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
	isolateUserService(dir)
	code := m.Run()
	if b, _ := os.ReadFile(guardLog); len(b) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: tests reached the user's service manager:\n%s", b)
		code = 1
	}
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

// isolateUserService points HOME, XDG_CONFIG_HOME and the session bus away
// from the real user, after pinning the Go caches the moved HOME would hide,
// and puts failing systemctl/loginctl first on PATH.
func isolateUserService(dir string) {
	out, err := exec.Command("go", "env", "-json", "GOCACHE", "GOMODCACHE", "GOPATH", "GOENV").Output()
	if err != nil {
		panic(fmt.Sprintf("go env: %v", err))
	}
	var goenv map[string]string
	if err := json.Unmarshal(out, &goenv); err != nil {
		panic(fmt.Sprintf("go env output: %v", err))
	}
	for k, v := range goenv {
		os.Setenv(k, v)
	}
	home := filepath.Join(dir, "home")
	guard := filepath.Join(dir, "guard-bin")
	for _, d := range []string{home, guard} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			panic(err)
		}
	}
	os.Setenv("HOME", home)
	os.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	os.Unsetenv("XDG_RUNTIME_DIR")
	os.Unsetenv("DBUS_SESSION_BUS_ADDRESS")
	guardLog = filepath.Join(dir, "service-calls.log")
	fake := "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> '" + guardLog + "'\nexit 1\n"
	for _, tool := range []string{"systemctl", "loginctl"} {
		if err := os.WriteFile(filepath.Join(guard, tool), []byte(fake), 0o755); err != nil {
			panic(err)
		}
	}
	os.Setenv("PATH", guard+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestCLIEnvKeepsOffUserService checks the isolation itself, so it can run
// alone before anything that starts a daemon.
func TestCLIEnvKeepsOffUserService(t *testing.T) {
	if p := service.UnitPath(); !strings.HasPrefix(p, os.Getenv("HOME")) || !strings.Contains(p, "pipedpeer-test-xdg-") {
		t.Fatalf("unit path %s is outside the test HOME", p)
	}
	if service.Installed() {
		t.Fatal("a pipedpeer unit is visible to the tests")
	}
	for _, v := range []string{"XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"} {
		if os.Getenv(v) != "" {
			t.Fatalf("%s is still set", v)
		}
	}
	for _, tool := range []string{"systemctl", "loginctl"} {
		p, err := exec.LookPath(tool)
		if err != nil || filepath.Dir(p) != filepath.Join(filepath.Dir(guardLog), "guard-bin") {
			t.Fatalf("%s resolves to %q (%v), not the guard", tool, p, err)
		}
	}
	if err := exec.Command("systemctl", "--user", "status").Run(); err == nil {
		t.Fatal("the guard systemctl succeeded")
	}
	b, _ := os.ReadFile(guardLog)
	if !strings.Contains(string(b), "systemctl --user status") {
		t.Fatalf("guard did not record the call: %q", b)
	}
	// This test's own call is expected; leave the log for real offenders.
	if err := os.Remove(guardLog); err != nil {
		t.Fatal(err)
	}
}
