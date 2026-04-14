package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartStopStatusLifecycle(t *testing.T) {
	stateDir := t.TempDir()
	port := freePort(t)
	nodeID := "node-lifecycle-test"

	_ = runCLI(t, stateDir, "stop")

	out := runCLI(t, stateDir, "start", "--node-id", nodeID, "--daemon-port", fmt.Sprintf("%d", port))
	if !strings.Contains(out, "started daemon") && !strings.Contains(out, "daemon already running") {
		t.Fatalf("expected daemon start output, got: %s", out)
	}

	out = runCLI(t, stateDir, "status")
	if !strings.Contains(out, "daemon running") || !strings.Contains(out, nodeID) {
		t.Fatalf("expected running status with node id, got: %s", out)
	}

	out = runCLI(t, stateDir, "stop")
	if !strings.Contains(out, "stopped daemon") {
		t.Fatalf("expected stopped daemon output, got: %s", out)
	}

	out = runCLI(t, stateDir, "status")
	if !strings.Contains(out, "daemon stopped") {
		t.Fatalf("expected daemon stopped status, got: %s", out)
	}
}

func TestRunCheckOnlyAutoStartsDaemon(t *testing.T) {
	stateDir := t.TempDir()
	port := freePort(t)
	nodeID := "node-autostart-test"
	scriptPath := writeScript(t)

	_ = runCLI(t, stateDir, "stop")

	out := runCLI(t,
		stateDir,
		"run",
		"--script", scriptPath,
		"--remote", "root@127.0.0.1:22",
		"--target-id", nodeID,
		"--daemon-port", fmt.Sprintf("%d", port),
		"--local-node-id", nodeID,
		"--check-only",
	)

	if !strings.Contains(out, "started daemon") {
		t.Fatalf("expected auto-start message, got: %s", out)
	}
	if !strings.Contains(out, "remote daemon accepted job") {
		t.Fatalf("expected remote acceptance message, got: %s", out)
	}

	_ = runCLI(t, stateDir, "stop")
}

func TestRunCheckOnlyRejectsWrongTargetID(t *testing.T) {
	stateDir := t.TempDir()
	port := freePort(t)
	nodeID := "node-accept-test"
	scriptPath := writeScript(t)

	_ = runCLI(t, stateDir, "stop")
	_ = runCLI(t, stateDir, "start", "--node-id", nodeID, "--daemon-port", fmt.Sprintf("%d", port))
	defer func() { _ = runCLI(t, stateDir, "stop") }()

	_, errOut, err := runCLIE(t,
		stateDir,
		"run",
		"--script", scriptPath,
		"--remote", "root@127.0.0.1:22",
		"--target-id", "different-node",
		"--daemon-port", fmt.Sprintf("%d", port),
		"--local-node-id", nodeID,
		"--check-only",
	)
	if err == nil {
		t.Fatalf("expected rejection error, got none")
	}
	if !strings.Contains(errOut, "job rejected by remote daemon") {
		t.Fatalf("expected rejection error output, got: %s", errOut)
	}
}

func runCLI(t *testing.T, stateDir string, args ...string) string {
	t.Helper()
	out, errOut, err := runCLIE(t, stateDir, args...)
	if err != nil {
		t.Fatalf("cli failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
	}
	return out + errOut
}

func runCLIE(t *testing.T, stateDir string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "."}, args...)...)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "PIPEDPEER_DAEMON_STATE_DIR="+stateDir)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func writeScript(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	p := filepath.Join(d, "script.py")
	if err := os.WriteFile(p, []byte("print('ok')\n"), 0644); err != nil {
		t.Fatalf("write script failed: %v", err)
	}
	return p
}
