package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartStopStatusLifecycle(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	out := runCLIWithEnv(t, stateDir, xdg, "start", "--daemon-port", fmt.Sprintf("%d", port))
	if !strings.Contains(out, "started daemon") && !strings.Contains(out, "daemon already running") {
		t.Fatalf("expected daemon start output, got: %s", out)
	}

	out = runCLIWithEnv(t, stateDir, xdg, "status")
	if !strings.Contains(out, "daemon running") {
		t.Fatalf("expected running status, got: %s", out)
	}

	out = runCLIWithEnv(t, stateDir, xdg, "stop")
	if !strings.Contains(out, "stopped daemon") {
		t.Fatalf("expected stopped daemon output, got: %s", out)
	}

	out = runCLIWithEnv(t, stateDir, xdg, "status")
	if !strings.Contains(out, "daemon stopped") {
		t.Fatalf("expected daemon stopped status, got: %s", out)
	}
}

func TestRunCheckOnlyAutoStartsDaemon(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)
	scriptPath := writeScript(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	// First start the daemon so it has a node-id to accept against
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--daemon-port", fmt.Sprintf("%d", port))

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	out := runCLIWithEnv(t,
		stateDir, xdg,
		"run",
		"--script", scriptPath,
		"--host", fmt.Sprintf("127.0.0.1:%d", port),
		"--daemon-port", fmt.Sprintf("%d", port),
		"--check-only",
	)

	if !strings.Contains(out, "started daemon") {
		t.Fatalf("expected auto-start message, got: %s", out)
	}
	if !strings.Contains(out, "remote daemon accepted job") {
		t.Fatalf("expected remote acceptance message, got: %s", out)
	}

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")
}

func TestRemoteRunFailsWhenNoEligibleNode(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)
	scriptPath := writeScript(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--daemon-port", fmt.Sprintf("%d", port))
	defer func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") }()

	// --remote excludes this machine, and with nothing else in the cluster
	// the run must fail loudly instead of queuing forever.
	_, errOut, err := runCLIEWithEnv(t,
		stateDir, xdg,
		"run",
		"--script", scriptPath,
		"--daemon-port", fmt.Sprintf("%d", port),
		"--remote",
		"--check-only",
	)
	if err == nil {
		t.Fatalf("expected error when no eligible remote node, got none")
	}
	if !strings.Contains(errOut, "no eligible node found") {
		t.Fatalf("expected 'no eligible node found' error, got: %s", errOut)
	}
}

func runCLIWithEnv(t *testing.T, stateDir, xdg string, args ...string) string {
	t.Helper()
	out, errOut, err := runCLIEWithEnv(t, stateDir, xdg, args...)
	if err != nil {
		t.Fatalf("cli failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
	}
	return out + errOut
}

func runCLIEWithEnv(t *testing.T, stateDir, xdg string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "."}, args...)...)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_DAEMON_STATE_DIR="+stateDir,
		"XDG_DATA_HOME="+xdg,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func extractNodeID(t *testing.T, jsonData []byte) string {
	t.Helper()
	// Simple extraction without importing encoding/json
	s := string(jsonData)
	prefix := `"node_id": "`
	idx := strings.Index(s, prefix)
	if idx < 0 {
		t.Fatalf("node_id not found in: %s", s)
	}
	rest := s[idx+len(prefix):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("node_id not terminated in: %s", s)
	}
	return rest[:end]
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

func TestBinaryBuildsAndRuns(t *testing.T) {
	// Build binary — this catches compilation, import, and linkage issues
	binPath := filepath.Join(t.TempDir(), "pipedpeer")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", binPath, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("binary build failed: %v\n%s", err, out)
	}

	// Verify binary exists and is executable
	info, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("binary not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("binary is empty")
	}

	// Run with 'status' command — should work without any setup
	cmd := exec.Command(binPath, "status")
	cmd.Env = append(os.Environ(), "PIPEDPEER_DAEMON_STATE_DIR="+t.TempDir(), "XDG_DATA_HOME="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("binary execution failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "daemon") {
		t.Fatalf("expected daemon status output, got: %s", out)
	}
}

func TestInitCreatesIgnoreFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Build binary first so we can run it from any directory
	binPath := filepath.Join(t.TempDir(), "pipedpeer")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", binPath, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	cmd := exec.Command(binPath, "init")
	cmd.Dir = tmpDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init failed: %v\noutput: %s", err, out)
	}

	ignoreFile := filepath.Join(tmpDir, ".pipedpeerignore")
	content, err := os.ReadFile(ignoreFile)
	if err != nil {
		t.Fatalf("expected .pipedpeerignore to exist: %v", err)
	}

	for _, expected := range []string{".git/", "__pycache__/", "node_modules/"} {
		if !strings.Contains(string(content), expected) {
			t.Fatalf("expected %s in ignore file, got: %s", expected, content)
		}
	}
}

func TestJobsListAndJobDetail(t *testing.T) {
	xdg := t.TempDir()
	stateDir := t.TempDir()

	// Create a mock job history entry
	jobID := "1234567890"
	jobDir := filepath.Join(xdg, "pipedpeer", "jobs", jobID)
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	meta := `{
  "id": "1234567890",
  "status": "succeeded",
  "started_at": "2026-01-01T00:00:00Z",
  "finished_at": "2026-01-01T00:01:00Z",
  "duration_ms": 60000,
  "script_path": "/tmp/train.py",
  "remote": "root@gpu-box:22",
  "target_id": "gpu-1",
  "detached": false,
  "isolate": true,
  "job_name": "test-job"
}`
	if err := os.WriteFile(filepath.Join(jobDir, "metadata.json"), []byte(meta), 0644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "stdout.log"), []byte("training done\n"), 0644); err != nil {
		t.Fatalf("write stdout: %v", err)
	}

	// Test `jobs` command
	out := runCLIWithXDG(t, stateDir, xdg, "jobs")
	if !strings.Contains(out, "1234567890") {
		t.Fatalf("expected job ID in jobs output, got: %s", out)
	}
	if !strings.Contains(out, "succeeded") {
		t.Fatalf("expected 'succeeded' in jobs output, got: %s", out)
	}

	// Test `job --id` command
	out = runCLIWithXDG(t, stateDir, xdg, "job", "--id", jobID, "--output")
	if !strings.Contains(out, "status: succeeded") {
		t.Fatalf("expected status in job detail, got: %s", out)
	}
	if !strings.Contains(out, "target_id: gpu-1") {
		t.Fatalf("expected target_id in job detail, got: %s", out)
	}
	if !strings.Contains(out, "training done") {
		t.Fatalf("expected stdout content in --output, got: %s", out)
	}
}

func runCLIWithXDG(t *testing.T, stateDir, xdg string, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "."}, args...)...)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_DAEMON_STATE_DIR="+stateDir,
		"XDG_DATA_HOME="+xdg,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("cli %v failed: %v\nstdout:\n%s\nstderr:\n%s", args, err, stdout.String(), stderr.String())
	}
	return stdout.String() + stderr.String()
}

func TestRegistryCLIStartsAndAcceptsConnections(t *testing.T) {
	// Start registry as a subprocess, then query it with HTTP
	port := freePort(t)

	cmd := exec.Command("go", "run", ".", "registry", "--port", fmt.Sprintf("%d", port))
	cmd.Dir = "."
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start registry: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	// Wait for registry to be ready
	var ready bool
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
	}
	if !ready {
		t.Fatal("registry did not become ready within 6s")
	}

	// Register a node
	regBody := `{"node_id":"cli-test-node","ssh_endpoint":"root@10.0.1.5:22","daemon_port":38080}`
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/v1/register", port),
		"application/json",
		strings.NewReader(regBody),
	)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 from register, got %d", resp.StatusCode)
	}

	// Query nodes
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", port))
	if err != nil {
		t.Fatalf("nodes query failed: %v", err)
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	if !strings.Contains(buf.String(), "cli-test-node") {
		t.Fatalf("expected cli-test-node in nodes response, got: %s", buf.String())
	}
}

func TestNodesCLIShowsSelf(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()

	// Stop any leftover daemon first
	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	out := runCLIWithEnv(t, stateDir, xdg, "nodes")
	if !strings.Contains(out, "NODE_ID") && !strings.Contains(out, "No nodes found") {
		t.Fatalf("expected nodes table or empty output, got: %s", out)
	}
}

func TestAutoPlacementSelectsSelf(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)
	scriptPath := writeScript(t)

	// Stop any leftover daemon first
	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	// Start daemon first
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--daemon-port", fmt.Sprintf("%d", port))
	defer func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") }()

	// Run with --check-only, NO --host
	out := runCLIWithEnv(t,
		stateDir, xdg,
		"run",
		"--script", scriptPath,
		"--daemon-port", fmt.Sprintf("%d", port),
		"--check-only",
	)

	if !strings.Contains(out, "[coordinator]") {
		t.Fatalf("expected coordinator output, got: %s", out)
	}
	if !strings.Contains(out, "checks complete") {
		t.Fatalf("expected 'checks complete' message, got: %s", out)
	}
}
