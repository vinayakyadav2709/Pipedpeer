package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

func TestStartStopStatusLifecycle(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	out := runCLIWithEnv(t, stateDir, xdg, "start", "--port", fmt.Sprintf("%d", port))
	if !strings.Contains(out, "daemon running") && !strings.Contains(out, "daemon already running") {
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

	// A stopped daemon is a failed status: exit 1 and a line saying so.
	sout, serr, err := runCLIEWithEnv(t, stateDir, xdg, "status")
	if err == nil || !strings.Contains(sout+serr, "daemon stopped") {
		t.Fatalf("expected a failing 'daemon stopped' status, got err=%v: %s%s", err, sout, serr)
	}
}

func TestExitCode(t *testing.T) {
	for _, c := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 3}), 3},
		{errCancelReported, 130},
		{fmt.Errorf("wrapped: %w", context.Canceled), 130},
		{errors.New("anything else"), 1},
	} {
		if got := exitCode(c.err); got != c.want {
			t.Errorf("exitCode(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}

func TestRunCheckOnlyAutoStartsDaemon(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)
	scriptPath := writeScript(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")

	// First start the daemon so it has a node-id to accept against
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--port", fmt.Sprintf("%d", port))

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")
	defer func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") }()
	// Stop removed the port file and run has no --port: without this the
	// auto-start would take the real daemon's 4242.
	if err := os.WriteFile(filepath.Join(stateDir, "daemon.port"), []byte(fmt.Sprint(port)), 0o644); err != nil {
		t.Fatal(err)
	}

	out := runCLIWithEnv(t,
		stateDir, xdg,
		"run",
		"--host", fmt.Sprintf("127.0.0.1:%d", port),
		"--check-only",
		"-v",       // "started daemon" is a verbose-only line
		scriptPath, // flags after the script are the script's own
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
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--port", fmt.Sprintf("%d", port))
	defer func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") }()

	// --remote excludes this machine, and with nothing else in the cluster
	// the run must fail loudly instead of queuing forever. One look only:
	// given its usual wait, discovery finds whatever else runs on this
	// network, and the cluster is no longer empty.
	t.Setenv("PIPEDPEER_REMOTE_WAIT", "0s")
	_, errOut, err := runCLIEWithEnv(t,
		stateDir, xdg,
		"run",
		"--remote",
		"--check-only",
		scriptPath,
	)
	if err == nil {
		t.Fatalf("expected error when no eligible remote node, got none")
	}
	// check-only reports it as "NOT READY: no eligible node other than this
	// machine"; a real run as "no eligible node found".
	if !strings.Contains(errOut, "no eligible node") {
		t.Fatalf("expected 'no eligible node' error, got: %s", errOut)
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
	cmd := exec.Command(cliBinary(t), args...)
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_DAEMON_STATE_DIR="+stateDir,
		"XDG_DATA_HOME="+xdg,
	)
	cmd.Env = append(cmd.Env, isolatedDaemonEnv...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// isolatedDaemonEnv keeps a test's daemon out of any real cluster. Without it
// a daemon started here answered the LAN probes of a lab on the same host and
// was listed (and placeable) there as a "yeet" node, and an inherited
// rendezvous, registry or NATS address would join it to the user's cluster.
// Empty values override the inherited ones: exec keeps the last of each key.
var isolatedDaemonEnv = []string{
	"PIPEDPEER_NO_LAN_DISCOVERY=1",
	"PIPEDPEER_RENDEZVOUS=",
	"PIPEDPEER_REGISTRY=",
	"PIPEDPEER_NATS_URL=",
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
	binPath := cliBinary(t)

	// Verify binary exists and is executable
	info, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("binary not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("binary is empty")
	}

	// Bare `pipedpeer` prints help (it used to try to run a script), and
	// help lists only the commands a user needs.
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "PIPEDPEER_DAEMON_STATE_DIR="+t.TempDir(), "XDG_DATA_HOME="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bare pipedpeer failed: %v\n%s", err, out)
	}
	help := string(out)
	for _, want := range []string{"Usage:", "status", "dashboard", "ui", "jobs", "attach", "run", "setup", "start", "stop"} {
		if !strings.Contains(help, want) {
			t.Errorf("help lacks %q:\n%s", want, help)
		}
	}
	for _, gone := range []string{"registry", "net-check", "rendezvous", "completion", "traffic", " map ", "events", " trust ", " auth "} {
		if strings.Contains(help, gone) {
			t.Errorf("help still lists %q:\n%s", gone, help)
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
	// jobs shows the short id a person types: the last 8 digits.
	if !strings.Contains(out, "34567890") {
		t.Fatalf("expected short job ID in jobs output, got: %s", out)
	}
	if !strings.Contains(out, "succeeded") {
		t.Fatalf("expected 'succeeded' in jobs output, got: %s", out)
	}

	if strings.Contains(out, "history path") || strings.Contains(out, "DURATION_MS") {
		t.Fatalf("jobs still shows raw columns or the history path: %s", out)
	}
	if !strings.Contains(out, "1m0s") || !strings.Contains(out, "train.py") {
		t.Fatalf("expected the script and a human duration, got: %s", out)
	}

	// `jobs <id>` absorbed `job --id`.
	out = runCLIWithXDG(t, stateDir, xdg, "jobs", jobID, "--output")
	if !strings.Contains(out, "succeeded") {
		t.Fatalf("expected status in job detail, got: %s", out)
	}
	if !strings.Contains(out, "gpu-box (gpu-1)") {
		t.Fatalf("expected where it ran in job detail, got: %s", out)
	}
	if !strings.Contains(out, "training done") {
		t.Fatalf("expected stdout content in --output, got: %s", out)
	}
}

func runCLIWithXDG(t *testing.T, stateDir, xdg string, args ...string) string {
	t.Helper()
	cmd := exec.Command(cliBinary(t), args...)
	cmd.Env = append(os.Environ(),
		"PIPEDPEER_DAEMON_STATE_DIR="+stateDir,
		"XDG_DATA_HOME="+xdg,
	)
	cmd.Env = append(cmd.Env, isolatedDaemonEnv...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("cli %v failed: %v\nstdout:\n%s\nstderr:\n%s", args, err, stdout.String(), stderr.String())
	}
	return stdout.String() + stderr.String()
}

// TestStatusShowsSelf checks that `status` lists this machine in its table.
// Accepting any output here once let the test pass with no daemon at all.
func TestStatusShowsSelf(t *testing.T) {
	stateDir := t.TempDir()
	xdg := t.TempDir()
	port := freePort(t)

	_ = runCLIWithEnv(t, stateDir, xdg, "stop")
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--port", fmt.Sprintf("%d", port))
	t.Cleanup(func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") })

	// The daemon registers itself on its first health poll, which is not
	// instant.
	var out string
	for i := 0; i < 30; i++ {
		out = runCLIWithEnv(t, stateDir, xdg, "status")
		if strings.Contains(out, "self") {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	if !strings.Contains(out, "MEM FREE") {
		t.Fatalf("no node table at all:\n%s", out)
	}
	if !strings.Contains(out, "self") {
		t.Fatalf("the running daemon does not list this machine:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("port=%d", port)) {
		t.Errorf("status does not report the daemon's own port %d:\n%s", port, out)
	}
	if host, err := os.Hostname(); err == nil && !strings.Contains(out, host) {
		t.Errorf("status lists a self row, but not this machine's name %q:\n%s", host, out)
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
	_ = runCLIWithEnv(t, stateDir, xdg, "start", "--port", fmt.Sprintf("%d", port))
	defer func() { _ = runCLIWithEnv(t, stateDir, xdg, "stop") }()

	// Run with --check-only, NO --host
	out := runCLIWithEnv(t,
		stateDir, xdg,
		"run",
		"--check-only",
		"-v", // [coordinator] lines are verbose-only
		scriptPath,
	)

	if !strings.Contains(out, "[coordinator]") {
		t.Fatalf("expected coordinator output, got: %s", out)
	}
	if !strings.Contains(out, "checks complete") {
		t.Fatalf("expected 'checks complete' message, got: %s", out)
	}
}
