package daemonctl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type State struct {
	Running bool
	PID     int
	NodeID  string
	Port    int
}

type Paths struct {
	PIDFile  string
	NodeFile string
	PortFile string
	LogFile  string
}

func DefaultPaths() Paths {
	base := os.Getenv("PIPEDPEER_DAEMON_STATE_DIR")
	if base == "" {
		base = filepath.Join(os.TempDir(), "pipedpeer")
	}
	return Paths{
		PIDFile:  filepath.Join(base, "daemon.pid"),
		NodeFile: filepath.Join(base, "daemon.node"),
		PortFile: filepath.Join(base, "daemon.port"),
		LogFile:  filepath.Join(base, "daemon.log"),
	}
}

func Status() State {
	paths := DefaultPaths()
	pid, _ := readInt(paths.PIDFile)
	nodeID, _ := readString(paths.NodeFile)
	port, _ := readInt(paths.PortFile)
	running := processRunning(pid)
	if !running {
		pid = 0
	}
	return State{Running: running, PID: pid, NodeID: strings.TrimSpace(nodeID), Port: port}
}

func Start(nodeID string, port int) error {
	st := Status()
	if st.Running {
		return nil
	}

	paths := DefaultPaths()
	if err := os.MkdirAll(filepath.Dir(paths.PIDFile), 0755); err != nil {
		return fmt.Errorf("failed to create daemon state dir: %w", err)
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to resolve executable: %w", err)
	}

	logFile, err := os.OpenFile(paths.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open daemon log file: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(exe, "__daemon__", "--node-id", nodeID, "--port", strconv.Itoa(port))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	if err := writeInt(paths.PIDFile, cmd.Process.Pid); err != nil {
		return err
	}
	if err := os.WriteFile(paths.NodeFile, []byte(nodeID+"\n"), 0644); err != nil {
		return err
	}
	if err := writeInt(paths.PortFile, port); err != nil {
		return err
	}

	if err := waitForHealthy(port, 4*time.Second); err != nil {
		return fmt.Errorf("daemon did not become healthy: %w", err)
	}

	return nil
}

func Stop() error {
	paths := DefaultPaths()
	pid, err := readInt(paths.PIDFile)
	if err != nil || pid <= 0 {
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err == nil {
		_ = proc.Signal(syscall.SIGTERM)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !processRunning(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if processRunning(pid) {
		_ = proc.Signal(syscall.SIGKILL)
	}

	_ = os.Remove(paths.PIDFile)
	_ = os.Remove(paths.NodeFile)
	_ = os.Remove(paths.PortFile)
	return nil
}

func EnsureStarted(nodeID string, port int) (bool, error) {
	st := Status()
	if st.Running {
		return false, nil
	}
	if err := Start(nodeID, port); err != nil {
		return false, err
	}
	return true, nil
}

func waitForHealthy(port int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func processRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}

func readInt(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, err
	}
	return v, nil
}

func readString(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func writeInt(path string, v int) error {
	return os.WriteFile(path, []byte(strconv.Itoa(v)+"\n"), 0644)
}

type AcceptResponse struct {
	Accepted bool   `json:"accepted"`
	NodeID   string `json:"node_id"`
	Reason   string `json:"reason,omitempty"`
}

func CheckRemoteAcceptance(host string, port int, targetID, jobName string) error {
	url := fmt.Sprintf("http://%s:%d/v1/accept", host, port)
	payload := fmt.Sprintf(`{"target_id":%q,"job_name":%q}`, targetID, jobName)
	resp, err := http.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to contact remote daemon at %s: %w", url, err)
	}
	defer resp.Body.Close()

	var out AcceptResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("remote daemon returned invalid response: %w", err)
	}

	if !out.Accepted {
		reason := out.Reason
		if reason == "" {
			reason = "remote daemon rejected job"
		}
		return fmt.Errorf("job rejected by remote daemon: %s", reason)
	}
	return nil
}
