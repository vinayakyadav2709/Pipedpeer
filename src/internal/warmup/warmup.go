// Package warmup builds the common Python environment in the background
// after `connect`, so the first real script starts fast without any prep
// script (D9). Progress goes to userdir.State()/warmup.log (C19): the last
// line is the current step, "done" when finished.
package warmup

import (
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

//go:embed common.py
var commonPy []byte

// LogPath is the warm-up log `status` reads.
func LogPath() string { return filepath.Join(userdir.State(), "warmup.log") }

// Args is the run that builds the environment. Without a usable NVIDIA GPU
// torch is the CPU build: the CUDA wheel set is gigabytes nobody here uses.
func Args(script string, nvidia bool) []string {
	a := []string{"run", script, "--check-only"}
	if !nvidia {
		a = append(a, "--gpu", "off")
	}
	return a
}

// shell runs "$0" "$@" (the pipedpeer run), appending its output to the log,
// then the final line.
const shell = `"$0" "$@" >>"$PIPEDPEER_WARMUP_LOG" 2>&1 && echo done >>"$PIPEDPEER_WARMUP_LOG" ||
echo "failed (exit $?); details above" >>"$PIPEDPEER_WARMUP_LOG"`

// Running reports whether a warm-up started within the last hour has not
// written its final line.
// ponytail: a killed warm-up blocks a new one for up to an hour; a pid file
// if that bites.
func Running() bool {
	fi, err := os.Stat(LogPath())
	if err != nil || time.Since(fi.ModTime()) > time.Hour {
		return false
	}
	body, err := os.ReadFile(LogPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: reading %s: %v\n", LogPath(), err)
		return false
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	last := lines[len(lines)-1]
	return last != "done" && !strings.HasPrefix(last, "failed")
}

// Start launches the warm-up detached from this process (it outlives the
// CLI) and returns at once.
func Start(exe string, nvidia bool) error {
	if Running() {
		return nil
	}
	// Its own directory: run ships the working directory as the workspace.
	dir := filepath.Join(userdir.Data(), "warmup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("warm-up dir: %w", err)
	}
	script := filepath.Join(dir, "common.py")
	if err := os.WriteFile(script, commonPy, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", script, err)
	}
	if err := os.MkdirAll(userdir.State(), 0o755); err != nil {
		return fmt.Errorf("state dir: %w", err)
	}
	torch := "torch CPU"
	if nvidia {
		torch = "torch CUDA"
	}
	head := fmt.Sprintf("%s building the common environment (numpy, pandas, scikit-learn, scipy, "+
		"matplotlib, seaborn, tqdm, %s)\n", time.Now().Format(time.RFC3339), torch)
	if err := os.WriteFile(LogPath(), []byte(head), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", LogPath(), err)
	}
	cmd := exec.Command("sh", append([]string{"-c", shell, exe}, Args(script, nvidia)...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PIPEDPEER_WARMUP_LOG="+LogPath())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting the warm-up: %w", err)
	}
	return cmd.Process.Release()
}
