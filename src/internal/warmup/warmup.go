// Package warmup builds the common Python environment in the background
// after `connect`, so the first real script starts fast without any prep
// script (D9). Progress goes to userdir.State()/warmup.log (C19): the first
// line is the header (it carries the build and torch kind), the last line is
// the current step, "done" when finished.
package warmup

import (
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

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

// kind is the torch build named in the log header.
func kind(nvidia bool) string {
	if nvidia {
		return "CUDA"
	}
	return "CPU"
}

// shell runs "$0" "$@" (the pipedpeer run), appending its output to the log,
// then the final line.
const shell = `"$0" "$@" >>"$PIPEDPEER_WARMUP_LOG" 2>&1 && echo done >>"$PIPEDPEER_WARMUP_LOG" ||
echo "failed (exit $?); details above" >>"$PIPEDPEER_WARMUP_LOG"`

// Running reports whether a warm-up started within the last hour has not
// written its final line. An empty log has no progress line, so it is not
// running.
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
	if last == "" {
		return false
	}
	return last != "done" && !strings.HasPrefix(last, "failed")
}

// Result says what Start did.
type Result int

const (
	Started        Result = iota // a new build was launched
	AlreadyRunning               // another warm-up is running (or starting)
	AlreadyDone                  // finished for this build and torch kind
	RecentlyFailed               // this build failed within failBackoff; not retried yet
)

// failBackoff is how long a failed build is not retried: setup and connect
// would otherwise re-launch the same multi-GB build on every run.
const failBackoff = 10 * time.Minute

// buildable reports whether version identifies one build. "dev" and an
// empty version name every local build alike, and a version with spaces
// cannot be read back from the header, so none of them is ever Done or
// backed off: they rebuild (the run itself is cached by nix).
func buildable(version string) bool {
	return version != "" && version != "dev" && !strings.ContainsAny(version, " \t\r\n")
}

// readLog returns the log's lines, or nil when absent or unreadable.
func readLog() []string {
	body, err := os.ReadFile(LogPath())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "pipedpeer: reading %s: %v\n", LogPath(), err)
		}
		return nil
	}
	return strings.Split(strings.TrimSpace(string(body)), "\n")
}

// sameBuild reports whether the header carries "build=<version>
// torch=<kind>" for this build.
func sameBuild(header, version string, nvidia bool) bool {
	return buildable(version) && headerMatches(header, version, nvidia)
}

func headerMatches(header, version string, nvidia bool) bool {
	f := strings.Fields(header)
	n := len(f)
	return n >= 2 && f[n-2] == "build="+version && f[n-1] == "torch="+kind(nvidia)
}

// RetryEnv, when set, skips the failure backoff: the user has fixed the cause.
const RetryEnv = "PIPEDPEER_WARMUP_RETRY"

// Done reports whether the log is a finished warm-up for this build and torch
// kind: the last line is "done" and the header ends with the marker
// "build=<version> torch=<CPU|CUDA>". A log from a build without the marker
// is not Done, so the first run after an upgrade rebuilds once.
func Done(version string, nvidia bool) bool {
	lines := readLog()
	if len(lines) < 2 || lines[len(lines)-1] != "done" {
		return false
	}
	return sameBuild(lines[0], version, nvidia)
}

// recentlyFailed: the last line is a failure of this same build, written
// less than failBackoff ago.
func recentlyFailed(version string, nvidia bool) bool {
	if os.Getenv(RetryEnv) != "" {
		return false
	}
	fi, err := os.Stat(LogPath())
	if err != nil || time.Since(fi.ModTime()) > failBackoff {
		return false
	}
	lines := readLog()
	return len(lines) >= 2 && strings.HasPrefix(lines[len(lines)-1], "failed") &&
		headerMatches(lines[0], version, nvidia)
}

// Start launches the warm-up detached from this process (it outlives the
// CLI) and returns at once. It does nothing when a warm-up is Running, Done
// for this version and torch kind, or failed a moment ago. A lock file makes
// the check and the launch one step across processes.
func Start(exe, version string, nvidia bool) (Result, error) {
	if err := os.MkdirAll(userdir.State(), 0o755); err != nil {
		return Started, fmt.Errorf("state dir: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(userdir.State(), "warmup.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return Started, fmt.Errorf("warm-up lock: %w", err)
	}
	defer lock.Close() // closing releases the flock
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return AlreadyRunning, nil
		}
		return Started, fmt.Errorf("warm-up lock: %w", err)
	}
	switch {
	case Running():
		return AlreadyRunning, nil
	case Done(version, nvidia):
		return AlreadyDone, nil
	case recentlyFailed(version, nvidia):
		return RecentlyFailed, nil
	}
	// Its own directory: run ships the working directory as the workspace.
	dir := filepath.Join(userdir.Data(), "warmup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Started, fmt.Errorf("warm-up dir: %w", err)
	}
	script := filepath.Join(dir, "common.py")
	if err := os.WriteFile(script, commonPy, 0o644); err != nil {
		return Started, fmt.Errorf("writing %s: %w", script, err)
	}
	// An empty ignore file marks this dir as the project root; without it
	// run walks up to a .git in $HOME and packs the whole home directory.
	if err := os.WriteFile(filepath.Join(dir, ".pipedpeerignore"), nil, 0o644); err != nil {
		return Started, fmt.Errorf("writing warm-up ignore file: %w", err)
	}
	// The earlier log (a failure's details) survives one rerun.
	if err := os.Rename(LogPath(), LogPath()+".prev"); err != nil && !errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "pipedpeer: keeping the earlier warm-up log: %v\n", err)
	}
	head := fmt.Sprintf("%s building the common environment (numpy, pandas, scikit-learn, scipy, "+
		"matplotlib, seaborn, tqdm, torch %s) build=%s torch=%s\n",
		time.Now().Format(time.RFC3339), kind(nvidia), version, kind(nvidia))
	if err := os.WriteFile(LogPath(), []byte(head), 0o644); err != nil {
		return Started, fmt.Errorf("writing %s: %w", LogPath(), err)
	}
	cmd := termuxexec.Command("sh", append([]string{"-c", shell, exe}, Args(script, nvidia)...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PIPEDPEER_WARMUP_LOG="+LogPath())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		// A header-only log would read as "building" for an hour.
		if f, ferr := os.OpenFile(LogPath(), os.O_APPEND|os.O_WRONLY, 0o644); ferr == nil {
			fmt.Fprintf(f, "failed (could not start: %v)\n", err)
			f.Close()
		} else {
			fmt.Fprintf(os.Stderr, "pipedpeer: recording the failed warm-up start: %v\n", ferr)
		}
		return Started, fmt.Errorf("starting the warm-up: %w", err)
	}
	return Started, cmd.Process.Release()
}
