package app

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

func TestIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"build", &BuildError{Err: errors.New("nix build failed: exit status 1")}, true},
		{"script error", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 1}), true},
		{"script exit 125", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 125}), true},
		{"run not executable", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 126}), false},
		{"run missing", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 127}), false},
		{"sandbox setup", fmt.Errorf("execution failed: %w", &SandboxError{Err: &daemonctl.RemoteExitError{Code: 1}}), false},
		{"killed by signal", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 137}), false},
		{"lost connection", errors.New("execution failed: stdout scanner: unexpected EOF"), false},
		{"upload", errors.New("upload failed: connection refused"), false},
	} {
		if got := IsPermanent(tc.err); got != tc.want {
			t.Errorf("%s: IsPermanent = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSandboxFailed(t *testing.T) {
	exit1 := &daemonctl.RemoteExitError{Code: 1}
	for _, tc := range []struct {
		name   string
		err    error
		stderr string
		want   bool
	}{
		{"crun mount", exit1, "mount `proc` to `/proc`: Operation not permitted\n", true},
		{"crun unshare", exit1, "unshare: Operation not permitted\n", true},
		{"crun chdir", exit1, "chdir `/work` failed: No such file or directory\n\n", true},
		{"python traceback", exit1, "Traceback (most recent call last):\n  File \"m.py\", line 1\nPermissionError: [Errno 1] Operation not permitted: '/x'\n", false},
		{"script message", exit1, "loading data\nerror: bad input\n", false},
		{"script printed crun-like then traceback", exit1, "open `x`: Permission denied\nTraceback (most recent call last):\nValueError: no\n", false},
		{"not an exit", errors.New("execution failed: EOF"), "unshare: Operation not permitted\n", false},
		{"signal", &daemonctl.RemoteExitError{Code: 137}, "unshare: Operation not permitted\n", false},
	} {
		if got := sandboxFailed(tc.err, tc.stderr); got != tc.want {
			t.Errorf("%s: sandboxFailed = %v, want %v", tc.name, got, tc.want)
		}
	}
}
