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
		{"killed by signal", fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: 137}), false},
		{"lost connection", errors.New("execution failed: stdout scanner: unexpected EOF"), false},
		{"upload", errors.New("upload failed: connection refused"), false},
	} {
		if got := IsPermanent(tc.err); got != tc.want {
			t.Errorf("%s: IsPermanent = %v, want %v", tc.name, got, tc.want)
		}
	}
}
