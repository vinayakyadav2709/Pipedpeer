package main

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/setup"
)

// The setup command hands its build to setup, which restarts a running
// daemon of another build on upgrade; unset, no daemon ever looked stale.
func TestSetupCmdSetsVersion(t *testing.T) {
	for _, v := range []string{"0.0.1-nightly.20261104-abc1234", "dev"} {
		t.Run(v, func(t *testing.T) {
			stubSetupRuns(t, 4242, connConnected)
			oldV, oldSV := version, setup.Version
			t.Cleanup(func() { version, setup.Version = oldV, oldSV })
			version, setup.Version = v, ""
			cmd := newSetupCmd()
			cmd.SetArgs([]string{"--no-install"})
			if err := cmd.Execute(); err != nil {
				t.Fatalf("setup: %v", err)
			}
			if setup.Version != v {
				t.Fatalf("setup.Version = %q, want %q", setup.Version, v)
			}
		})
	}
}
