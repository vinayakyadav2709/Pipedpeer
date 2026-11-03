package daemonapi

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The hourly sweeper tick prunes stale scratch dirs, so a daemon that is
// never restarted still cleans up.
func TestSweepTickPrunesScratch(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	root := filepath.Join(os.Getenv("XDG_CACHE_HOME"), "pipedpeer", "scratch")
	old := filepath.Join(root, "build-old")
	fresh := filepath.Join(root, "build-fresh")
	for _, d := range []string{old, fresh} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ts := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(old, ts, ts); err != nil {
		t.Fatal(err)
	}
	s := NewWithConfig("sweep-node", time.Minute, time.Minute, 10*time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(old); os.IsNotExist(err) {
			break
		}
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("stale scratch dir survived the sweeper tick")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh scratch dir removed: %v", err)
	}
}
