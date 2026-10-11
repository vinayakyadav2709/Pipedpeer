package daemonapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

// TestTheDaemonPrunerNeverTouchesJobHistory: the daemon prunes its own job
// dirs hourly at JobRetention. When those lived in jobhistory.BaseDir() the
// same sweep deleted the CLI's history too, whatever `jobs prune` was told.
func TestTheDaemonPrunerNeverTouchesJobHistory(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("j", 8))
	defer s.StopWarmWorkers()

	if s.jobDir == jobhistory.BaseDir() {
		t.Fatalf("daemon job dir is the history dir %s", s.jobDir)
	}
	old := time.Now().Add(-2 * JobRetention)
	mkOld := func(dir string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(dir, old, old); err != nil {
			t.Fatal(err)
		}
	}
	history := filepath.Join(jobhistory.BaseDir(), "1700000000000000000")
	work := filepath.Join(s.jobDir, "00000000-0000-4000-8000-0000000000aa") // a daemon job ID
	mkOld(history)
	mkOld(work)

	n, err := s.PruneCompletedJobDirs(JobRetention)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d dirs, want 1 (the daemon's own)", n)
	}
	if _, err := os.Stat(work); !os.IsNotExist(err) {
		t.Fatalf("old daemon job dir survived: %v", err)
	}
	if _, err := os.Stat(history); err != nil {
		t.Fatalf("job history was pruned by the daemon: %v", err)
	}
}
