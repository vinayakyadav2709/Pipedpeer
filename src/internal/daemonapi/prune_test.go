package daemonapi

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The daemon's job directories share a parent with the submitter's job
// history and the NAR cache. Daemon start-up used to prune every old
// directory there, so `pipedpeer jobs` lost its history on each restart.
func TestPruneCompletedJobDirsOnlyTouchesDaemonJobs(t *testing.T) {
	s := New("node-prune")
	s.jobDir = t.TempDir()

	old := time.Now().Add(-30 * 24 * time.Hour)
	mk := func(name string) string {
		dir := filepath.Join(s.jobDir, name)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(dir, old, old); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	daemonJob := mk("29ee7061-c933-4d5c-abfa-fa7c11ca61df")
	history := mk("1790955244747470053")
	narCache := mk("nar-cache")

	n, err := s.PruneCompletedJobDirs(JobRetention)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d dirs, want 1", n)
	}
	if _, err := os.Stat(daemonJob); !os.IsNotExist(err) {
		t.Fatalf("stale daemon job dir survived: %v", err)
	}
	for _, dir := range []string{history, narCache} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("%s removed by the daemon pruner: %v", filepath.Base(dir), err)
		}
	}
}
