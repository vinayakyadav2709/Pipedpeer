package jobhistory

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeEntry(t *testing.T, name string, record bool, age time.Duration) string {
	t.Helper()
	dir := filepath.Join(BaseDir(), name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if record {
		if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if age > 0 {
		when := time.Now().Add(-age)
		if err := os.Chtimes(dir, when, when); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestPrune(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_DATA_HOME", base)

	oldDir := makeEntry(t, "oldjob", true, 10*24*time.Hour)
	freshDir := makeEntry(t, "freshjob", true, 0)

	n, err := Prune(7 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d entries, want 1", n)
	}
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("old entry still exists: %v", err)
	}
	if _, err := os.Stat(freshDir); err != nil {
		t.Fatalf("fresh entry removed: %v", err)
	}
}

// The history shares its directory with the daemon's job directories and NAR
// cache. Pruning history must leave those alone however old they are.
func TestPruneLeavesDaemonEntries(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_DATA_HOME", base)

	daemonJob := makeEntry(t, "29ee7061-c933-4d5c-abfa-fa7c11ca61df", false, 30*24*time.Hour)
	narCache := makeEntry(t, "nar-cache", false, 30*24*time.Hour)

	n, err := Prune(7 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pruned %d entries, want 0", n)
	}
	for _, dir := range []string{daemonJob, narCache} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("%s removed by the history pruner: %v", filepath.Base(dir), err)
		}
	}
}
