package jobhistory

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrune(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_DATA_HOME", base)

	oldDir := filepath.Join(BaseDir(), "oldjob")
	if err := os.MkdirAll(oldDir, 0755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-10 * 24 * time.Hour)
	if err := os.Chtimes(oldDir, old, old); err != nil {
		t.Fatal(err)
	}
	freshDir := filepath.Join(BaseDir(), "freshjob")
	if err := os.MkdirAll(freshDir, 0755); err != nil {
		t.Fatal(err)
	}

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
