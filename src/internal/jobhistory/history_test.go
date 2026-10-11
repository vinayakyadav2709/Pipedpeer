package jobhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveByPrefixAndShortID(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	for _, id := range []string{"1760000000111111111", "1760000000222222222"} {
		dir := filepath.Join(BaseDir(), id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := SaveRecord(dir, Record{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if r, _, err := Resolve(ShortID("1760000000222222222")); err != nil || r.ID != "1760000000222222222" {
		t.Fatalf("short id: got %q, %v", r.ID, err)
	}
	if r, _, err := Resolve("17600000001"); err != nil || r.ID != "1760000000111111111" {
		t.Fatalf("prefix: got %q, %v", r.ID, err)
	}
	if _, _, err := Resolve("1760"); err == nil {
		t.Fatal("ambiguous prefix resolved")
	}
	if _, _, err := Resolve("99"); err == nil {
		t.Fatal("unknown id resolved")
	}
}

func TestFinalizeCancelled(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	r, dir, err := NewRecord("x.py", "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := Finalize(dir, r, fmt.Errorf("execution failed: %w", context.Canceled)); err != nil {
		t.Fatal(err)
	}
	got, err := ReadRecordAt(dir)
	if err != nil || got.Status != StateCancelled {
		t.Fatalf("status %q, %v", got.Status, err)
	}
}
