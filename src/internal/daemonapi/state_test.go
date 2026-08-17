package daemonapi

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := &state{path: filepath.Join(dir, "daemon-state.json")}

	leases := map[string]*Lease{
		"L1": {LeaseID: "L1", JobName: "j1", State: LeaseRunning, CreatedAt: time.Now()},
	}
	jobs := map[string]*JobRecord{
		"J1": {JobID: "J1", WorkDir: "/tmp/work", Status: "done", StorePath: "/nix/store/abc"},
	}
	st.save(leases, jobs)

	// Fresh maps, reload.
	l2 := map[string]*Lease{}
	j2 := map[string]*JobRecord{}
	st.load(l2, j2)

	if len(l2) != 1 || l2["L1"].JobName != "j1" {
		t.Fatalf("leases not restored: %+v", l2)
	}
	if len(j2) != 1 || j2["J1"].Status != "done" {
		t.Fatalf("jobs not restored: %+v", j2)
	}
}

func TestStateLoadMissingFile(t *testing.T) {
	st := &state{path: filepath.Join(t.TempDir(), "nope.json")}
	leases := map[string]*Lease{}
	jobs := map[string]*JobRecord{}
	st.load(leases, jobs) // must not panic, must be empty
	if len(leases) != 0 || len(jobs) != 0 {
		t.Fatal("missing state should load empty")
	}
	_ = os.MkdirAll(filepath.Dir(st.path), 0755)
	os.WriteFile(st.path, []byte("not json"), 0644)
	st.load(leases, jobs) // corrupt file must not panic
	if len(leases) != 0 {
		t.Fatal("corrupt state should load empty")
	}
}
