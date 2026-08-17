package daemonapi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// state persists the daemon's in-memory maps (leases + jobs) to disk so a
// daemon restart does not lose them. It is a coarse JSON snapshot, written on
// every mutation and read once at startup. Coarse is deliberate: the maps are
// small and the daemon is a single process, so there is no contention to
// optimise and no need for a real database. Running leases that survive a
// restart are reaped by the sweeper because their RenewedAt is stale.
type state struct {
	path string
	mu   sync.Mutex
}

func newState() *state {
	return &state{path: filepath.Join(defaultJobDir(), "daemon-state.json")}
}

type stateSnapshot struct {
	Leases map[string]*Lease     `json:"leases"`
	Jobs   map[string]*JobRecord `json:"jobs"`
}

// load reads the snapshot into the caller's maps. Missing or corrupt files are
// treated as empty — a fresh start is always safe.
func (st *state) load(leases map[string]*Lease, jobs map[string]*JobRecord) {
	st.mu.Lock()
	defer st.mu.Unlock()

	b, err := os.ReadFile(st.path)
	if err != nil {
		return
	}
	var snap stateSnapshot
	if json.Unmarshal(b, &snap) != nil {
		return
	}
	for k, v := range snap.Leases {
		if v != nil {
			leases[k] = v
		}
	}
	for k, v := range snap.Jobs {
		if v != nil {
			jobs[k] = v
		}
	}
}

// save writes the current maps to disk atomically.
func (st *state) save(leases map[string]*Lease, jobs map[string]*JobRecord) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(st.path), 0755); err != nil {
		return
	}
	snap := stateSnapshot{Leases: leases, Jobs: jobs}
	b, err := json.Marshal(snap)
	if err != nil {
		return
	}
	tmp := st.path + ".tmp"
	if os.WriteFile(tmp, b, 0644) != nil {
		return
	}
	_ = os.Rename(tmp, st.path)
}
