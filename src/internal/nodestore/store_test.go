package nodestore

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewAt(filepath.Join(t.TempDir(), "nodes.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestUpsertAndList(t *testing.T) {
	s := newTestStore(t)

	if err := s.UpsertNode(Node{
		NodeID: "node-a", Host: "10.0.0.1", Port: 38080,
		State: "healthy", Source: "discovery",
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	nodes, err := s.ListAll()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].NodeID != "node-a" || nodes[0].Port != 38080 {
		t.Fatalf("round-trip mismatch: %+v", nodes[0])
	}
}

// Capabilities and load survive the round trip, so GPU and core data reaches
// the scheduler.
func TestCapabilitiesAndLoadRoundTrip(t *testing.T) {
	s := newTestStore(t)

	caps := `{"cpu_cores":"32","gpu_name":"RTX 4090","gpu_compute_cap":"8.9"}`
	load := `{"cpu_percent":12.5,"gpus":[{"index":0,"memory_free_bytes":21474836480}]}`

	if err := s.UpsertNode(Node{
		NodeID: "gpu-node", Host: "10.0.0.2", Port: 38080,
		State: "healthy", Source: "discovery",
		CapsJSON: caps, LoadJSON: load,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	nodes, _ := s.ListAll()
	if nodes[0].CapsJSON != caps {
		t.Fatalf("capabilities not preserved:\n got %s\nwant %s", nodes[0].CapsJSON, caps)
	}
	if nodes[0].LoadJSON != load {
		t.Fatalf("load not preserved:\n got %s\nwant %s", nodes[0].LoadJSON, load)
	}
}

// A discovery upsert (which knows only an address) must not erase the richer
// hardware detail an earlier health poll recorded.
func TestUpsertDoesNotClobberCapabilitiesWithEmpty(t *testing.T) {
	s := newTestStore(t)

	caps := `{"cpu_cores":"64"}`
	_ = s.UpsertNode(Node{
		NodeID: "n", Host: "10.0.0.3", Port: 38080,
		State: "healthy", Source: "discovery", CapsJSON: caps,
		LoadJSON: `{"cpu_percent":5}`,
	})

	// Second upsert simulates rediscovery: address only, no capabilities.
	_ = s.UpsertNode(Node{
		NodeID: "n", Host: "10.0.0.3", Port: 38080,
		State: "unknown", Source: "discovery",
	})

	nodes, _ := s.ListAll()
	if nodes[0].CapsJSON != caps {
		t.Fatalf("rediscovery wiped capabilities: got %q, want %q", nodes[0].CapsJSON, caps)
	}
	if nodes[0].LoadJSON == "" {
		t.Fatal("rediscovery wiped load data")
	}
}

// A manually added node keeps its manual flag when discovery later finds it.
func TestManualFlagSurvivesRediscovery(t *testing.T) {
	s := newTestStore(t)

	_ = s.UpsertNode(Node{
		NodeID: "m", Host: "10.0.0.4", Port: 38080,
		Source: "manual", IsManual: true, State: "healthy",
	})
	_ = s.UpsertNode(Node{
		NodeID: "m", Host: "10.0.0.4", Port: 38080,
		Source: "discovery", State: "healthy",
	})

	manual, err := s.ListManual()
	if err != nil {
		t.Fatalf("list manual: %v", err)
	}
	if len(manual) != 1 {
		t.Fatalf("expected the node to still be manual, got %d manual entries", len(manual))
	}
	if manual[0].Source != "manual" {
		t.Fatalf("expected source to stay manual, got %q", manual[0].Source)
	}
}

// ListHealthy is the death filter: a node last seen long ago is not a
// placement candidate even if its stored state still says healthy.
func TestListHealthyExcludesStaleNodes(t *testing.T) {
	s := newTestStore(t)

	_ = s.UpsertNode(Node{
		NodeID: "fresh", Host: "10.0.0.5", Port: 38080,
		State: "healthy", Source: "discovery",
	})
	_ = s.UpsertNode(Node{
		NodeID: "stale", Host: "10.0.0.6", Port: 38080,
		State: "healthy", Source: "discovery",
		LastSeen: time.Now().Add(-1 * time.Hour).Unix(),
	})

	healthy, err := s.ListHealthy(30 * time.Second)
	if err != nil {
		t.Fatalf("list healthy: %v", err)
	}
	if len(healthy) != 1 || healthy[0].NodeID != "fresh" {
		t.Fatalf("expected only the fresh node, got %+v", healthy)
	}
}

// PruneStale drops forgotten discovered nodes but keeps manual ones, which the
// user added deliberately.
func TestPruneStaleKeepsManualNodes(t *testing.T) {
	s := newTestStore(t)

	old := time.Now().Add(-2 * time.Hour).Unix()
	_ = s.UpsertNode(Node{NodeID: "auto", Host: "h1", Port: 1, Source: "discovery", LastSeen: old})
	_ = s.UpsertNode(Node{NodeID: "man", Host: "h2", Port: 2, Source: "manual", IsManual: true, LastSeen: old})

	s.PruneStale(time.Minute)

	nodes, _ := s.ListAll()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node left, got %d: %+v", len(nodes), nodes)
	}
	if nodes[0].NodeID != "man" {
		t.Fatalf("prune removed the manual node instead of the discovered one: %+v", nodes[0])
	}
}

func TestMarkUnreachableClearsLiveCounters(t *testing.T) {
	s := newTestStore(t)

	_ = s.UpsertNode(Node{
		NodeID: "n", Host: "h", Port: 1, State: "healthy", Source: "discovery",
		ActiveJobs: 3, AvailableMem: 1 << 30,
	})
	s.MarkUnreachable("n")

	nodes, _ := s.ListAll()
	if nodes[0].State != "unreachable" {
		t.Fatalf("expected state unreachable, got %q", nodes[0].State)
	}
	if nodes[0].ActiveJobs != 0 || nodes[0].AvailableMem != 0 {
		t.Fatalf("unreachable node should report no capacity, got jobs=%d mem=%d",
			nodes[0].ActiveJobs, nodes[0].AvailableMem)
	}
}

func TestDeleteNode(t *testing.T) {
	s := newTestStore(t)
	_ = s.UpsertNode(Node{NodeID: "gone", Host: "h", Port: 1, Source: "manual", IsManual: true})

	if err := s.DeleteNode("gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	nodes, _ := s.ListAll()
	if len(nodes) != 0 {
		t.Fatalf("expected node removed, got %+v", nodes)
	}
}

// An existing database created before caps_json/load_json existed must migrate
// in place rather than failing to open.
func TestMigrationFromPreviousSchema(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")

	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE nodes (
			node_id       TEXT PRIMARY KEY,
			host          TEXT NOT NULL,
			port          INTEGER NOT NULL,
			ssh_endpoint  TEXT,
			arch          TEXT,
			hostname      TEXT,
			state         TEXT DEFAULT 'unknown',
			active_jobs   INTEGER DEFAULT 0,
			available_mem INTEGER DEFAULT 0,
			reserved_mem  INTEGER DEFAULT 0,
			total_mem     INTEGER DEFAULT 0,
			cpu_percent   REAL DEFAULT 0,
			health_score  REAL DEFAULT 0,
			source        TEXT NOT NULL,
			is_manual     INTEGER DEFAULT 0,
			last_seen     INTEGER DEFAULT 0
		);
		INSERT INTO nodes (node_id, host, port, source, state, last_seen)
		VALUES ('legacy-node', '10.0.0.9', 38080, 'manual', 'healthy', 0);
	`); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}
	db.Close()

	s, err := NewAt(p)
	if err != nil {
		t.Fatalf("opening a pre-existing database should migrate, not fail: %v", err)
	}
	defer s.Close()

	nodes, err := s.ListAll()
	if err != nil {
		t.Fatalf("list after migration: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "legacy-node" {
		t.Fatalf("existing rows should survive migration, got %+v", nodes)
	}

	// And the new columns must be usable.
	nodes[0].CapsJSON = `{"cpu_cores":"8"}`
	if err := s.UpsertNode(nodes[0]); err != nil {
		t.Fatalf("write to migrated schema: %v", err)
	}
	reread, _ := s.ListAll()
	if reread[0].CapsJSON != `{"cpu_cores":"8"}` {
		t.Fatalf("migrated column did not persist, got %q", reread[0].CapsJSON)
	}
}

// Migration must be idempotent — the daemon reopens this database every start.
func TestMigrationIsIdempotent(t *testing.T) {
	p := filepath.Join(t.TempDir(), "repeat.db")
	for i := 0; i < 3; i++ {
		s, err := NewAt(p)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		s.Close()
	}
}
