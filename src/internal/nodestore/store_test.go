package nodestore

import (
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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

// Removal must say when it deleted nothing — a 200 no-op is how ghost node
// entries survived cleanup — and short ID prefixes must resolve like the ones
// `pipedpeer nodes` prints.
func TestRemoveReportsCountsAndResolvesPrefixes(t *testing.T) {
	s := newTestStore(t)
	_ = s.UpsertNode(Node{NodeID: "2b74d935-b039-4adb", Host: "10.0.0.9", Port: 38081, Source: "manual", IsManual: true})
	_ = s.UpsertNode(Node{NodeID: "2b7fffff-0000-0000", Host: "10.0.0.9", Port: 38082, Source: "discovery"})

	if n, err := s.RemoveManual("no-such-host"); err != nil || n != 0 {
		t.Fatalf("no-op remove: n=%d err=%v, want 0 rows", n, err)
	}

	if _, err := s.ResolveNodeID("2b7"); err == nil {
		t.Fatal("ambiguous prefix must error, not pick one")
	}
	id, err := s.ResolveNodeID("2b74d935")
	if err != nil || id != "2b74d935-b039-4adb" {
		t.Fatalf("prefix resolve: id=%q err=%v", id, err)
	}
	if id, _ := s.ResolveNodeID("ffffffff"); id != "" {
		t.Fatalf("unknown prefix resolved to %q", id)
	}

	if n, err := s.RemoveManual("10.0.0.9"); err != nil || n != 1 {
		t.Fatalf("manual remove by host: n=%d err=%v, want 1 (discovery entry must survive)", n, err)
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

// TestReaddingAnAddressReplacesTheOldIdentity. A worker that comes back with a
// new node ID - a container recreated, a machine reinstalled - used to land
// beside its old entry rather than replacing it, and manual entries are exempt
// from PruneStale, so they accumulated forever. Eleven rows for a four-node
// cluster, each dead one health-checked on every cycle.
//
// Goes through AddManual rather than the helper it calls: an earlier version
// of this test called the helper directly and stayed green when the call site
// was deleted, which is the failure this whole audit exists to catch.
func TestReaddingAnAddressReplacesTheOldIdentity(t *testing.T) {
	s := newTestStore(t)

	// One address, answering with a different identity each time - a container
	// recreated between the two adds.
	var nodeID atomic.Value
	nodeID.Store("gen1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node_id":%q}`, nodeID.Load().(string))
	}))
	defer srv.Close()
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)

	// A manual neighbour at another port, and a discovered row at this one:
	// neither is ours to remove.
	_ = s.UpsertNode(Node{NodeID: "other", Host: host, Port: port + 1, Source: "manual", IsManual: true})
	_ = s.UpsertNode(Node{NodeID: "found", Host: host, Port: port, Source: "discovery"})

	if err := s.AddManual(host, port); err != nil {
		t.Fatal(err)
	}
	nodeID.Store("gen2")
	if err := s.AddManual(host, port); err != nil {
		t.Fatal(err)
	}

	nodes, _ := s.ListAll()
	got := map[string]bool{}
	for _, n := range nodes {
		got[n.NodeID] = true
	}
	if got["gen1"] {
		t.Errorf("the superseded identity at %s:%d survived: %v", host, port, keysOf(got))
	}
	if !got["gen2"] {
		t.Errorf("the node currently at %s:%d is missing: %v", host, port, keysOf(got))
	}
	if !got["other"] {
		t.Errorf("a manual node at a different port was removed: %v", keysOf(got))
	}
	if !got["found"] {
		t.Errorf("a discovered node was removed; only manual residue is ours to " +
			"clear, and deleting discovered rows here races the discovery loop")
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
