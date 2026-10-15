package nodestore

import (
	"database/sql"
	"os"
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

// setClock pins the store's clock for the rest of the test.
func setClock(t *testing.T, at time.Time) {
	t.Helper()
	old := now
	now = func() time.Time { return at }
	t.Cleanup(func() { now = old })
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func nodeByID(t *testing.T, s *Store, id string) (Node, bool) {
	t.Helper()
	nodes, err := s.ListAll()
	must(t, err)
	for _, n := range nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return Node{}, false
}

func ids(t *testing.T, s *Store) map[string]bool {
	t.Helper()
	nodes, err := s.ListAll()
	must(t, err)
	out := map[string]bool{}
	for _, n := range nodes {
		out[n.NodeID] = true
	}
	return out
}

func TestUpsertAndList(t *testing.T) {
	s := newTestStore(t)
	must(t, s.UpsertNode(Node{NodeID: "node-a", Hostname: "a", State: "healthy"}))
	must(t, s.SeePath("node-a", PathLAN, "10.0.0.1", 4242))

	n, ok := nodeByID(t, s, "node-a")
	if !ok || n.Hostname != "a" || n.Host != "10.0.0.1" || n.Port != 4242 || n.Source != "lan" {
		t.Fatalf("round-trip mismatch: %+v", n)
	}
}

// Capabilities and load survive the round trip, so GPU and core data reaches
// the scheduler.
func TestCapabilitiesAndLoadRoundTrip(t *testing.T) {
	s := newTestStore(t)
	caps := `{"cpu_cores":"32","gpu_name":"RTX 4090","gpu_compute_cap":"8.9"}`
	load := `{"cpu_percent":12.5,"gpus":[{"index":0,"memory_free_bytes":21474836480}]}`
	must(t, s.UpsertNode(Node{NodeID: "gpu-node", State: "healthy", CapsJSON: caps, LoadJSON: load}))

	n, _ := nodeByID(t, s, "gpu-node")
	if n.CapsJSON != caps || n.LoadJSON != load {
		t.Fatalf("caps/load not preserved: %+v", n)
	}
}

// An upsert that knows less must not erase the richer hardware detail an
// earlier health poll recorded, and must never move the address.
func TestUpsertKeepsCapsAndNeverMovesTheAddress(t *testing.T) {
	s := newTestStore(t)
	caps := `{"cpu_cores":"64"}`
	must(t, s.PathOK("n", PathInternet, "127.0.0.1", 40001))
	must(t, s.UpsertNode(Node{NodeID: "n", State: "healthy", CapsJSON: caps, LoadJSON: `{"cpu_percent":5}`}))
	must(t, s.UpsertNode(Node{NodeID: "n", Host: "10.0.0.3", Port: 4242, Source: "lan", State: "unknown"}))

	n, _ := nodeByID(t, s, "n")
	if n.CapsJSON != caps || n.LoadJSON == "" {
		t.Fatalf("second upsert wiped capabilities/load: %+v", n)
	}
	if n.Host != "127.0.0.1" || n.Port != 40001 || n.Source != "internet" {
		t.Fatalf("UpsertNode moved the address: %+v", n)
	}
}

// E5e: the QUIC link was up, then LAN discovery overwrote the working
// internet address with an unreachable LAN one and the row flapped. A
// discovery candidate must never replace a verified internet path, even when
// the candidate is tried and fails.
func TestDiscoveryCandidateNeverReplacesVerifiedInternetPath(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("d", PathInternet, "127.0.0.1", 34089))
	must(t, s.SeePath("d", PathLAN, "10.79.220.165", 4242))

	n, _ := nodeByID(t, s, "d")
	if n.Host != "127.0.0.1" || n.Port != 34089 || n.Source != "internet" || n.Reason != "" {
		t.Fatalf("candidate replaced the internet path: %+v", n)
	}
	must(t, s.PathFailed("d", "10.79.220.165", 4242, "context deadline exceeded"))
	n, _ = nodeByID(t, s, "d")
	if n.Host != "127.0.0.1" || n.Reason != "" {
		t.Fatalf("a failed candidate disturbed the active path: %+v", n)
	}
	ps, err := s.Paths("d")
	must(t, err)
	if len(ps) != 2 || !ps[0].Active || ps[0].Kind != PathInternet || ps[1].LastErr == "" {
		t.Fatalf("paths: %+v", ps)
	}
}

// A moved peer: its new LAN address is a candidate until verified, and the
// old one stays active while it still answers.
func TestMovedPeerNewLANPathActiveOnlyAfterPathOK(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("b", PathLAN, "10.77.1.3", 4242))
	must(t, s.SeePath("b", PathLAN, "10.77.1.23", 4242))
	if n, _ := nodeByID(t, s, "b"); n.Host != "10.77.1.3" {
		t.Fatalf("a sighting moved the active path: %+v", n)
	}

	// The new address answers while the old one still does too: keep it.
	must(t, s.PathOK("b", PathLAN, "10.77.1.23", 4242))
	if n, _ := nodeByID(t, s, "b"); n.Host != "10.77.1.3" {
		t.Fatalf("an answering incumbent of the same kind was replaced: %+v", n)
	}

	// The old address stops answering: the shown reason says so until the
	// verified challenger takes over on its next success.
	must(t, s.PathFailed("b", "10.77.1.3", 4242, "no route to host"))
	if n, _ := nodeByID(t, s, "b"); n.Host != "10.77.1.3" || n.Reason != "no route to host" {
		t.Fatalf("failed incumbent: %+v", n)
	}
	must(t, s.PathOK("b", PathLAN, "10.77.1.23", 4242))
	if n, _ := nodeByID(t, s, "b"); n.Host != "10.77.1.23" || n.Reason != "" {
		t.Fatalf("verified challenger did not take over from a failing incumbent: %+v", n)
	}
}

func TestVerifiedLANBeatsVerifiedInternet(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("p", PathInternet, "127.0.0.1", 40001))
	must(t, s.PathOK("p", PathLAN, "192.168.1.9", 4242))
	if n, _ := nodeByID(t, s, "p"); n.Source != "lan" || n.Host != "192.168.1.9" {
		t.Fatalf("verified lan should beat verified internet: %+v", n)
	}
	// And internet does not take it back while lan answers.
	must(t, s.PathOK("p", PathInternet, "127.0.0.1", 40001))
	if n, _ := nodeByID(t, s, "p"); n.Source != "lan" {
		t.Fatalf("internet replaced an answering lan path: %+v", n)
	}
}

// Whoever answers (authenticated) at an address is the node there: a
// forwarder port reused by another peer, a recreated container.
func TestPathOKTakesTheAddressFromOtherNodes(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("old", PathInternet, "127.0.0.1", 40001))
	must(t, s.PathOK("other", PathInternet, "127.0.0.1", 40002))
	must(t, s.PathOK("new", PathInternet, "127.0.0.1", 40001))

	if ps, _ := s.Paths("old"); len(ps) != 0 {
		t.Fatalf("superseded node kept the address: %+v", ps)
	}
	if ps, _ := s.Paths("other"); len(ps) != 1 {
		t.Fatalf("a different port was touched: %+v", ps)
	}
}

func TestSelfPathReplacesOldSelfAddress(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("me", PathSelf, "10.0.0.2", 4242))
	must(t, s.PathOK("me", PathSelf, "10.24.7.5", 4242))
	ps, err := s.Paths("me")
	must(t, err)
	if len(ps) != 1 || ps[0].Host != "10.24.7.5" || !ps[0].Active {
		t.Fatalf("self paths: %+v", ps)
	}
}

func TestSightingNeverDowngradesKind(t *testing.T) {
	s := newTestStore(t)
	must(t, s.SeePath("n", PathLAN, "10.0.0.7", 4242))
	must(t, s.SeePath("n", PathGossip, "10.0.0.7", 4242))
	if ps, _ := s.Paths("n"); len(ps) != 1 || ps[0].Kind != PathLAN {
		t.Fatalf("gossip downgraded a lan path: %+v", ps)
	}
}

func TestBadPathsAreRefused(t *testing.T) {
	s := newTestStore(t)
	for _, c := range []struct {
		id, host string
		kind     PathKind
		port     int
	}{
		{"", "h", PathLAN, 1}, {"n", "", PathLAN, 1}, {"n", "h", PathLAN, 0},
		{"n", "h", PathLAN, 70000}, {"n", "h", "manual", 1},
	} {
		if err := s.SeePath(c.id, c.kind, c.host, c.port); err == nil {
			t.Errorf("SeePath(%q, %q, %q, %d) accepted", c.id, c.kind, c.host, c.port)
		}
	}
}

// Prune rules, and E4: a sighting alone never keeps a node alive.
func TestPruneRules(t *testing.T) {
	s := newTestStore(t)
	t0 := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	setClock(t, t0)
	must(t, s.PathOK("me", PathSelf, "10.0.0.1", 4242))
	must(t, s.PathOK("verified", PathLAN, "10.0.0.2", 4242))
	must(t, s.SeePath("candidate", PathLAN, "10.0.0.3", 4242))
	must(t, s.SeePath("gossiped", PathGossip, "203.0.113.4", 4243))
	must(t, s.SeePath("inet-unverified", PathInternet, "127.0.0.1", 40009))
	must(t, s.PathOK("moved", PathLAN, "10.77.1.3", 4242))

	// 3 min: the never-verified lan/gossip nodes go, even though the
	// candidate is still being sighted.
	setClock(t, t0.Add(3*time.Minute))
	must(t, s.SeePath("candidate", PathLAN, "10.0.0.3", 4242))
	must(t, s.PathOK("verified", PathLAN, "10.0.0.2", 4242))
	must(t, s.SeePath("moved", PathLAN, "10.77.1.3", 4242))
	must(t, s.PruneStale())
	got := ids(t, s)
	if got["candidate"] || got["gossiped"] {
		t.Fatalf("never-verified lan/gossip nodes survived 2 min: %v", got)
	}
	if !got["inet-unverified"] || !got["verified"] || !got["moved"] || !got["me"] {
		t.Fatalf("pruned too early: %v", got)
	}

	// 16 min: "moved" was sighted every poll but never verified again.
	setClock(t, t0.Add(16*time.Minute))
	must(t, s.PathOK("verified", PathLAN, "10.0.0.2", 4242))
	must(t, s.SeePath("moved", PathLAN, "10.77.1.3", 4242))
	must(t, s.PruneStale())
	got = ids(t, s)
	if got["moved"] || got["inet-unverified"] {
		t.Fatalf("nodes without a verified path for 15 min survived: %v", got)
	}
	if !got["verified"] || !got["me"] {
		t.Fatalf("live nodes pruned: %v", got)
	}

	// Path TTLs: a lan path unseen 10 min goes, an internet path unseen less
	// than an hour stays; self is never pruned however old.
	must(t, s.SeePath("verified", PathLAN, "10.0.0.99", 4242))
	must(t, s.SeePath("verified", PathInternet, "127.0.0.1", 40010))
	setClock(t, t0.Add(27*time.Minute))
	must(t, s.PathOK("verified", PathLAN, "10.0.0.2", 4242))
	must(t, s.PruneStale())
	ps, _ := s.Paths("verified")
	if len(ps) != 2 || ps[0].Host != "10.0.0.2" || ps[1].Kind != PathInternet {
		t.Fatalf("after 11 min want the verified lan + internet paths, got %+v", ps)
	}
	setClock(t, t0.Add(80*time.Minute))
	must(t, s.PathOK("verified", PathLAN, "10.0.0.2", 4242))
	must(t, s.PruneStale())
	if ps, _ := s.Paths("verified"); len(ps) != 1 {
		t.Fatalf("internet path unseen for an hour survived: %+v", ps)
	}
	if ps, _ := s.Paths("me"); len(ps) != 1 || !ids(t, s)["me"] {
		t.Fatalf("self pruned: %+v", ps)
	}
}

func TestMarkUnreachableClearsLiveCounters(t *testing.T) {
	s := newTestStore(t)
	must(t, s.UpsertNode(Node{NodeID: "n", State: "healthy", ActiveJobs: 3, AvailableMem: 1 << 30}))
	must(t, s.MarkUnreachable("n"))
	n, _ := nodeByID(t, s, "n")
	if n.State != "unreachable" || n.ActiveJobs != 0 || n.AvailableMem != 0 {
		t.Fatalf("unreachable node should report no capacity: %+v", n)
	}
}

func TestDeleteNodeTakesItsPaths(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("gone", PathLAN, "10.0.0.5", 4242))
	must(t, s.DeleteNode("gone"))
	if len(ids(t, s)) != 0 {
		t.Fatal("node survived delete")
	}
	var n int
	must(t, s.db.QueryRow(`SELECT count(*) FROM paths`).Scan(&n))
	if n != 0 {
		t.Fatalf("%d paths survived their node (foreign keys off?)", n)
	}
}

func TestDropPathFallsBackToBestRemaining(t *testing.T) {
	s := newTestStore(t)
	must(t, s.PathOK("n", PathInternet, "127.0.0.1", 40001))
	must(t, s.PathOK("n", PathLAN, "10.0.0.5", 4242))
	must(t, s.DropPath("n", "10.0.0.5", 4242))
	if n, _ := nodeByID(t, s, "n"); n.Host != "127.0.0.1" || n.Source != "internet" {
		t.Fatalf("after dropping the active path: %+v", n)
	}
}

// Short ID prefixes resolve like the ones `pipedpeer nodes` prints.
func TestResolveNodeIDPrefixes(t *testing.T) {
	s := newTestStore(t)
	must(t, s.UpsertNode(Node{NodeID: "2b74d935b039"}))
	must(t, s.UpsertNode(Node{NodeID: "2b7fffff0000"}))
	if _, err := s.ResolveNodeID("2b7"); err == nil {
		t.Fatal("ambiguous prefix must error, not pick one")
	}
	if id, err := s.ResolveNodeID("2b74d935"); err != nil || id != "2b74d935b039" {
		t.Fatalf("prefix resolve: id=%q err=%v", id, err)
	}
	if id, _ := s.ResolveNodeID("ffffffff"); id != "" {
		t.Fatalf("unknown prefix resolved to %q", id)
	}
}

// A database from an older build is a hard cut: dropped and recreated, not
// half-migrated.
func TestOldSchemaIsRecreated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", p)
	must(t, err)
	_, err = db.Exec(`
		CREATE TABLE nodes (node_id TEXT PRIMARY KEY, host TEXT NOT NULL, port INTEGER NOT NULL,
			source TEXT NOT NULL, is_manual INTEGER DEFAULT 0, last_seen INTEGER DEFAULT 0);
		INSERT INTO nodes (node_id, host, port, source, is_manual) VALUES ('legacy', '10.0.0.9', 38080, 'manual', 1);`)
	must(t, err)
	db.Close()
	must(t, os.Chmod(p, 0o644))

	s, err := NewAt(p)
	if err != nil {
		t.Fatalf("an old schema must be recreated, not fail: %v", err)
	}
	defer s.Close()
	if got := ids(t, s); len(got) != 0 {
		t.Fatalf("old rows survived the hard cut: %v", got)
	}
	must(t, s.PathOK("fresh", PathLAN, "10.0.0.1", 4242))
	if n, ok := nodeByID(t, s, "fresh"); !ok || n.Host != "10.0.0.1" {
		t.Fatalf("recreated schema unusable: %+v", n)
	}
	fi, err := os.Stat(p)
	must(t, err)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("nodes.db mode %v, want 0600", fi.Mode().Perm())
	}
}

// The daemon reopens this database every start; rows survive a reopen.
func TestReopenKeepsRows(t *testing.T) {
	p := filepath.Join(t.TempDir(), "repeat.db")
	for i := 0; i < 3; i++ {
		s, err := NewAt(p)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		if i == 0 {
			must(t, s.PathOK("n", PathLAN, "10.0.0.1", 4242))
		}
		if !ids(t, s)["n"] {
			t.Fatalf("open %d lost the row", i+1)
		}
		s.Close()
	}
	fi, err := os.Stat(p)
	must(t, err)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("nodes.db mode %v, want 0600", fi.Mode().Perm())
	}
}
