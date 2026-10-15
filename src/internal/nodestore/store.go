// Package nodestore is the daemon's address book: every node it knows about,
// keyed by node id, and every address (path) it has for each one.
//
// A node can be reachable several ways at once: on the LAN, through an
// internet link's local forwarder, at an address another member gossiped.
// One address per node was the root of the demo's flapping rows (a LAN
// sighting overwrote a working internet link, a moved peer kept its old
// address forever). So a node holds a set of paths, and exactly one rule
// moves the active one: a candidate (SeePath) is only added to the set; a
// path becomes active only once it has been verified (PathOK) and beats the
// incumbent. An incumbent that still answers is kept.
//
// The daemon is the single source of truth for cluster membership: the CLI,
// the dashboard and the orchestrator all read it through the daemon's
// /v1/nodes endpoint rather than keeping their own copies.
package nodestore

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"

	_ "modernc.org/sqlite"
)

// PathKind is how a path was learned.
type PathKind string

const (
	PathLAN      PathKind = "lan"      // LAN discovery (UDP source address)
	PathInternet PathKind = "internet" // an internet link's local forwarder
	PathGossip   PathKind = "gossip"   // another member's signed address record
	PathSelf     PathKind = "self"     // this daemon's own address
)

// rank orders kinds for "beats the incumbent": verified lan > verified
// internet > verified gossip. self only ever competes with self.
func (k PathKind) rank() int {
	switch k {
	case PathSelf:
		return 4
	case PathLAN:
		return 3
	case PathInternet:
		return 2
	case PathGossip:
		return 1
	}
	return 0
}

// rankSQL is PathKind.rank as SQL over a column.
func rankSQL(col string) string {
	return "CASE " + col + " WHEN 'self' THEN 4 WHEN 'lan' THEN 3 WHEN 'internet' THEN 2 WHEN 'gossip' THEN 1 ELSE 0 END"
}

// Path is one address of a node. LastSeen is the last sighting or
// verification, LastOK the last verification; LastErr is the last failure's
// reason and is cleared by the next verification, so LastErr == "" with a
// non-zero LastOK means "verified and still answering".
type Path struct {
	Kind     PathKind
	Host     string
	Port     int
	LastSeen time.Time
	LastOK   time.Time
	LastErr  string
	Active   bool
}

func (p Path) answering() bool { return !p.LastOK.IsZero() && p.LastErr == "" }

type Node struct {
	NodeID string
	// Host, Port and Source are the active path and its kind. A node with no
	// verified path yet shows its best candidate instead, so there is always
	// an address to show and to try; State says whether it answered.
	Host string
	Port int
	// Source is the active path's kind (lan, internet, gossip, self).
	Source string
	// Reason is why the path in Host/Port last failed; empty while it answers.
	Reason       string
	SSHEndpoint  string
	Arch         string
	Hostname     string
	State        string
	ActiveJobs   int
	AvailableMem int64
	ReservedMem  int64
	TotalMem     int64
	CPUPercent   float64
	HealthScore  float64
	LastSeen     int64 // last health answer (UpsertNode/MarkHealthy), unix seconds

	// CapsJSON and LoadJSON hold the node's full registry.NodeRecord
	// Capabilities and Load, verbatim. They are opaque here on purpose: the
	// scheduler keeps adding hardware dimensions (GPU VRAM, compute
	// capability, core clock) and this store should not need a migration for
	// each one. The scalar columns above are the subset worth indexing and
	// showing in a table.
	CapsJSON string
	LoadJSON string
}

type Store struct {
	db *sql.DB
}

// Prune rules (NODES-PRUNE). A sighting alone never refreshes a node: only a
// verification does, so a peer that moved stops being kept alive by the
// probe that still finds its id at the old address (experiment E4).
const (
	nodeVerifiedTTL  = 15 * time.Minute // no verified path for this long
	candidateNodeTTL = 2 * time.Minute  // never-verified lan/gossip node
	lanPathTTL       = 10 * time.Minute // lan path unseen
	remotePathTTL    = time.Hour        // internet/gossip path unseen
)

// schemaVersion is PRAGMA user_version. Any other version is a hard cut: the
// tables are dropped and recreated (peers are re-found within a poll or two).
const schemaVersion = 2

// now is the clock; tests move it.
var now = time.Now

// nodeColumns is the column list for reads. Never SELECT *: scan order is
// positional, so a later ALTER TABLE would silently shift every field.
const nodeColumns = `node_id, ssh_endpoint, arch, hostname, state,
	active_jobs, available_mem, reserved_mem, total_mem, cpu_percent,
	health_score, last_seen, caps_json, load_json`

const pathColumns = `node_id, kind, host, port, last_seen, last_ok, last_err, active`

func path() string {
	return filepath.Join(userdir.Data(), "nodes.db")
}

func New() (*Store, error) {
	return open(path())
}

// NewAt opens a store at an explicit path. Used by tests so they do not share
// the user's real node database.
func NewAt(p string) (*Store, error) {
	return open(p)
}

func open(p string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return nil, err
	}
	// The file holds every peer's addresses: owner only. SQLite gives the
	// -wal and -shm files the main file's mode; older builds left 0644, so
	// tighten whatever exists.
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", p, err)
	}
	f.Close()
	for _, name := range []string{p, p + "-wal", p + "-shm"} {
		if err := os.Chmod(name, 0o600); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("chmod %s: %w", name, err)
		}
	}
	db, err := sql.Open("sqlite", p+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(p); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(p string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer tx.Rollback()

	var v int
	if err := tx.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return fmt.Errorf("migrate: read schema version: %w", err)
	}
	if v == schemaVersion {
		return nil
	}
	rows, err := tx.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return fmt.Errorf("migrate: list tables: %w", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("migrate: list tables: %w", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	if len(tables) > 0 {
		log.Printf("nodestore: %s has schema version %d, this build uses %d; starting an empty node list (peers are found again within a minute)", p, v, schemaVersion)
		if _, err := tx.Exec(`PRAGMA defer_foreign_keys = ON`); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		for _, t := range tables {
			if _, err := tx.Exec(`DROP TABLE IF EXISTS "` + t + `"`); err != nil {
				return fmt.Errorf("migrate: drop %s: %w", t, err)
			}
		}
	}
	if _, err := tx.Exec(`
		CREATE TABLE nodes (
			node_id       TEXT PRIMARY KEY,
			ssh_endpoint  TEXT NOT NULL DEFAULT '',
			arch          TEXT NOT NULL DEFAULT '',
			hostname      TEXT NOT NULL DEFAULT '',
			state         TEXT NOT NULL DEFAULT 'unknown',
			active_jobs   INTEGER NOT NULL DEFAULT 0,
			available_mem INTEGER NOT NULL DEFAULT 0,
			reserved_mem  INTEGER NOT NULL DEFAULT 0,
			total_mem     INTEGER NOT NULL DEFAULT 0,
			cpu_percent   REAL NOT NULL DEFAULT 0,
			health_score  REAL NOT NULL DEFAULT 0,
			last_seen     INTEGER NOT NULL DEFAULT 0,
			created       INTEGER NOT NULL DEFAULT 0,
			verified_at   INTEGER NOT NULL DEFAULT 0,
			caps_json     TEXT NOT NULL DEFAULT '',
			load_json     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE paths (
			node_id   TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
			host      TEXT NOT NULL,
			port      INTEGER NOT NULL,
			kind      TEXT NOT NULL,
			active    INTEGER NOT NULL DEFAULT 0,
			last_seen INTEGER NOT NULL DEFAULT 0,
			last_ok   INTEGER NOT NULL DEFAULT 0,
			last_err  TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (node_id, host, port)
		);
		CREATE INDEX idx_paths_addr ON paths(host, port);
	`); err != nil {
		return fmt.Errorf("migrate: create: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return tx.Commit()
}

// UpsertNode inserts or refreshes a node's details. It never touches paths:
// addresses move only through SeePath/PathOK/PathFailed/DropPath, so
// Host/Port/Source/Reason on n are ignored.
//
// Empty fields leave the stored values alone: a caller that knows only some
// of a node's details must not wipe the richer hardware detail that the
// health poller wrote.
func (s *Store) UpsertNode(n Node) error {
	if n.NodeID == "" {
		return errors.New("nodestore: upsert without a node id")
	}
	if n.LastSeen == 0 {
		n.LastSeen = now().Unix()
	}
	if n.State == "" {
		n.State = "unknown"
	}
	_, err := s.db.Exec(`
		INSERT INTO nodes (node_id, ssh_endpoint, arch, hostname, state,
			active_jobs, available_mem, reserved_mem, total_mem, cpu_percent,
			health_score, last_seen, caps_json, load_json, created)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(node_id) DO UPDATE SET
			ssh_endpoint=CASE WHEN excluded.ssh_endpoint='' THEN nodes.ssh_endpoint ELSE excluded.ssh_endpoint END,
			arch=CASE WHEN excluded.arch='' THEN nodes.arch ELSE excluded.arch END,
			hostname=CASE WHEN excluded.hostname='' THEN nodes.hostname ELSE excluded.hostname END,
			state=excluded.state,
			active_jobs=excluded.active_jobs, available_mem=excluded.available_mem,
			reserved_mem=excluded.reserved_mem, total_mem=excluded.total_mem,
			cpu_percent=excluded.cpu_percent, health_score=excluded.health_score,
			last_seen=excluded.last_seen,
			caps_json=CASE WHEN excluded.caps_json='' THEN nodes.caps_json ELSE excluded.caps_json END,
			load_json=CASE WHEN excluded.load_json='' THEN nodes.load_json ELSE excluded.load_json END
	`, n.NodeID, n.SSHEndpoint, n.Arch, n.Hostname, n.State,
		n.ActiveJobs, n.AvailableMem, n.ReservedMem, n.TotalMem, n.CPUPercent,
		n.HealthScore, n.LastSeen, n.CapsJSON, n.LoadJSON, now().Unix())
	return err
}

func checkPath(nodeID string, kind PathKind, host string, port int) error {
	if nodeID == "" || host == "" || port <= 0 || port > 65535 {
		return fmt.Errorf("nodestore: bad path %q %s:%d", nodeID, host, port)
	}
	if kind.rank() == 0 {
		return fmt.Errorf("nodestore: unknown path kind %q for %s %s:%d", kind, nodeID, host, port)
	}
	return nil
}

// seePath adds or refreshes a path inside tx. A kind never downgrades: a
// gossip sighting of an address already known as lan stays lan.
func seePath(tx *sql.Tx, nodeID string, kind PathKind, host string, port int, ts int64) error {
	if _, err := tx.Exec(`INSERT INTO nodes (node_id, created) VALUES (?, ?) ON CONFLICT(node_id) DO NOTHING`,
		nodeID, ts); err != nil {
		return err
	}
	_, err := tx.Exec(`
		INSERT INTO paths (node_id, host, port, kind, last_seen) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(node_id, host, port) DO UPDATE SET
			last_seen=excluded.last_seen,
			kind=CASE WHEN `+rankSQL("excluded.kind")+` > `+rankSQL("paths.kind")+` THEN excluded.kind ELSE paths.kind END
	`, nodeID, host, port, string(kind), ts)
	return err
}

// SeePath records a candidate address for a node, creating the node if it is
// new. It never changes the active path.
func (s *Store) SeePath(nodeID string, kind PathKind, host string, port int) error {
	if err := checkPath(nodeID, kind, host, port); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := seePath(tx, nodeID, kind, host, port, now().Unix()); err != nil {
		return fmt.Errorf("nodestore: see %s %s:%d: %w", nodeID, host, port, err)
	}
	return tx.Commit()
}

// PathOK records that nodeID answered (authenticated) at host:port. The path
// becomes active when there is no incumbent, the incumbent's last attempt
// failed, or it outranks the incumbent (lan > internet > gossip). A self path
// always becomes active and replaces the node's other self paths: this daemon
// has one own address at a time.
//
// A verified answer also means no other node is at that address any more (a
// recreated container, a forwarder port reused by another peer), so the same
// host:port is dropped from every other node.
func (s *Store) PathOK(nodeID string, kind PathKind, host string, port int) error {
	if err := checkPath(nodeID, kind, host, port); err != nil {
		return err
	}
	ts := now().Unix()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	wrap := func(err error) error { return fmt.Errorf("nodestore: path ok %s %s:%d: %w", nodeID, host, port, err) }

	if err := seePath(tx, nodeID, kind, host, port, ts); err != nil {
		return wrap(err)
	}
	if _, err := tx.Exec(`UPDATE paths SET last_ok=?, last_err='' WHERE node_id=? AND host=? AND port=?`,
		ts, nodeID, host, port); err != nil {
		return wrap(err)
	}
	if _, err := tx.Exec(`UPDATE nodes SET verified_at=? WHERE node_id=?`, ts, nodeID); err != nil {
		return wrap(err)
	}
	res, err := tx.Exec(`DELETE FROM paths WHERE host=? AND port=? AND node_id != ?`, host, port, nodeID)
	if err != nil {
		return wrap(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("nodestore: %s now answers at %s:%d; dropped that address from %d other node(s)", nodeID, host, port, n)
	}

	var stored string
	if err := tx.QueryRow(`SELECT kind FROM paths WHERE node_id=? AND host=? AND port=?`,
		nodeID, host, port).Scan(&stored); err != nil {
		return wrap(err)
	}
	promote := true
	if PathKind(stored) == PathSelf {
		if _, err := tx.Exec(`DELETE FROM paths WHERE node_id=? AND kind='self' AND NOT (host=? AND port=?)`,
			nodeID, host, port); err != nil {
			return wrap(err)
		}
	} else {
		var incKind, incErr string
		var incOK int64
		err := tx.QueryRow(`SELECT kind, last_ok, last_err FROM paths
			WHERE node_id=? AND active=1 AND NOT (host=? AND port=?)`, nodeID, host, port).Scan(&incKind, &incOK, &incErr)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return wrap(err)
		default:
			incumbentAnswers := incOK > 0 && incErr == ""
			promote = !incumbentAnswers || PathKind(stored).rank() > PathKind(incKind).rank()
		}
	}
	if promote {
		if _, err := tx.Exec(`UPDATE paths SET active = (host=? AND port=?) WHERE node_id=?`,
			host, port, nodeID); err != nil {
			return wrap(err)
		}
	}
	return tx.Commit()
}

// PathFailed records a failed attempt on a path. The path stays where it is
// (an active path stays active until a verified challenger replaces it) and
// its reason becomes the node's Reason while it is shown. Not a sighting: a
// failing path ages out like an unseen one. A path that is already gone is a
// no-op.
func (s *Store) PathFailed(nodeID, host string, port int, reason string) error {
	if reason == "" {
		reason = "failed"
	}
	_, err := s.db.Exec(`UPDATE paths SET last_err=? WHERE node_id=? AND host=? AND port=?`,
		reason, nodeID, host, port)
	if err != nil {
		return fmt.Errorf("nodestore: path failed %s %s:%d: %w", nodeID, host, port, err)
	}
	return nil
}

// DropPath forgets one address of a node. When it was the active path, the
// node shows its best remaining path until the next verification picks one.
func (s *Store) DropPath(nodeID, host string, port int) error {
	_, err := s.db.Exec(`DELETE FROM paths WHERE node_id=? AND host=? AND port=?`, nodeID, host, port)
	if err != nil {
		return fmt.Errorf("nodestore: drop %s %s:%d: %w", nodeID, host, port, err)
	}
	return nil
}

// Paths returns a node's addresses, best first: the active path, then paths
// that are verified and answering, then never-tried candidates, then failing
// ones; within each group by kind (lan > internet > gossip), then most
// recently verified or seen.
func (s *Store) Paths(nodeID string) ([]Path, error) {
	byNode, err := s.paths(`WHERE node_id = ?`, nodeID)
	if err != nil {
		return nil, err
	}
	return byNode[nodeID], nil
}

func (s *Store) paths(where string, args ...any) (map[string][]Path, error) {
	rows, err := s.db.Query(`SELECT `+pathColumns+` FROM paths `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]Path{}
	for rows.Next() {
		var id, kind string
		var p Path
		var seen, ok int64
		var active int
		if err := rows.Scan(&id, &kind, &p.Host, &p.Port, &seen, &ok, &p.LastErr, &active); err != nil {
			return nil, err
		}
		p.Kind = PathKind(kind)
		p.LastSeen = unixTime(seen)
		p.LastOK = unixTime(ok)
		p.Active = active == 1
		out[id] = append(out[id], p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, ps := range out {
		sortPaths(ps)
	}
	return out, nil
}

func sortPaths(ps []Path) {
	group := func(p Path) int {
		switch {
		case p.Active:
			return 0
		case p.answering():
			return 1
		case p.LastErr == "":
			return 2
		}
		return 3
	}
	latest := func(p Path) time.Time {
		if p.LastOK.After(p.LastSeen) {
			return p.LastOK
		}
		return p.LastSeen
	}
	sort.SliceStable(ps, func(i, j int) bool {
		a, b := ps[i], ps[j]
		if ga, gb := group(a), group(b); ga != gb {
			return ga < gb
		}
		if ra, rb := a.Kind.rank(), b.Kind.rank(); ra != rb {
			return ra > rb
		}
		return latest(a).After(latest(b))
	})
}

func unixTime(s int64) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(s, 0)
}

// DeleteNode removes a single node and its paths.
func (s *Store) DeleteNode(nodeID string) error {
	_, err := s.db.Exec(`DELETE FROM nodes WHERE node_id = ?`, nodeID)
	return err
}

// ResolveNodeID expands a node ID or unique prefix (the short form printed by
// `pipedpeer nodes`) to the full ID. Returns "" when nothing matches and an
// error when the prefix is ambiguous.
func (s *Store) ResolveNodeID(idOrPrefix string) (string, error) {
	rows, err := s.db.Query(
		`SELECT node_id FROM nodes WHERE node_id = ? OR node_id LIKE ? LIMIT 2`,
		idOrPrefix, idOrPrefix+"%")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
		ids = append(ids, id)
	}
	switch len(ids) {
	case 0:
		return "", nil
	case 1:
		return ids[0], nil
	default:
		return "", fmt.Errorf("node id %q is ambiguous", idOrPrefix)
	}
}

func (s *Store) RemoveAll() error {
	_, err := s.db.Exec(`DELETE FROM nodes`)
	return err
}

// ListAll returns every node with Host/Port/Source/Reason taken from its
// active path (or best candidate), ordered by node id.
func (s *Store) ListAll() ([]Node, error) {
	byNode, err := s.paths(``)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT ` + nodeColumns + ` FROM nodes ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.NodeID, &n.SSHEndpoint, &n.Arch, &n.Hostname,
			&n.State, &n.ActiveJobs, &n.AvailableMem, &n.ReservedMem, &n.TotalMem,
			&n.CPUPercent, &n.HealthScore, &n.LastSeen, &n.CapsJSON, &n.LoadJSON); err != nil {
			return nil, err
		}
		if ps := byNode[n.NodeID]; len(ps) > 0 {
			n.Host, n.Port, n.Source, n.Reason = ps[0].Host, ps[0].Port, string(ps[0].Kind), ps[0].LastErr
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// PruneStale applies the prune rules: a node with no verified path for 15
// min goes, a never-verified node after 2 min (15 when it has an internet
// path); lan paths unseen for 10 min and internet/gossip paths unseen for an
// hour go. The node that has a self path, and self paths, are never pruned.
// Each removal is logged.
func (s *Store) PruneStale() error {
	t := now()
	verifiedCut := t.Add(-nodeVerifiedTTL).Unix()
	candidateCut := t.Add(-candidateNodeTTL).Unix()
	rows, err := s.db.Query(`
		DELETE FROM nodes
		WHERE NOT EXISTS (SELECT 1 FROM paths p WHERE p.node_id = nodes.node_id AND p.kind = 'self')
		AND (
			(verified_at > 0 AND verified_at < ?)
			OR (verified_at = 0 AND created < ?)
			OR (verified_at = 0 AND created < ? AND NOT EXISTS
				(SELECT 1 FROM paths p WHERE p.node_id = nodes.node_id AND p.kind = 'internet'))
		)
		RETURNING node_id, verified_at`, verifiedCut, verifiedCut, candidateCut)
	if err != nil {
		return fmt.Errorf("nodestore: prune nodes: %w", err)
	}
	for rows.Next() {
		var id string
		var verified int64
		if err := rows.Scan(&id, &verified); err != nil {
			rows.Close()
			return fmt.Errorf("nodestore: prune nodes: %w", err)
		}
		if verified > 0 {
			log.Printf("nodestore: forgot node %s: no verified path since %s", id, time.Unix(verified, 0).Format(time.TimeOnly))
		} else {
			log.Printf("nodestore: forgot node %s: seen but never verified", id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("nodestore: prune nodes: %w", err)
	}

	rows, err = s.db.Query(`
		DELETE FROM paths
		WHERE kind != 'self'
		AND MAX(last_seen, last_ok) < CASE kind WHEN 'lan' THEN ? ELSE ? END
		RETURNING node_id, kind, host, port`,
		t.Add(-lanPathTTL).Unix(), t.Add(-remotePathTTL).Unix())
	if err != nil {
		return fmt.Errorf("nodestore: prune paths: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, kind, host string
		var port int
		if err := rows.Scan(&id, &kind, &host, &port); err != nil {
			return fmt.Errorf("nodestore: prune paths: %w", err)
		}
		log.Printf("nodestore: forgot %s address %s:%d of %s: not seen recently", kind, host, port, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("nodestore: prune paths: %w", err)
	}
	return nil
}

// MarkHealthy records a successful health poll, refreshing both the indexed
// scalars and the full capability/load payload. It does not verify a path;
// the poller calls PathOK for the address that answered.
func (s *Store) MarkHealthy(n Node) error {
	n.State = "healthy"
	n.LastSeen = now().Unix()
	if n.HealthScore == 0 {
		n.HealthScore = 1.0
	}
	return s.UpsertNode(n)
}

func (s *Store) MarkUnreachable(nodeID string) error {
	_, err := s.db.Exec(`UPDATE nodes SET state='unreachable', active_jobs=0, available_mem=0 WHERE node_id=?`, nodeID)
	if err != nil {
		return fmt.Errorf("nodestore: mark %s unreachable: %w", nodeID, err)
	}
	return nil
}
