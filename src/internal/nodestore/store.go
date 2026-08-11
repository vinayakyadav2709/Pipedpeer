// Package nodestore is the daemon's record of every node it knows about:
// itself, peers found on the LAN via mDNS, and peers added by hand.
//
// The daemon is the single source of truth for cluster membership — the CLI,
// the dashboard and the orchestrator all read it through the daemon's
// /v1/nodes endpoint rather than keeping their own copies. Membership is
// persisted to SQLite so a daemon restart does not lose manually added peers.
package nodestore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Node struct {
	NodeID       string
	Host         string
	Port         int
	SSHEndpoint  string
	Arch         string
	Hostname     string
	State        string
	ActiveJobs   int
	AvailableMem int64
	ReservedMem  int64
	TotalMem     int64
	CPUPercent   float64
	Source       string // discovery, manual, registry, self
	HealthScore  float64
	IsManual     bool
	LastSeen     int64

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

// insertColumns is the column list for writes. Never use SELECT * anywhere —
// scan order is positional, so a later ALTER TABLE would silently shift every
// field.
const insertColumns = `node_id, host, port, ssh_endpoint, arch, hostname, state,
	active_jobs, available_mem, reserved_mem, total_mem, cpu_percent,
	health_score, source, is_manual, last_seen, caps_json, load_json`

// selectColumns mirrors insertColumns but coalesces the nullable text columns.
// Rows written by earlier versions (and by ALTER TABLE ADD COLUMN, which
// backfills NULL when no default applies) hold NULLs that will not scan into a
// Go string.
const selectColumns = `node_id, host, port,
	COALESCE(ssh_endpoint, ''), COALESCE(arch, ''), COALESCE(hostname, ''),
	COALESCE(state, 'unknown'),
	COALESCE(active_jobs, 0), COALESCE(available_mem, 0), COALESCE(reserved_mem, 0),
	COALESCE(total_mem, 0), COALESCE(cpu_percent, 0), COALESCE(health_score, 0),
	COALESCE(source, ''), COALESCE(is_manual, 0), COALESCE(last_seen, 0),
	COALESCE(caps_json, ''), COALESCE(load_json, '')`

func path() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "nodes.db")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "pipedpeer", "nodes.db")
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
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", p+"?_journal_mode=WAL&_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS nodes (
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
		CREATE INDEX IF NOT EXISTS idx_nodes_state ON nodes(state);
		CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
	`); err != nil {
		return err
	}

	// Additive migrations for databases created by earlier versions. SQLite has
	// no ADD COLUMN IF NOT EXISTS, and a duplicate-column error just means the
	// migration already ran.
	for _, col := range []string{
		`ALTER TABLE nodes ADD COLUMN caps_json TEXT DEFAULT ''`,
		`ALTER TABLE nodes ADD COLUMN load_json TEXT DEFAULT ''`,
	} {
		if _, err := s.db.Exec(col); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// UpsertNode inserts or refreshes a node.
//
// Empty CapsJSON/LoadJSON leave the stored values alone: discovery only learns
// a node's address, and it must not wipe the richer hardware detail that the
// health poller wrote.
func (s *Store) UpsertNode(n Node) error {
	if n.LastSeen == 0 {
		n.LastSeen = time.Now().Unix()
	}
	_, err := s.db.Exec(`
		INSERT INTO nodes (`+insertColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(node_id) DO UPDATE SET
			host=excluded.host, port=excluded.port,
			ssh_endpoint=CASE WHEN excluded.ssh_endpoint='' THEN nodes.ssh_endpoint ELSE excluded.ssh_endpoint END,
			arch=CASE WHEN excluded.arch='' THEN nodes.arch ELSE excluded.arch END,
			hostname=CASE WHEN excluded.hostname='' THEN nodes.hostname ELSE excluded.hostname END,
			state=excluded.state,
			active_jobs=excluded.active_jobs, available_mem=excluded.available_mem,
			reserved_mem=excluded.reserved_mem, total_mem=excluded.total_mem,
			cpu_percent=excluded.cpu_percent, health_score=excluded.health_score,
			source=CASE WHEN nodes.is_manual THEN nodes.source ELSE excluded.source END,
			is_manual=MAX(nodes.is_manual, excluded.is_manual),
			last_seen=excluded.last_seen,
			caps_json=CASE WHEN excluded.caps_json='' THEN nodes.caps_json ELSE excluded.caps_json END,
			load_json=CASE WHEN excluded.load_json='' THEN nodes.load_json ELSE excluded.load_json END
	`, n.NodeID, n.Host, n.Port, n.SSHEndpoint, n.Arch, n.Hostname, n.State,
		n.ActiveJobs, n.AvailableMem, n.ReservedMem, n.TotalMem, n.CPUPercent,
		n.HealthScore, n.Source, boolToInt(n.IsManual), n.LastSeen,
		n.CapsJSON, n.LoadJSON)
	return err
}

func (s *Store) AddManual(host string, port int) error {
	// Try to resolve the real node UUID immediately via the health endpoint, so
	// the entry merges with the same node when mDNS finds it later.
	healthURL := fmt.Sprintf("http://%s:%d/health", host, port)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(healthURL)
	nodeID := fmt.Sprintf("manual-%s:%d", host, port)
	state := "unknown"
	if err == nil {
		defer resp.Body.Close()
		var h struct {
			NodeID string `json:"node_id"`
		}
		if json.NewDecoder(resp.Body).Decode(&h) == nil && h.NodeID != "" {
			nodeID = h.NodeID
			state = "healthy"
		}
	}
	return s.UpsertNode(Node{
		NodeID:      nodeID,
		Host:        host,
		Port:        port,
		SSHEndpoint: fmt.Sprintf("root@%s:22", host),
		Source:      "manual",
		IsManual:    true,
		State:       state,
	})
}

func (s *Store) RemoveManual(host string) error {
	_, err := s.db.Exec(`DELETE FROM nodes WHERE is_manual = 1 AND host = ?`, host)
	return err
}

// DeleteNode removes a single node by ID, regardless of source.
func (s *Store) DeleteNode(nodeID string) error {
	_, err := s.db.Exec(`DELETE FROM nodes WHERE node_id = ?`, nodeID)
	return err
}

func (s *Store) RemoveAll() error {
	_, err := s.db.Exec(`DELETE FROM nodes`)
	return err
}

func (s *Store) ListManual() ([]Node, error) {
	return s.query(`SELECT ` + selectColumns + ` FROM nodes WHERE is_manual = 1 ORDER BY port`)
}

// ListHealthy returns nodes that reported healthy recently. The last_seen
// cutoff is the death filter: a node whose daemon died stops being a placement
// candidate even if nothing got round to marking it unreachable.
func (s *Store) ListHealthy(maxAge time.Duration) ([]Node, error) {
	cutoff := time.Now().Add(-maxAge).Unix()
	return s.query(`SELECT `+selectColumns+` FROM nodes WHERE state = 'healthy' AND last_seen > ? ORDER BY node_id`, cutoff)
}

func (s *Store) ListAll() ([]Node, error) {
	return s.query(`SELECT ` + selectColumns + ` FROM nodes ORDER BY node_id`)
}

// PruneStale drops auto-discovered nodes not seen for maxAge. Manual entries
// are kept: the user added them deliberately and a node being down is not a
// reason to forget it.
func (s *Store) PruneStale(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge).Unix()
	s.db.Exec(`DELETE FROM nodes WHERE is_manual = 0 AND last_seen < ?`, cutoff)
}

// MarkHealthy records a successful health poll, refreshing both the indexed
// scalars and the full capability/load payload.
func (s *Store) MarkHealthy(n Node) error {
	n.State = "healthy"
	n.LastSeen = time.Now().Unix()
	if n.HealthScore == 0 {
		n.HealthScore = 1.0
	}
	return s.UpsertNode(n)
}

func (s *Store) MarkUnreachable(nodeID string) {
	s.db.Exec(`UPDATE nodes SET state='unreachable', active_jobs=0, available_mem=0 WHERE node_id=?`, nodeID)
}

func (s *Store) query(q string, args ...any) ([]Node, error) {
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var n Node
		var isManual int
		if err := rows.Scan(&n.NodeID, &n.Host, &n.Port, &n.SSHEndpoint, &n.Arch, &n.Hostname,
			&n.State, &n.ActiveJobs, &n.AvailableMem, &n.ReservedMem, &n.TotalMem,
			&n.CPUPercent, &n.HealthScore, &n.Source, &isManual, &n.LastSeen,
			&n.CapsJSON, &n.LoadJSON); err != nil {
			return nil, err
		}
		n.IsManual = isManual == 1
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func SourceDisplay(node Node) string {
	sources := strings.Split(node.Source, ",")
	parts := make([]string, 0, len(sources))
	for _, s := range sources {
		s = strings.TrimSpace(s)
		switch s {
		case "discovery":
			parts = append(parts, "mDNS")
		case "manual":
			parts = append(parts, "manual")
		case "registry":
			parts = append(parts, "registry")
		case "self":
			parts = append(parts, "self")
		}
	}
	return strings.Join(parts, "+")
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
