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
	Source       string // discovery, manual, registry
	HealthScore  float64
	IsManual     bool
	LastSeen     int64
}

type Store struct {
	db *sql.DB
}

func path() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "nodes.db")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "pipedpeer", "nodes.db")
}

func New() (*Store, error) {
	p := path()
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
	_, err := s.db.Exec(`
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
	`)
	return err
}

func (s *Store) UpsertNode(n Node) error {
	n.LastSeen = time.Now().Unix()
	_, err := s.db.Exec(`
		INSERT INTO nodes (node_id, host, port, ssh_endpoint, arch, hostname, state,
			active_jobs, available_mem, reserved_mem, total_mem, cpu_percent,
			health_score, source, is_manual, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(node_id) DO UPDATE SET
			host=excluded.host, port=excluded.port, ssh_endpoint=excluded.ssh_endpoint,
			arch=excluded.arch, hostname=excluded.hostname, state=excluded.state,
			active_jobs=excluded.active_jobs, available_mem=excluded.available_mem,
			reserved_mem=excluded.reserved_mem, total_mem=excluded.total_mem,
			cpu_percent=excluded.cpu_percent, health_score=excluded.health_score,
			source=CASE WHEN nodes.is_manual THEN nodes.source ELSE excluded.source END,
			is_manual=MAX(nodes.is_manual, excluded.is_manual),
			last_seen=excluded.last_seen
	`, n.NodeID, n.Host, n.Port, n.SSHEndpoint, n.Arch, n.Hostname, n.State,
		n.ActiveJobs, n.AvailableMem, n.ReservedMem, n.TotalMem, n.CPUPercent,
		n.HealthScore, n.Source, boolToInt(n.IsManual), n.LastSeen)
	return err
}

func (s *Store) AddManual(host string, port int) error {
	// Try to resolve real UUID immediately via health endpoint
	healthURL := fmt.Sprintf("http://%s:%d/health", host, port)
	resp, err := http.Get(healthURL)
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
		NodeID:   nodeID,
		Host:     host,
		Port:     port,
		SSHEndpoint: fmt.Sprintf("root@%s:22", host),
		Source:   "manual",
		IsManual: true,
		State:    state,
	})
}

func (s *Store) RemoveManual(host string) error {
	_, err := s.db.Exec(`DELETE FROM nodes WHERE is_manual = 1 AND host = ?`, host)
	return err
}

func (s *Store) RemoveAll() error {
	_, err := s.db.Exec(`DELETE FROM nodes`)
	return err
}

func (s *Store) ListManual() ([]Node, error) {
	return s.query(`SELECT * FROM nodes WHERE is_manual = 1 ORDER BY port`)
}

func (s *Store) ListHealthy() ([]Node, error) {
	cutoff := time.Now().Unix() - 30
	return s.query(`SELECT * FROM nodes WHERE state = 'healthy' AND last_seen > ? ORDER BY node_id`, cutoff)
}

func (s *Store) ListAll() ([]Node, error) {
	return s.query(`SELECT * FROM nodes ORDER BY node_id`)
}

func (s *Store) PruneStale(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge).Unix()
	// Don't prune manual entries
	s.db.Exec(`DELETE FROM nodes WHERE is_manual = 0 AND last_seen < ?`, cutoff)
}

func (s *Store) UpdateHealth(nodeID string, activeJobs int, availableMem, reservedMem, totalMem int64, cpuPercent float64) {
	s.db.Exec(`
		UPDATE nodes SET state='healthy', active_jobs=?, available_mem=?,
			reserved_mem=?, total_mem=?, cpu_percent=?, last_seen=?
		WHERE node_id=?`, activeJobs, availableMem, reservedMem, totalMem, cpuPercent, time.Now().Unix(), nodeID)
}

func (s *Store) MarkUnreachable(nodeID string) {
	s.db.Exec(`UPDATE nodes SET state='unreachable', last_seen=? WHERE node_id=?`, time.Now().Unix(), nodeID)
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
			&n.CPUPercent, &n.HealthScore, &n.Source, &isManual, &n.LastSeen); err != nil {
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
