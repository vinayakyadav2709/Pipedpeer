package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

// NodeRecord represents a registered node in the cluster.
type NodeRecord struct {
	NodeID        string            `json:"node_id"`
	SSHEndpoint   string            `json:"ssh_endpoint"`
	DaemonPort    int               `json:"daemon_port"`
	Capabilities  map[string]string `json:"capabilities,omitempty"`
	Load          LoadInfo          `json:"load"`
	HealthScore   float64           `json:"health_score"`
	State         string            `json:"state"` // "healthy", "stale", "unavailable"
	RegisteredAt  time.Time         `json:"registered_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	LeaseExpiry   time.Time         `json:"lease_expiry"`
}

// PerGPUInfo carries live stats for a single GPU device on this node.
type PerGPUInfo struct {
	Index            int     `json:"index"`
	Name             string  `json:"name"`
	MemoryTotalBytes int64   `json:"memory_total_bytes"`
	MemoryFreeBytes  int64   `json:"memory_free_bytes"`
	UtilizationGPU   float64 `json:"utilization_gpu_percent"`
}

// LoadInfo carries current resource utilization.
type LoadInfo struct {
	CPUPercent        float64      `json:"cpu_percent"`
	MemPercent        float64      `json:"memory_percent"`
	ActiveJobs        int          `json:"active_jobs"`
	QueueDepth        int          `json:"queue_depth"`
	RecentFailures    int          `json:"recent_failures"`
	TotalMemBytes     int64        `json:"total_mem_bytes,omitempty"`
	AvailableMemBytes int64        `json:"available_mem_bytes,omitempty"`
	ReservedMemBytes  int64        `json:"reserved_mem_bytes,omitempty"`
	TotalCPUs         int          `json:"total_cpus"`
	GPUModel          string       `json:"gpu_model,omitempty"`
	GPUMemBytes       int64        `json:"gpu_mem_bytes,omitempty"`
	GPUMemUsedBytes   int64        `json:"gpu_mem_used_bytes,omitempty"`
	GPUUtilPercent    float64      `json:"gpu_util_percent,omitempty"`
	GPUs              []PerGPUInfo `json:"gpus,omitempty"`
}

// Config controls registry timing behavior.
type Config struct {
	LeaseDuration    time.Duration // how long a heartbeat keeps node alive (default 30s)
	StaleGrace       time.Duration // time after lease expiry before marking unavailable (default 60s)
	RemovalThreshold time.Duration // time after unavailable before removal (default 5m)
	SweepInterval    time.Duration // how often to sweep leases (default 5s)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		LeaseDuration:    30 * time.Second,
		StaleGrace:       60 * time.Second,
		RemovalThreshold: 5 * time.Minute,
		SweepInterval:    5 * time.Second,
	}
}

// Backend defines how node records are stored.
type Backend interface {
	Put(nodeID string, rec NodeRecord) error
	Get(nodeID string) (NodeRecord, bool)
	Delete(nodeID string)
	List() []NodeRecord
}

// memoryBackend is an in-memory node store (for tests / standalone / no NATS).
type memoryBackend struct {
	mu    sync.RWMutex
	nodes map[string]*NodeRecord
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{nodes: make(map[string]*NodeRecord)}
}

func (m *memoryBackend) Put(nodeID string, rec NodeRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = &rec
	return nil
}

func (m *memoryBackend) Get(nodeID string) (NodeRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return NodeRecord{}, false
	}
	return *n, true
}

func (m *memoryBackend) Delete(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

func (m *memoryBackend) List() []NodeRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]NodeRecord, 0, len(m.nodes))
	for _, n := range m.nodes {
		result = append(result, *n)
	}
	return result
}

// natsKVBackend stores node records in NATS JetStream KV.
type natsKVBackend struct {
	kv jetstream.KeyValue
}

func newNATSKVBackend(kv jetstream.KeyValue) *natsKVBackend {
	return &natsKVBackend{kv: kv}
}

func (n *natsKVBackend) Put(nodeID string, rec NodeRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = n.kv.Put(context.Background(), nodeID, data)
	return err
}

func (n *natsKVBackend) Get(nodeID string) (NodeRecord, bool) {
	entry, err := n.kv.Get(context.Background(), nodeID)
	if err != nil {
		return NodeRecord{}, false
	}
	var rec NodeRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return NodeRecord{}, false
	}
	return rec, true
}

func (n *natsKVBackend) Delete(nodeID string) {
	_ = n.kv.Delete(context.Background(), nodeID)
}

func (n *natsKVBackend) List() []NodeRecord {
	ctx := context.Background()
	keys, err := n.kv.ListKeys(ctx)
	if err != nil {
		return nil
	}
	var result []NodeRecord
	for key := range keys.Keys() {
		entry, err := n.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var rec NodeRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			continue
		}
		result = append(result, rec)
	}
	return result
}

// Registry is the central node directory service.
type Registry struct {
	backend Backend
	config  Config
	stopCh  chan struct{}
}

// New creates a new registry with in-memory backend.
func New(cfg Config) *Registry {
	return &Registry{
		backend: newMemoryBackend(),
		config:  cfg,
		stopCh:  make(chan struct{}),
	}
}

// NewWithNATSKV creates a registry backed by NATS JetStream KV.
func NewWithNATSKV(cfg Config, bus *natsbus.Bus) (*Registry, error) {
	ctx := context.Background()
	kv, err := bus.JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "PIPEDPEER_NODES",
		TTL:     cfg.LeaseDuration + cfg.StaleGrace + cfg.RemovalThreshold,
		History: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create NATS KV bucket: %w", err)
	}
	return &Registry{
		backend: newNATSKVBackend(kv),
		config:  cfg,
		stopCh:  make(chan struct{}),
	}, nil
}

// Start begins the background lease sweep goroutine.
func (r *Registry) Start() {
	go r.sweepLoop()
}

// Stop halts the lease sweep goroutine.
func (r *Registry) Stop() {
	close(r.stopCh)
}

// Register adds or updates a node in the directory.
func (r *Registry) Register(rec NodeRecord) NodeRecord {
	now := time.Now().UTC()
	rec.RegisteredAt = now
	rec.LastHeartbeat = now
	rec.LeaseExpiry = now.Add(r.config.LeaseDuration)
	rec.State = "healthy"
	rec.HealthScore = 1.0
	_ = r.backend.Put(rec.NodeID, rec)
	return rec
}

// Heartbeat renews a node's lease and updates its load info.
func (r *Registry) Heartbeat(nodeID string, load LoadInfo) (NodeRecord, bool) {
	node, ok := r.backend.Get(nodeID)
	if !ok {
		return NodeRecord{}, false
	}

	now := time.Now().UTC()
	node.LastHeartbeat = now
	node.LeaseExpiry = now.Add(r.config.LeaseDuration)
	node.Load = load
	node.State = "healthy"

	// Simple health score: penalize high load and recent failures
	score := 1.0
	score -= node.Load.CPUPercent / 200.0 // max -0.5
	score -= node.Load.MemPercent / 200.0 // max -0.5
	score -= float64(node.Load.RecentFailures) * 0.1
	if score < 0 {
		score = 0
	}
	node.HealthScore = score

	_ = r.backend.Put(nodeID, node)
	return node, true
}

// Deregister gracefully removes a node.
func (r *Registry) Deregister(nodeID string) {
	r.backend.Delete(nodeID)
}

// GetNode returns a single node by ID.
func (r *Registry) GetNode(nodeID string) (NodeRecord, bool) {
	return r.backend.Get(nodeID)
}

// ListNodes returns all nodes, optionally filtered by state.
func (r *Registry) ListNodes(stateFilter string) []NodeRecord {
	all := r.backend.List()
	if stateFilter == "" {
		return all
	}
	result := make([]NodeRecord, 0, len(all))
	for _, n := range all {
		if n.State == stateFilter {
			result = append(result, n)
		}
	}
	return result
}

// Stats returns summary statistics.
type Stats struct {
	Total       int `json:"total"`
	Healthy     int `json:"healthy"`
	Stale       int `json:"stale"`
	Unavailable int `json:"unavailable"`
}

func (r *Registry) Stats() Stats {
	nodes := r.backend.List()
	var s Stats
	for _, n := range nodes {
		s.Total++
		switch n.State {
		case "healthy":
			s.Healthy++
		case "stale":
			s.Stale++
		case "unavailable":
			s.Unavailable++
		}
	}
	return s
}

func (r *Registry) sweepLoop() {
	ticker := time.NewTicker(r.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.sweep()
		case <-r.stopCh:
			return
		}
	}
}

func (r *Registry) sweep() {
	now := time.Now().UTC()
	nodes := r.backend.List()

	for _, node := range nodes {
		if now.After(node.LeaseExpiry) {
			staleDuration := now.Sub(node.LeaseExpiry)
			if staleDuration > r.config.StaleGrace+r.config.RemovalThreshold {
				r.backend.Delete(node.NodeID)
			} else if staleDuration > r.config.StaleGrace {
				node.State = "unavailable"
				_ = r.backend.Put(node.NodeID, node)
			} else {
				if node.State != "stale" {
					node.State = "stale"
					_ = r.backend.Put(node.NodeID, node)
				}
			}
		}
	}
}

// Sweep runs a single sweep cycle (exported for testing).
func (r *Registry) Sweep() {
	r.sweep()
}

// Handler returns an http.Handler for the registry API (using chi router).
func (r *Registry) Handler() http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)

	router.Post("/v1/register", func(w http.ResponseWriter, req *http.Request) {
		var rec NodeRecord
		if err := json.NewDecoder(req.Body).Decode(&rec); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
			return
		}
		if rec.NodeID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "node_id required"})
			return
		}
		result := r.Register(rec)
		writeJSON(w, http.StatusOK, result)
	})

	router.Post("/v1/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			NodeID string   `json:"node_id"`
			Load   LoadInfo `json:"load"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
			return
		}
		result, ok := r.Heartbeat(body.NodeID, body.Load)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "node not registered"})
			return
		}
		writeJSON(w, http.StatusOK, result)
	})

	router.Post("/v1/deregister", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
			return
		}
		r.Deregister(body.NodeID)
		writeJSON(w, http.StatusOK, map[string]string{"status": "deregistered"})
	})

	router.Get("/v1/nodes", func(w http.ResponseWriter, req *http.Request) {
		state := req.URL.Query().Get("state")
		nodes := r.ListNodes(state)
		writeJSON(w, http.StatusOK, nodes)
	})

	router.Get("/v1/stats", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, r.Stats())
	})

	router.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "registry"})
	})

	return router
}

// ListenAndServe starts the registry HTTP server.
func (r *Registry) ListenAndServe(port int) error {
	r.Start()
	defer r.Stop()
	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, r.Handler())
}

// BackendType returns a string identifying the backend type ("memory" or "nats_kv").
func (r *Registry) BackendType() string {
	switch r.backend.(type) {
	case *natsKVBackend:
		return "nats_kv"
	default:
		return "memory"
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// MatchesNodeID checks if a given ID matches a node (full or short prefix).
func MatchesNodeID(fullID, query string) bool {
	return fullID == query || strings.HasPrefix(fullID, query)
}
