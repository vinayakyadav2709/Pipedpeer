package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// Default lease configuration
const (
	DefaultLeaseDuration = 30 * time.Second
	DefaultGracePeriod   = 5 * time.Second
	DefaultSweepInterval = 2 * time.Second

	// DefaultRunningLeaseTTL is how long a running lease survives with no
	// renewal from its submitter. Generous, because it only ever fires when a
	// submitter has actually gone away: a live one renews every few seconds.
	DefaultRunningLeaseTTL = 5 * time.Minute
)

// LeaseState tracks where a lease is in its lifecycle.
type LeaseState string

const (
	LeaseReserved LeaseState = "reserved"
	LeaseRunning  LeaseState = "running"
)

// Lease represents an active resource reservation on this node.
type Lease struct {
	LeaseID       string     `json:"lease_id"`
	JobName       string     `json:"job_name"`
	SubmitterNode string     `json:"submitter_node"`
	State         LeaseState `json:"state"`
	MemBytes      int64      `json:"mem_bytes"`
	GPUIndex      int        `json:"gpu_index"` // reserved GPU device (-1 = none)
	CreatedAt     time.Time  `json:"created_at"`
	ExpiresAt     time.Time  `json:"expires_at"`
	// RenewedAt is the last sign of life from the submitter. A running lease
	// is held for RunningLeaseTTL past this, so a long job that keeps
	// renewing is never reaped while a submitter that died releases its slot.
	RenewedAt time.Time `json:"renewed_at"`
}

// Server is the daemon API server that manages job leases and resource reservations.
type Server struct {
	nodeID          string
	router          chi.Router
	leaseDuration   time.Duration
	gracePeriod     time.Duration
	sweepInterval   time.Duration
	runningLeaseTTL time.Duration

	mu     sync.Mutex
	leases map[string]*Lease // lease_id → Lease

	stopSweep chan struct{}

	// Job execution tracking
	jobDir string
	jobsMu sync.Mutex
	jobs   map[string]*JobRecord

	// NATS subscriptions (nil if NATS not configured)
	natsSubs []*nats.Subscription

	// Round-robin counter for cross-invocation persistence
	rrCounter atomic.Int64

	// Maximum concurrent tasks this node will hold (0 = unlimited)
	maxConcurrent atomic.Int64

	// Node store (SQLite — single source of truth for all peers)
	store *nodestore.Store

	// Peer health cache (populated by background poller)
	peersMu     sync.RWMutex
	peerHealths map[string]*PeerHealth // key: "host:port"
	stopPoller  chan struct{}

	// Discovery function for mDNS scanning (set before StartPeerPoller)
	discoverFn func() []NodeDiscovered

	// How this node describes itself in the node store (set by SetSelfInfo)
	selfMu   sync.RWMutex
	selfSSH  string
	selfPort int
	selfCaps map[string]string
}

// healthResponse is the payload of GET /health. It carries the node's full
// capabilities and load so a peer polling it learns everything the scheduler
// needs — CPU cores, GPU model, free VRAM — in one round trip.
type healthResponse struct {
	Status        string            `json:"status"`
	NodeID        string            `json:"node_id"`
	ActiveJobs    int               `json:"active_jobs"`
	ActiveLeases  int               `json:"active_leases"`
	ReservedMem   int64             `json:"reserved_mem"`
	AvailableMem  int64             `json:"available_mem"`
	MaxConcurrent int               `json:"max_concurrent"`
	Capabilities  map[string]string `json:"capabilities"`
	Load          registry.LoadInfo `json:"load"`
}

// NodeDiscovered represents a node found via mDNS or other discovery.
type NodeDiscovered struct {
	NodeID      string
	SSHEndpoint string
	DaemonPort  int
	Arch        string
	Hostname    string
}

// PeerHealth represents the live state of a peer worker.
type PeerHealth struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	NodeID       string `json:"node_id"`
	Status       string `json:"status"` // "healthy", "unreachable"
	ActiveJobs   int    `json:"active_jobs"`
	AvailableMem int64  `json:"available_mem"`
	Source       string `json:"source"` // "manual", "discovery", "registry"
}

// --- Request/Response types ---

type acceptRequest struct {
	TargetID         string `json:"target_id"`
	JobName          string `json:"job_name"`
	SubmitterNode    string `json:"submitter_node"`
	RequiredMemBytes int64  `json:"required_mem_bytes,omitempty"`
	RequiredCores    int    `json:"required_cores,omitempty"`
	RequireGPU       bool   `json:"require_gpu,omitempty"`
	RequiredGPUMem   int64  `json:"required_gpu_mem_bytes,omitempty"`
}

type acceptResponse struct {
	Accepted  bool   `json:"accepted"`
	NodeID    string `json:"node_id"`
	Reason    string `json:"reason,omitempty"`
	LeaseID   string `json:"lease_id,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
	GPUIndex  int    `json:"gpu_index"`
}

type commitRequest struct {
	LeaseID string `json:"lease_id"`
}

type commitResponse struct {
	Committed bool   `json:"committed"`
	NodeID    string `json:"node_id"`
	Reason    string `json:"reason,omitempty"`
}

type completeRequest struct {
	LeaseID string `json:"lease_id"`
	JobName string `json:"job_name"` // backwards compat
	Status  string `json:"status"`   // "succeeded" or "failed"
}

type cancelRequest struct {
	LeaseID       string `json:"lease_id"`
	SubmitterNode string `json:"submitter_node"`
}

// --- Constructor ---

func New(nodeID string) *Server {
	return NewWithConfig(nodeID, DefaultLeaseDuration, DefaultGracePeriod, DefaultSweepInterval)
}

func NewWithConfig(nodeID string, leaseDuration, gracePeriod, sweepInterval time.Duration) *Server {
	store, err := nodestore.New()
	if err != nil {
		// Non-fatal: store may be unavailable, daemon still works via mDNS-only
		store = nil
	}
	s := &Server{
		nodeID:          nodeID,
		leaseDuration:   leaseDuration,
		gracePeriod:     gracePeriod,
		sweepInterval:   sweepInterval,
		runningLeaseTTL: DefaultRunningLeaseTTL,
		leases:          make(map[string]*Lease),
		stopSweep:       make(chan struct{}),
		jobDir:          defaultJobDir(),
		jobs:            make(map[string]*JobRecord),
		store:           store,
	}
	s.buildRouter()
	return s
}

// SetDiscoverFn installs the LAN discovery probe used by the peer poller.
// Must be called before StartPeerPoller.
func (s *Server) SetDiscoverFn(fn func() []NodeDiscovered) {
	s.discoverFn = fn
}

// SetMaxConcurrentJobs caps how many tasks this node will hold at once.
// 0 means unlimited. Beyond the cap, /v1/accept replies 503 and the submitting
// orchestrator moves on to the next candidate node.
func (s *Server) SetMaxConcurrentJobs(n int) {
	if n < 0 {
		n = 0
	}
	s.maxConcurrent.Store(int64(n))
}

// MaxConcurrentJobs returns the configured cap (0 = unlimited).
func (s *Server) MaxConcurrentJobs() int {
	return int(s.maxConcurrent.Load())
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) ListenAndServe(port int) error {
	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, s.Handler())
}

// BindNATS subscribes to NATS subjects for this node's lease operations.
// Enables NATS-based transport alongside HTTP.
func (s *Server) BindNATS(bus *natsbus.Bus) error {
	prefix := fmt.Sprintf("pipedpeer.daemon.%s", s.nodeID)

	acceptSub, err := bus.Subscribe(prefix+".accept", s.handleNATSAccept)
	if err != nil {
		return fmt.Errorf("subscribe accept: %w", err)
	}

	commitSub, err := bus.Subscribe(prefix+".commit", s.handleNATSCommit)
	if err != nil {
		return fmt.Errorf("subscribe commit: %w", err)
	}

	completeSub, err := bus.Subscribe(prefix+".complete", s.handleNATSComplete)
	if err != nil {
		return fmt.Errorf("subscribe complete: %w", err)
	}

	cancelSub, err := bus.Subscribe(prefix+".cancel", s.handleNATSCancel)
	if err != nil {
		return fmt.Errorf("subscribe cancel: %w", err)
	}

	healthSub, err := bus.Subscribe(prefix+".health", s.handleNATSHealth)
	if err != nil {
		return fmt.Errorf("subscribe health: %w", err)
	}

	renewSub, err := bus.Subscribe(prefix+".renew", s.handleNATSRenew)
	if err != nil {
		return fmt.Errorf("subscribe renew: %w", err)
	}

	s.natsSubs = append(s.natsSubs, acceptSub, commitSub, completeSub, cancelSub, healthSub, renewSub)
	return nil
}

// UnbindNATS unsubscribes from all NATS subjects.
func (s *Server) UnbindNATS() {
	for _, sub := range s.natsSubs {
		_ = sub.Unsubscribe()
	}
	s.natsSubs = nil
}

// StartSweeper starts the background goroutine that expires stale leases.
func (s *Server) StartSweeper() {
	go func() {
		ticker := time.NewTicker(s.sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.sweepExpired()
			case <-s.stopSweep:
				return
			}
		}
	}()
}

// StopSweeper stops the background lease sweeper.
func (s *Server) StopSweeper() {
	select {
	case s.stopSweep <- struct{}{}:
	default:
	}
}

// sweepExpired removes leases in "reserved" state past expiry + grace.
// Running leases are NOT expired — only uncommitted reservations.
// sweepExpired releases leases nobody is coming back for.
//
// Reserved leases expire quickly — the submitter should commit within seconds.
// Running leases are held far longer and only released once the submitter has
// stopped renewing, so a genuinely long job is never cut off mid-run.
func (s *Server) sweepExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, lease := range s.leases {
		switch lease.State {
		case LeaseReserved:
			if now.After(lease.ExpiresAt.Add(s.gracePeriod)) {
				delete(s.leases, id)
			}
		case LeaseRunning:
			last := lease.RenewedAt
			if last.IsZero() {
				last = lease.CreatedAt
			}
			if s.runningLeaseTTL > 0 && now.Sub(last) > s.runningLeaseTTL {
				delete(s.leases, id)
			}
		}
	}
}

// SetRunningLeaseTTL overrides how long a running lease survives without a
// renewal. 0 disables reaping of running leases entirely.
func (s *Server) SetRunningLeaseTTL(d time.Duration) {
	s.mu.Lock()
	s.runningLeaseTTL = d
	s.mu.Unlock()
}

// processRenew records a sign of life from the lease holder.
func (s *Server) processRenew(leaseID string) (map[string]interface{}, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[leaseID]
	if !ok {
		return map[string]interface{}{"renewed": false, "reason": "unknown lease"}, http.StatusNotFound
	}
	now := time.Now()
	lease.RenewedAt = now
	if lease.State == LeaseReserved {
		lease.ExpiresAt = now.Add(s.leaseDuration)
	}
	return map[string]interface{}{
		"renewed":    true,
		"lease_id":   leaseID,
		"expires_at": lease.ExpiresAt.Format(time.RFC3339Nano),
	}, http.StatusOK
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req struct {
		LeaseID string `json:"lease_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LeaseID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "lease_id required"})
		return
	}
	resp, status := s.processRenew(req.LeaseID)
	writeJSON(w, status, resp)
}

// --- Public accessors ---

// ActiveJobs returns the number of leases in "running" state.
func (s *Server) ActiveJobs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, l := range s.leases {
		if l.State == LeaseRunning {
			count++
		}
	}
	return count
}

// ActiveLeases returns the total number of leases (reserved + running).
func (s *Server) ActiveLeases() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.leases)
}

// ReservedMem returns the total memory reserved by all active leases.
func (s *Server) ReservedMem() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, l := range s.leases {
		total += l.MemBytes
	}
	return total
}

// AvailableForJob returns real OS available memory minus pipedpeer reservations.
func (s *Server) AvailableForJob() int64 {
	load := heartbeat.CollectLoad(s.ActiveJobs(), s.ReservedMem())
	return load.AvailableMemBytes
}

// GetLease returns a copy of a lease by ID.
func (s *Server) GetLease(leaseID string) (Lease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[leaseID]
	if !ok {
		return Lease{}, false
	}
	return *l, true
}

// --- Routes (chi) ---

func (s *Server) buildRouter() {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/health", s.handleHealth)
	r.Post("/v1/accept", s.handleAccept)
	r.Post("/v1/commit", s.handleCommit)
	r.Post("/v1/renew", s.handleRenew)
	r.Post("/v1/complete", s.handleComplete)
	r.Post("/v1/cancel", s.handleCancel)
	r.Post("/v1/jobs/upload", s.handleJobUpload)
	r.Get("/v1/jobs/{id}/exec", s.handleJobExec)
	r.Get("/v1/jobs/{id}/results", s.handleJobResults)
	r.Get("/v1/roundrobin", s.handleRoundRobin)
	r.Get("/v1/jobs", s.handleJobs)
	r.Get("/v1/nodes", s.handleNodes)
	r.Post("/v1/nodes", s.handleNodesAdd)
	r.Delete("/v1/nodes/{host}", s.handleNodesRemove)

	s.router = r
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.healthSnapshot())
}

func (s *Server) healthSnapshot() healthResponse {
	activeJobs := s.ActiveJobs()
	reserved := s.ReservedMem()

	s.selfMu.RLock()
	caps := s.selfCaps
	s.selfMu.RUnlock()
	if caps == nil {
		caps = map[string]string{}
	}

	load := heartbeat.CollectLoad(activeJobs, reserved)
	return healthResponse{
		Status:        "ok",
		NodeID:        s.nodeID,
		ActiveJobs:    activeJobs,
		ActiveLeases:  s.ActiveLeases(),
		ReservedMem:   reserved,
		AvailableMem:  load.AvailableMemBytes,
		MaxConcurrent: s.MaxConcurrentJobs(),
		Capabilities:  caps,
		Load:          load,
	}
}

// handleJobs lists this node's live leases, so any machine can render a
// cluster-wide view of what is running and where.
func (s *Server) handleJobs(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]Lease, 0, len(s.leases))
	for _, l := range s.leases {
		out = append(out, *l)
	}
	s.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id": s.nodeID,
		"jobs":    out,
	})
}

func (s *Server) handleRoundRobin(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		http.Error(w, `{"error":"invalid count"}`, http.StatusBadRequest)
		return
	}
	idx := int(s.rrCounter.Add(1)-1) % count
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"index":%d}`, idx)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	dbNodes, err := s.store.ListAll()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	nodes := make([]nodeResp, 0, len(dbNodes))
	for _, n := range dbNodes {
		nodes = append(nodes, toNodeResp(n))
	}
	writeJSON(w, http.StatusOK, nodes)
}

// nodeResp is a registry.NodeRecord as served by /v1/nodes. The orchestrator
// decodes it straight back into registry.NodeRecord, so the JSON tags must
// match that type's.
type nodeResp struct {
	NodeID       string            `json:"node_id"`
	SSHEndpoint  string            `json:"ssh_endpoint"`
	DaemonPort   int               `json:"daemon_port"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
	State        string            `json:"state"`
	HealthScore  float64           `json:"health_score"`
	Source       string            `json:"source"`
}

func toNodeResp(n nodestore.Node) nodeResp {
	ssh := n.SSHEndpoint
	if ssh == "" {
		ssh = fmt.Sprintf("root@%s:22", n.Host)
	}

	// Prefer the full payload captured from the node's own /health; fall back
	// to the indexed columns for entries that have never been polled.
	caps := map[string]string{}
	if n.CapsJSON != "" {
		_ = json.Unmarshal([]byte(n.CapsJSON), &caps)
	}
	if caps["arch"] == "" && n.Arch != "" {
		caps["arch"] = n.Arch
	}
	if caps["hostname"] == "" && n.Hostname != "" {
		caps["hostname"] = n.Hostname
	}

	var load registry.LoadInfo
	if n.LoadJSON != "" {
		_ = json.Unmarshal([]byte(n.LoadJSON), &load)
	}
	load.AvailableMemBytes = n.AvailableMem
	load.ActiveJobs = n.ActiveJobs
	load.ReservedMemBytes = n.ReservedMem
	if n.TotalMem > 0 {
		load.TotalMemBytes = n.TotalMem
	}
	if n.CPUPercent > 0 {
		load.CPUPercent = n.CPUPercent
	}

	return nodeResp{
		NodeID:       n.NodeID,
		SSHEndpoint:  ssh,
		DaemonPort:   n.Port,
		Capabilities: caps,
		Load:         load,
		State:        n.State,
		HealthScore:  n.HealthScore,
		Source:       n.Source,
	}
}

func (s *Server) handleNodesAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Host == "" || req.Port == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "host and port required"})
		return
	}
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
		return
	}
	if err := s.store.AddManual(req.Host, req.Port); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (s *Server) handleNodesRemove(w http.ResponseWriter, r *http.Request) {
	host := chi.URLParam(r, "host")
	if host == "_all" {
		if s.store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
			return
		}
		if err := s.store.RemoveAll(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "all removed"})
		return
	}
	if host == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "host required"})
		return
	}
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
		return
	}
	if err := s.store.RemoveManual(host); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

// StartPeerPoller launches a background goroutine that discovers nodes via mDNS
// and polls health from both discovered and manually added nodes.
func (s *Server) StartPeerPoller(interval time.Duration) {
	s.peersMu.Lock()
	s.peerHealths = make(map[string]*PeerHealth)
	s.stopPoller = make(chan struct{})
	s.peersMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.pollAllNodes()

		for {
			select {
			case <-ticker.C:
				s.pollAllNodes()
			case <-s.stopPoller:
				return
			}
		}
	}()
}

func (s *Server) StopPeerPoller() {
	s.peersMu.Lock()
	if s.stopPoller != nil {
		close(s.stopPoller)
	}
	s.peersMu.Unlock()
}

// extractHost pulls "host" from "user@host:port" or "host:port" or "host".
func extractHost(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx != -1 {
		s = s[idx+1:]
	}
	if idx := strings.LastIndex(s, ":"); idx != -1 {
		s = s[:idx]
	}
	return s
}

// SetSelfInfo tells the daemon how to describe itself in the node store, so
// this machine is a placement candidate for every peer that polls it and shows
// up in its own node list.
func (s *Server) SetSelfInfo(sshEndpoint string, daemonPort int, caps map[string]string) {
	s.selfMu.Lock()
	s.selfSSH = sshEndpoint
	s.selfPort = daemonPort
	s.selfCaps = caps
	s.selfMu.Unlock()
}

// pollAllNodes is one refresh cycle of cluster membership:
//
//	discover on the LAN → record self → health-check every peer → prune the dead.
//
// Everything the orchestrator later reads from /v1/nodes is written here.
func (s *Server) pollAllNodes() {
	if s.store == nil {
		return
	}

	// 1. mDNS discovery. New nodes are inserted with state "unknown" and get
	//    promoted to healthy by the health check below.
	if s.discoverFn != nil {
		for _, d := range s.discoverFn() {
			if d.NodeID == "" || d.NodeID == s.nodeID {
				continue
			}
			_ = s.store.UpsertNode(nodestore.Node{
				NodeID:      d.NodeID,
				Host:        extractHost(d.SSHEndpoint),
				Port:        d.DaemonPort,
				SSHEndpoint: d.SSHEndpoint,
				Arch:        d.Arch,
				Hostname:    d.Hostname,
				State:       "unknown",
				Source:      "discovery",
			})
		}
	}

	// 2. Record self. Self is always a placement candidate, so it belongs in
	//    the same table as everyone else rather than being special-cased.
	s.upsertSelf()

	// 3. Health-check every known peer concurrently.
	nodes, err := s.store.ListAll()
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	finalHealths := make(map[string]*PeerHealth)

	for _, n := range nodes {
		if n.NodeID == s.nodeID {
			continue // self is refreshed directly, not over the network
		}
		wg.Add(1)
		go func(node nodestore.Node) {
			defer wg.Done()
			ph := s.pollOne(node)
			mu.Lock()
			finalHealths[fmt.Sprintf("%s:%d", node.Host, node.Port)] = ph
			mu.Unlock()
		}(n)
	}
	wg.Wait()

	s.peersMu.Lock()
	s.peerHealths = finalHealths
	s.peersMu.Unlock()

	// 4. Forget auto-discovered nodes that have been gone a while. Manual
	//    entries survive — see nodestore.PruneStale.
	s.store.PruneStale(staleNodeTTL)
}

// staleNodeTTL is how long a discovered node may go unseen before it is
// dropped: long enough to ride out a daemon restart, short enough that a
// machine taken off the LAN stops being offered work.
const staleNodeTTL = 5 * time.Minute

func (s *Server) pollOne(node nodestore.Node) *PeerHealth {
	source := node.Source
	if source == "" {
		source = "discovery"
	}
	ph := &PeerHealth{Host: node.Host, Port: node.Port, NodeID: node.NodeID, Source: source}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s:%d/health", node.Host, node.Port))
	if err != nil {
		ph.Status = "unreachable"
		s.store.MarkUnreachable(node.NodeID)
		return ph
	}
	defer resp.Body.Close()

	var h healthResponse
	if json.NewDecoder(resp.Body).Decode(&h) != nil || h.NodeID == "" {
		ph.Status = "unreachable"
		s.store.MarkUnreachable(node.NodeID)
		return ph
	}

	ph.NodeID = h.NodeID
	ph.Status = "healthy"
	ph.ActiveJobs = h.ActiveJobs
	ph.AvailableMem = h.AvailableMem

	capsJSON, _ := json.Marshal(h.Capabilities)
	loadJSON, _ := json.Marshal(h.Load)

	updated := nodestore.Node{
		NodeID:       h.NodeID,
		Host:         node.Host,
		Port:         node.Port,
		SSHEndpoint:  node.SSHEndpoint,
		Arch:         h.Capabilities["arch"],
		Hostname:     h.Capabilities["hostname"],
		ActiveJobs:   h.ActiveJobs,
		AvailableMem: h.AvailableMem,
		ReservedMem:  h.ReservedMem,
		TotalMem:     h.Load.TotalMemBytes,
		CPUPercent:   h.Load.CPUPercent,
		Source:       source,
		IsManual:     node.IsManual,
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	}
	if err := s.store.MarkHealthy(updated); err != nil {
		return ph
	}

	// A manual entry added before the node was up carries a placeholder
	// "manual-host:port" ID. Now that the real UUID is known, drop the
	// placeholder row so the node is not counted twice.
	if node.NodeID != h.NodeID && strings.HasPrefix(node.NodeID, "manual-") {
		s.store.DeleteNode(node.NodeID)
	}
	return ph
}

// upsertSelf writes this node's own live state into the store.
func (s *Server) upsertSelf() {
	s.selfMu.RLock()
	ssh, port, caps := s.selfSSH, s.selfPort, s.selfCaps
	s.selfMu.RUnlock()
	if port == 0 {
		return // SetSelfInfo not called (tests, or daemon not fully started)
	}

	load := heartbeat.CollectLoad(s.ActiveJobs(), s.ReservedMem())
	capsJSON, _ := json.Marshal(caps)
	loadJSON, _ := json.Marshal(load)

	_ = s.store.MarkHealthy(nodestore.Node{
		NodeID:       s.nodeID,
		Host:         extractHost(ssh),
		Port:         port,
		SSHEndpoint:  ssh,
		Arch:         caps["arch"],
		Hostname:     caps["hostname"],
		ActiveJobs:   s.ActiveJobs(),
		AvailableMem: load.AvailableMemBytes,
		ReservedMem:  s.ReservedMem(),
		TotalMem:     load.TotalMemBytes,
		CPUPercent:   load.CPUPercent,
		Source:       "self",
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	})
}

// --- Core lease logic (shared by HTTP and NATS handlers) ---

func (s *Server) processAccept(req acceptRequest) (acceptResponse, int) {
	if req.TargetID == "" {
		return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id is required"}, http.StatusBadRequest
	}
	if req.TargetID != s.nodeID {
		return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id does not match this node"}, http.StatusConflict
	}

	// Concurrency cap. Counts every live lease, not just running ones: a
	// reserved-but-not-yet-committed lease is a task on its way here, and
	// counting only running leases would let an unbounded number of
	// submitters slip past the cap in the window before they commit.
	if max := s.MaxConcurrentJobs(); max > 0 {
		if active := s.ActiveLeases(); active >= max {
			reason := fmt.Sprintf("at concurrency limit: %d/%d tasks", active, max)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	// Resource admission check
	if req.RequiredMemBytes > 0 {
		available := s.AvailableForJob()
		if available < req.RequiredMemBytes {
			reason := fmt.Sprintf("insufficient memory: need %d bytes, available %d bytes",
				req.RequiredMemBytes, available)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	if req.RequiredCores > 0 {
		if free := s.freeCores(); free < float64(req.RequiredCores) {
			reason := fmt.Sprintf("insufficient cores: need %d, free %.1f", req.RequiredCores, free)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	// GPU admission: reserve a specific device so two concurrent jobs on this
	// node do not land on the same card.
	gpuIndex := -1
	if req.RequireGPU {
		gpuIndex = s.reserveGPU(req.RequiredGPUMem)
		if gpuIndex < 0 {
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "no GPU available"}, http.StatusServiceUnavailable
		}
	}

	// Create lease
	leaseID := generateLeaseID()
	now := time.Now()
	lease := &Lease{
		LeaseID:       leaseID,
		JobName:       req.JobName,
		SubmitterNode: req.SubmitterNode,
		State:         LeaseReserved,
		MemBytes:      req.RequiredMemBytes,
		GPUIndex:      gpuIndex,
		CreatedAt:     now,
		ExpiresAt:     now.Add(s.leaseDuration),
	}

	s.mu.Lock()
	s.leases[leaseID] = lease
	s.mu.Unlock()

	return acceptResponse{
		Accepted:  true,
		NodeID:    s.nodeID,
		LeaseID:   leaseID,
		GPUIndex:  gpuIndex,
		ExpiresAt: lease.ExpiresAt.Format(time.RFC3339Nano),
	}, http.StatusOK
}

// freeCores estimates CPU cores not currently in use on this node.
func (s *Server) freeCores() float64 {
	load := heartbeat.CollectLoad(s.ActiveJobs(), s.ReservedMem())
	cores := load.TotalCPUs
	if cores <= 0 {
		cores = runtime.NumCPU()
	}
	free := float64(cores) * (1.0 - load.CPUPercent/100.0)
	if free < 0 {
		return 0
	}
	return free
}

// reserveGPU picks the free GPU with the most available VRAM, skipping devices
// already held by a live lease. Returns -1 when nothing satisfies minVRAM.
func (s *Server) reserveGPU(minVRAM int64) int {
	devices := gpu.PerDevice()
	if len(devices) == 0 {
		// A GPU with no per-device telemetry (Intel, or AMD without a
		// parseable rocm-smi) can still run the job on device 0. But with no
		// VRAM numbers we cannot honour a minimum, so a request that specifies
		// one must be refused rather than accepted on a guess.
		if gpu.Detect().Vendor != gpu.VendorNone && minVRAM == 0 {
			return 0
		}
		return -1
	}

	s.mu.Lock()
	reserved := make(map[int]bool, len(s.leases))
	for _, l := range s.leases {
		if l.GPUIndex >= 0 && (l.State == LeaseReserved || l.State == LeaseRunning) {
			reserved[l.GPUIndex] = true
		}
	}
	s.mu.Unlock()

	best := -1
	var bestFree int64 = -1
	for _, d := range devices {
		if reserved[d.Index] {
			continue
		}
		if minVRAM > 0 && d.MemoryFreeBytes < minVRAM {
			continue
		}
		if d.MemoryFreeBytes > bestFree {
			best, bestFree = d.Index, d.MemoryFreeBytes
		}
	}
	return best
}

func (s *Server) processCommit(req commitRequest) (commitResponse, int) {
	s.mu.Lock()
	lease, ok := s.leases[req.LeaseID]
	if !ok {
		s.mu.Unlock()
		return commitResponse{Committed: false, NodeID: s.nodeID, Reason: "lease not found (expired or cancelled)"}, http.StatusNotFound
	}

	if lease.State != LeaseReserved {
		s.mu.Unlock()
		return commitResponse{
			Committed: false, NodeID: s.nodeID,
			Reason: fmt.Sprintf("lease state is %s, expected reserved", lease.State),
		}, http.StatusConflict
	}

	// Check if lease has expired beyond grace period
	if time.Now().After(lease.ExpiresAt.Add(s.gracePeriod)) {
		delete(s.leases, req.LeaseID)
		s.mu.Unlock()
		return commitResponse{Committed: false, NodeID: s.nodeID, Reason: "lease expired"}, http.StatusGone
	}

	// Re-check resources at commit time (conditions may have changed since accept)
	if lease.MemBytes > 0 {
		s.mu.Unlock() // Unlock for AvailableForJob (which calls CollectLoad)
		available := s.AvailableForJob()
		s.mu.Lock()

		// Re-verify lease still exists (could have been swept during unlock)
		if _, stillExists := s.leases[req.LeaseID]; !stillExists {
			s.mu.Unlock()
			return commitResponse{Committed: false, NodeID: s.nodeID, Reason: "lease expired during commit"}, http.StatusNotFound
		}

		// Available already accounts for this lease's reservation, so we only check
		// if overall system health is still acceptable. If available dropped below 0
		// (other system processes consumed memory), reject.
		if available < 0 {
			delete(s.leases, req.LeaseID)
			s.mu.Unlock()
			return commitResponse{
				Committed: false, NodeID: s.nodeID,
				Reason: fmt.Sprintf("resources no longer available: available=%d", available),
			}, http.StatusServiceUnavailable
		}
	}

	// Commit: transition to running. Stamp RenewedAt so the running-lease TTL
	// is measured from the moment execution actually started.
	lease.State = LeaseRunning
	lease.RenewedAt = time.Now()
	s.mu.Unlock()

	return commitResponse{Committed: true, NodeID: s.nodeID}, http.StatusOK
}

func (s *Server) processComplete(req completeRequest) {
	s.mu.Lock()
	// Support both lease_id and job_name for backwards compatibility
	leaseID := req.LeaseID
	if leaseID == "" && req.JobName != "" {
		for id, l := range s.leases {
			if l.JobName == req.JobName {
				leaseID = id
				break
			}
		}
	}
	if leaseID != "" {
		delete(s.leases, leaseID)
	}
	s.mu.Unlock()
}

func (s *Server) processCancel(req cancelRequest) (map[string]string, int) {
	s.mu.Lock()
	lease, ok := s.leases[req.LeaseID]
	if !ok {
		s.mu.Unlock()
		return map[string]string{"error": "lease not found"}, http.StatusNotFound
	}

	// Only the submitter can cancel
	if lease.SubmitterNode != "" && req.SubmitterNode != lease.SubmitterNode {
		s.mu.Unlock()
		return map[string]string{"error": "only the submitter can cancel"}, http.StatusForbidden
	}

	delete(s.leases, req.LeaseID)
	s.mu.Unlock()

	return map[string]string{"status": "cancelled"}, http.StatusOK
}

// --- HTTP Handlers ---

// handleAccept creates a lease reservation. Resources are held but job is NOT running yet.
// Submitter must call /v1/commit within the lease duration (+ grace) to start.
func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
	var req acceptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "invalid request body"})
		return
	}
	resp, status := s.processAccept(req)
	writeJSON(w, status, resp)
}

// handleCommit transitions a lease from reserved → running.
// Re-checks resource availability at commit time (early commit allowed).
func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req commitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, commitResponse{Committed: false, NodeID: s.nodeID, Reason: "invalid request body"})
		return
	}
	resp, status := s.processCommit(req)
	writeJSON(w, status, resp)
}

// handleComplete finalizes a running lease (succeeded or failed).
func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req completeRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.processComplete(req)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCancel releases a reservation. Only the original submitter can cancel.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var req cancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	resp, status := s.processCancel(req)
	writeJSON(w, status, resp)
}

// --- NATS Handlers (same logic, NATS transport) ---

func (s *Server) handleNATSAccept(msg *nats.Msg) {
	var req acceptRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		natsReply(msg, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "invalid request"})
		return
	}
	resp, _ := s.processAccept(req)
	natsReply(msg, resp)
}

func (s *Server) handleNATSCommit(msg *nats.Msg) {
	var req commitRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		natsReply(msg, commitResponse{Committed: false, NodeID: s.nodeID, Reason: "invalid request"})
		return
	}
	resp, _ := s.processCommit(req)
	natsReply(msg, resp)
}

func (s *Server) handleNATSRenew(msg *nats.Msg) {
	var req struct {
		LeaseID string `json:"lease_id"`
	}
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		natsReply(msg, map[string]interface{}{"renewed": false, "reason": "invalid request"})
		return
	}
	resp, _ := s.processRenew(req.LeaseID)
	natsReply(msg, resp)
}

func (s *Server) handleNATSComplete(msg *nats.Msg) {
	var req completeRequest
	_ = json.Unmarshal(msg.Data, &req)
	s.processComplete(req)
	natsReply(msg, map[string]string{"status": "ok"})
}

func (s *Server) handleNATSCancel(msg *nats.Msg) {
	var req cancelRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		natsReply(msg, map[string]string{"error": "invalid request"})
		return
	}
	resp, _ := s.processCancel(req)
	natsReply(msg, resp)
}

func (s *Server) handleNATSHealth(msg *nats.Msg) {
	natsReply(msg, map[string]interface{}{
		"status":        "ok",
		"node_id":       s.nodeID,
		"active_jobs":   s.ActiveJobs(),
		"active_leases": s.ActiveLeases(),
		"reserved_mem":  s.ReservedMem(),
		"available_mem": s.AvailableForJob(),
	})
}

// --- Helpers ---

func natsReply(msg *nats.Msg, body any) {
	data, _ := json.Marshal(body)
	_ = msg.Respond(data)
}

func generateLeaseID() string {
	return uuid.New().String()
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
