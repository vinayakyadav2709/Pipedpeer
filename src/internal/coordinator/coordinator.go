package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// NodeSource describes where candidate nodes came from.
type NodeSource string

const (
	SourceRegistry  NodeSource = "registry"
	SourceCache     NodeSource = "cache"
	SourceDiscovery NodeSource = "discovery"
	SourceSelf      NodeSource = "self"
	SourceExplicit  NodeSource = "explicit"
	SourceManual    NodeSource = "manual"
)

// ScoredNode is a candidate with a placement score.
type ScoredNode struct {
	Node         registry.NodeRecord `json:"node"`
	Score        float64             `json:"score"`
	Source       NodeSource          `json:"source"`
	Rejected     bool                `json:"rejected"`
	RejectReason string              `json:"reject_reason,omitempty"`
}

// PlacementDecision records the full decision for diagnostics.
type PlacementDecision struct {
	ChosenNode   registry.NodeRecord `json:"chosen_node"`
	Source       NodeSource          `json:"source"`
	DegradedMode bool               `json:"degraded_mode"`
	Candidates   []ScoredNode       `json:"candidates"`
	Reason       string              `json:"reason"`
}

// DiscoveryFunc is a pluggable function for mDNS or other LAN discovery.
// Returns discovered nodes. The coordinator calls this per-task.
type DiscoveryFunc func() []registry.NodeRecord

// Coordinator handles node selection and placement.
type Coordinator struct {
	registryURL      string
	httpClient       *http.Client
	bus              *natsbus.Bus // optional NATS bus for inter-node transport
	selfIdentity     identity.NodeIdentity
	selfSSH          string
	selfDaemon       int
	selfLoad         registry.LoadInfo
	discoverFn       DiscoveryFunc
	maxCacheAge      time.Duration
	requiredMemBytes int64
	retryInterval    time.Duration
	noSelf           bool
	strategy         string
	roundRobinFn     func(count int) int // external round-robin counter, nil = in-memory

	mu          sync.Mutex
	cachedNodes []registry.NodeRecord
	cacheTime   time.Time
	rrCounter   int // round-robin counter
}

// Config for the coordinator.
type Config struct {
	RegistryURL      string            // empty = no registry
	Bus              *natsbus.Bus      // optional: NATS bus for inter-node transport (nil = HTTP only)
	SelfIdentity     identity.NodeIdentity
	SelfSSH          string            // "root@localhost:22"
	SelfDaemon       int               // daemon port
	SelfLoad         registry.LoadInfo // current self load (from CollectLoad)
	DiscoverFn       DiscoveryFunc     // mDNS or nil
	MaxCacheAge      time.Duration     // default 2m
	RequiredMemBytes int64             // estimated memory requirement for this job (0 = no check)
	RetryInterval    time.Duration     // how long to wait between queue retries (default: 30s)
	NoSelf           bool              // exclude self-node from placement
	Strategy         string            // "smart" (default) or "round-robin"
	RoundRobinFn     func(count int) int // external round-robin counter (e.g. daemon endpoint), nil = in-memory
}

// LeaseResult is the outcome of a successful placement with lease.
type LeaseResult struct {
	Decision   PlacementDecision
	LeaseID    string
	ExpiresAt  string
	NodeID     string
	Endpoint   string // SSH endpoint to use
	DaemonPort int    // daemon port for lease API calls
}

// New creates a coordinator.
func New(cfg Config) *Coordinator {
	if cfg.MaxCacheAge == 0 {
		cfg.MaxCacheAge = 2 * time.Minute
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 30 * time.Second
	}
	return &Coordinator{
		registryURL:      cfg.RegistryURL,
		httpClient:       &http.Client{Timeout: 5 * time.Second},
		bus:              cfg.Bus,
		selfIdentity:     cfg.SelfIdentity,
		selfSSH:          cfg.SelfSSH,
		selfDaemon:       cfg.SelfDaemon,
		selfLoad:         cfg.SelfLoad,
		discoverFn:       cfg.DiscoverFn,
		maxCacheAge:      cfg.MaxCacheAge,
		requiredMemBytes: cfg.RequiredMemBytes,
		retryInterval:    cfg.RetryInterval,
		noSelf:           cfg.NoSelf,
		strategy:         cfg.Strategy,
		roundRobinFn:     cfg.RoundRobinFn,
	}
}

// FindNode selects the best node for a task.
// Called per-task — always fetches fresh data.
func (c *Coordinator) FindNode() PlacementDecision {
	var candidates []ScoredNode
	degraded := false

	// 1. Query daemon for all known nodes (single source of truth)
	resp, err := c.httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", c.selfDaemon))
	if err == nil {
		defer resp.Body.Close()
		var nodes []registry.NodeRecord
		if json.NewDecoder(resp.Body).Decode(&nodes) == nil {
			for _, n := range nodes {
				candidates = append(candidates, ScoredNode{Node: n, Source: SourceDiscovery})
			}
		}
	} else {
		degraded = true
	}

	// 4. Inject self-node (unless --no-self)
	if !c.noSelf {
		selfNode := c.buildSelfNode()
		candidates = mergeNode(candidates, ScoredNode{Node: selfNode, Source: SourceSelf})
	}

	// 4. Arch compatibility filter — closure is built for local arch
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		nodeArch, ok := candidates[i].Node.Capabilities["arch"]
		if ok && nodeArch != "" && nodeArch != c.selfIdentity.Arch {
			candidates[i].Rejected = true
			candidates[i].RejectReason = fmt.Sprintf(
				"arch mismatch: closure %s, node %s", c.selfIdentity.Arch, nodeArch)
		}
	}

	// 5. Resource filter — reject nodes with insufficient memory
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		if c.requiredMemBytes > 0 && candidates[i].Node.Load.AvailableMemBytes > 0 {
			if candidates[i].Node.Load.AvailableMemBytes < c.requiredMemBytes {
				candidates[i].Rejected = true
				candidates[i].RejectReason = fmt.Sprintf(
					"insufficient memory: need %d, available %d",
					c.requiredMemBytes, candidates[i].Node.Load.AvailableMemBytes)
			}
		}
	}

	// 6. Pick node — separate codepaths for deterministic round-robin vs scored smart
	var chosen registry.NodeRecord
	chosenSource := SourceSelf
	reason := "no candidates available"

	if c.strategy == "round-robin" {
		// Collect non-rejected candidates, sort by NodeID for deterministic order
		var eligible []*ScoredNode
		for i := range candidates {
			if !candidates[i].Rejected {
				eligible = append(eligible, &candidates[i])
			}
		}
		sort.Slice(eligible, func(i, j int) bool {
			return eligible[i].Node.NodeID < eligible[j].Node.NodeID
		})

		if len(eligible) > 0 {
			var idx int
			if c.roundRobinFn != nil {
				idx = c.roundRobinFn(len(eligible)) % len(eligible)
			} else {
				c.mu.Lock()
				idx = c.rrCounter % len(eligible)
				c.rrCounter++
				c.mu.Unlock()
			}
			chosen = eligible[idx].Node
			chosenSource = eligible[idx].Source
			reason = fmt.Sprintf("round-robin #%d/%d, source=%s", idx+1, len(eligible), chosenSource)
		}
	} else {
		// Score all non-rejected, sort by score, pick best
		for i := range candidates {
			if !candidates[i].Rejected {
				candidates[i].Score = scoreNode(candidates[i].Node)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Score > candidates[j].Score
		})
		for _, sn := range candidates {
			if !sn.Rejected {
				chosen = sn.Node
				chosenSource = sn.Source
				reason = fmt.Sprintf("score=%.3f, source=%s", sn.Score, sn.Source)
				break
			}
		}
	}

	return PlacementDecision{
		ChosenNode:   chosen,
		Source:       chosenSource,
		DegradedMode: degraded,
		Candidates:   candidates,
		Reason:       reason,
	}
}

// PlaceWithRetry attempts to place a task on a node with a lease.
// If no node can accept, it queues the task and retries every RetryInterval
// until placement succeeds or ctx is cancelled.
// Returns the lease result including the lease_id and daemon endpoint.
func (c *Coordinator) PlaceWithRetry(ctx context.Context, cfg Config, statusFn func(string)) (*LeaseResult, error) {
	retryInterval := cfg.RetryInterval
	if retryInterval == 0 {
		retryInterval = 30 * time.Second
	}
	submitterNode := cfg.SelfIdentity.NodeID

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if statusFn != nil {
				statusFn(fmt.Sprintf("Queued — retrying in %s (attempt %d)...", retryInterval, attempt+1))
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("placement cancelled: %w", ctx.Err())
			case <-time.After(retryInterval):
			}
		}

		decision := c.FindNode()

		// Round-robin: try the chosen node, no fallback to other candidates
		if c.strategy == "round-robin" {
			if decision.ChosenNode.NodeID != "" {
				result, ok := c.tryCandidate(&decision.ChosenNode, &decision, submitterNode, cfg.RequiredMemBytes, statusFn)
				if ok {
					return result, nil
				}
			}
			// Chosen node rejected — let ExecuteWithRetry handle reschedule on next attempt
			if statusFn != nil {
				statusFn(fmt.Sprintf("Round-robin node %s rejected — queued (attempt %d)", decision.ChosenNode.NodeID[:min(8, len(decision.ChosenNode.NodeID))], attempt+1))
			}
		} else {
			// Smart: try candidates in score order
			for i := range decision.Candidates {
				cand := &decision.Candidates[i]
				result, ok := c.tryCandidate(&cand.Node, &decision, submitterNode, cfg.RequiredMemBytes, statusFn)
				if ok {
					return result, nil
				}
			}

			// No node accepted — will retry (unless context cancelled)
			if statusFn != nil {
				statusFn(fmt.Sprintf("No node has capacity — task queued (attempt %d)", attempt+1))
			}
		}
	}
}

// tryCandidate attempts to place a task on a specific node.
// Returns the lease result and true if successful.
func (c *Coordinator) tryCandidate(node *registry.NodeRecord, decision *PlacementDecision, submitterNode string, requiredMemBytes int64, statusFn func(string)) (*LeaseResult, bool) {
	if node.NodeID == "" {
		return nil, false
	}

	// Resolve real UUID + load via health endpoint (needed for manual peers)
	host := extractHost(node.SSHEndpoint)
	healthURL := fmt.Sprintf("http://%s:%d/health", host, node.DaemonPort)
	resp, err := c.httpClient.Get(healthURL)
	if err == nil {
		var h struct {
			NodeID       string `json:"node_id"`
			AvailableMem int64  `json:"available_mem"`
			ActiveJobs   int    `json:"active_jobs"`
			ReservedMem  int64  `json:"reserved_mem"`
		}
		if json.NewDecoder(resp.Body).Decode(&h) == nil && h.NodeID != "" {
			node.NodeID = h.NodeID
			node.Load.AvailableMemBytes = h.AvailableMem
			node.Load.ActiveJobs = h.ActiveJobs
			node.Load.ReservedMemBytes = h.ReservedMem
			node.HealthScore = 0.8
		}
		resp.Body.Close()
	}

	endpoint := c.resolveEndpointForNode(*node)
	daemonURL := fmt.Sprintf("http://%s:%d", extractHost(endpoint), node.DaemonPort)

	leaseID, expiresAt, err := c.requestLease(daemonURL, node.NodeID, submitterNode, requiredMemBytes)
	if err != nil {
		if statusFn != nil {
			statusFn(fmt.Sprintf("Node %s rejected: %v", node.NodeID, err))
		}
		return nil, false
	}

	return &LeaseResult{
		Decision:   *decision,
		LeaseID:    leaseID,
		ExpiresAt:  expiresAt,
		NodeID:     node.NodeID,
		Endpoint:   endpoint,
		DaemonPort: node.DaemonPort,
	}, true
}

// requestLease sends an accept request to a daemon via NATS (preferred) or HTTP (fallback).
func (c *Coordinator) requestLease(daemonURL, targetID, submitterNode string, memBytes int64) (string, string, error) {
	req := struct {
		TargetID         string `json:"target_id"`
		SubmitterNode    string `json:"submitter_node"`
		RequiredMemBytes int64  `json:"required_mem_bytes,omitempty"`
	}{targetID, submitterNode, memBytes}

	var result struct {
		Accepted  bool   `json:"accepted"`
		LeaseID   string `json:"lease_id"`
		ExpiresAt string `json:"expires_at"`
		Reason    string `json:"reason"`
	}

	// Prefer NATS if available
	if c.bus != nil {
		subject := fmt.Sprintf("pipedpeer.daemon.%s.accept", targetID)
		if err := c.bus.RequestJSON(subject, req, &result, 5*time.Second); err != nil {
			return "", "", fmt.Errorf("nats request: %w", err)
		}
		if !result.Accepted {
			return "", "", fmt.Errorf("rejected: %s", result.Reason)
		}
		return result.LeaseID, result.ExpiresAt, nil
	}

	// Fallback to HTTP
	body := fmt.Sprintf(`{"target_id":%q,"submitter_node":%q,"required_mem_bytes":%d}`,
		targetID, submitterNode, memBytes)
	resp, err := c.httpClient.Post(daemonURL+"/v1/accept", "application/json", strings.NewReader(body))
	if err != nil {
		return "", "", fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode response: %w", err)
	}
	if !result.Accepted {
		return "", "", fmt.Errorf("rejected: %s", result.Reason)
	}
	return result.LeaseID, result.ExpiresAt, nil
}

// CommitLease sends a commit request via NATS (preferred) or HTTP (fallback).
func (c *Coordinator) CommitLease(daemonURL, leaseID string, nodeID ...string) error {
	req := struct {
		LeaseID string `json:"lease_id"`
	}{leaseID}

	var result struct {
		Committed bool   `json:"committed"`
		Reason    string `json:"reason"`
	}

	// Prefer NATS if available and nodeID is provided
	if c.bus != nil && len(nodeID) > 0 && nodeID[0] != "" {
		subject := fmt.Sprintf("pipedpeer.daemon.%s.commit", nodeID[0])
		if err := c.bus.RequestJSON(subject, req, &result, 5*time.Second); err != nil {
			return fmt.Errorf("nats commit: %w", err)
		}
		if !result.Committed {
			return fmt.Errorf("commit rejected: %s", result.Reason)
		}
		return nil
	}

	// Fallback to HTTP
	body := fmt.Sprintf(`{"lease_id":%q}`, leaseID)
	resp, err := c.httpClient.Post(daemonURL+"/v1/commit", "application/json", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if !result.Committed {
		return fmt.Errorf("commit rejected: %s", result.Reason)
	}
	return nil
}

// CompleteLease sends a complete request via NATS (preferred) or HTTP (fallback).
func (c *Coordinator) CompleteLease(daemonURL, leaseID, status string, nodeID ...string) error {
	req := struct {
		LeaseID string `json:"lease_id"`
		Status  string `json:"status"`
	}{leaseID, status}

	if c.bus != nil && len(nodeID) > 0 && nodeID[0] != "" {
		// Use node-specific subject for targeted delivery
		subject := fmt.Sprintf("pipedpeer.daemon.%s.complete", nodeID[0])
		var resp map[string]string
		return c.bus.RequestJSON(subject, req, &resp, 5*time.Second)
	}

	body := fmt.Sprintf(`{"lease_id":%q,"status":%q}`, leaseID, status)
	resp, err := c.httpClient.Post(daemonURL+"/v1/complete", "application/json", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	return nil
}

// CancelLease sends a cancel request via NATS or HTTP.
func (c *Coordinator) CancelLease(daemonURL, leaseID, submitterNode string, nodeID ...string) error {
	req := struct {
		LeaseID       string `json:"lease_id"`
		SubmitterNode string `json:"submitter_node"`
	}{leaseID, submitterNode}

	if c.bus != nil && len(nodeID) > 0 && nodeID[0] != "" {
		subject := fmt.Sprintf("pipedpeer.daemon.%s.cancel", nodeID[0])
		return c.bus.PublishJSON(subject, req)
	}

	body := fmt.Sprintf(`{"lease_id":%q,"submitter_node":%q}`, leaseID, submitterNode)
	resp, err := c.httpClient.Post(daemonURL+"/v1/cancel", "application/json", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	return nil
}

// ExecutorFunc is a function that executes a task on a placed node.
// It receives the daemon host, port, and target node ID.
// Returns nil on success, error on failure (triggering reschedule).
type ExecutorFunc func(host string, port int, targetNodeID string) error

// StatusFunc is called with human-readable status updates.
type StatusFunc func(msg string)

// ExecuteWithRetry is the full task lifecycle:
//   place → commit → execute → succeed or fail → reschedule if failed.
//
// The loop continues until the executor succeeds or ctx is cancelled (user ^C).
// A failed execution completes the lease as "failed" and retries placement.
// A failed commit (resources changed) retries placement immediately.
// Tasks are NEVER auto-cancelled — only ctx cancellation stops the loop.
func (c *Coordinator) ExecuteWithRetry(ctx context.Context, executor ExecutorFunc, statusFn StatusFunc) (*LeaseResult, error) {
	cfg := c.configSnapshot()

	for attempt := 0; ; attempt++ {
		// Check for cancellation before each attempt
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		default:
		}

		if attempt > 0 && statusFn != nil {
			statusFn(fmt.Sprintf("Rescheduling (attempt %d)...", attempt+1))
		}

		// 1. Place: find a node and get a lease (queues if none available)
		result, err := c.PlaceWithRetry(ctx, cfg, statusFn)
		if err != nil {
			return nil, err // context cancelled
		}

		daemonURL := fmt.Sprintf("http://%s:%d", extractHost(result.Endpoint), result.DaemonPort)

		if statusFn != nil {
			statusFn(fmt.Sprintf("Placed on node %s (lease=%s)", result.NodeID[:min(8, len(result.NodeID))], result.LeaseID[:min(8, len(result.LeaseID))]))
		}

		// 2. Commit the lease
		if err := c.CommitLease(daemonURL, result.LeaseID, result.NodeID); err != nil {
			if statusFn != nil {
				statusFn(fmt.Sprintf("Commit failed on %s: %v — retrying placement", result.NodeID[:min(8, len(result.NodeID))], err))
			}
			// Commit failed (resources changed since accept) — retry placement
			continue
		}

		if statusFn != nil {
			statusFn(fmt.Sprintf("Lease committed — executing on %s", result.NodeID[:min(8, len(result.NodeID))]))
		}

		// 3. Execute
		execErr := executor(extractHost(result.Endpoint), result.DaemonPort, result.NodeID)

		// 4. Complete the lease
		if execErr == nil {
			_ = c.CompleteLease(daemonURL, result.LeaseID, "succeeded", result.NodeID)
			return result, nil // success!
		}

		// Execution failed — complete as failed and reschedule
		_ = c.CompleteLease(daemonURL, result.LeaseID, "failed", result.NodeID)

		if statusFn != nil {
			statusFn(fmt.Sprintf("Execution failed on %s: %v — rescheduling on another node", result.NodeID[:min(8, len(result.NodeID))], execErr))
		}

		// Loop back to placement — never auto-cancel
	}
}


// ResolveEndpoint returns the SSH endpoint to use for the chosen node.
// Uses loopback for self-node to avoid hairpin NAT.
func (c *Coordinator) ResolveEndpoint(chosen registry.NodeRecord) string {
	return c.resolveEndpointForNode(chosen)
}

func (c *Coordinator) resolveEndpointForNode(node registry.NodeRecord) string {
	if node.NodeID == c.selfIdentity.NodeID {
		return c.selfSSH
	}
	return node.SSHEndpoint
}

// extractHost pulls the hostname from "user@host:port" or "host".
func extractHost(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = s[idx+1:]
	}
	if idx := strings.Index(s, ":"); idx >= 0 {
		s = s[:idx]
	}
	return s
}

func (c *Coordinator) buildSelfNode() registry.NodeRecord {
	return registry.NodeRecord{
		NodeID:      c.selfIdentity.NodeID,
		SSHEndpoint: c.selfSSH,
		DaemonPort:  c.selfDaemon,
		Capabilities: map[string]string{
			"arch":     c.selfIdentity.Arch,
			"hostname": c.selfIdentity.Hostname,
		},
		Load:        c.selfLoad,
		State:       "healthy",
		HealthScore: 1.0,
	}
}

func (c *Coordinator) queryRegistry() ([]registry.NodeRecord, error) {
	resp, err := c.httpClient.Get(c.registryURL + "/v1/nodes?state=healthy")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("registry returned %d", resp.StatusCode)
	}
	var nodes []registry.NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (c *Coordinator) updateCache(nodes []registry.NodeRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cachedNodes = nodes
	c.cacheTime = time.Now()
}

func (c *Coordinator) getCachedNodes() []registry.NodeRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.cacheTime) > c.maxCacheAge {
		return nil // too stale
	}
	return c.cachedNodes
}

// scoreNode is the SHIM scorer — placeholder for the optimizer engine.
// Simple "least loaded" heuristic.
func scoreNode(n registry.NodeRecord) float64 {
	score := 1.0
	score -= n.Load.CPUPercent / 200.0
	score -= n.Load.MemPercent / 200.0
	score -= float64(n.Load.ActiveJobs) * 0.05
	score *= n.HealthScore
	if score < 0 {
		score = 0
	}
	return score
}

// mergeNode adds a node to the list, deduplicating by NodeID.
// Registry source takes priority over discovery/cache.
func mergeNode(list []ScoredNode, newNode ScoredNode) []ScoredNode {
	for i, existing := range list {
		if existing.Node.NodeID == newNode.Node.NodeID {
			// Keep the more authoritative source
			priority := map[NodeSource]int{
				SourceRegistry:  5,
				SourceSelf:      4,
				SourceManual:    3,
				SourceDiscovery: 2,
				SourceCache:     1,
			}
			if priority[newNode.Source] > priority[existing.Source] {
				list[i] = newNode
			}
			return list
		}
	}
	return append(list, newNode)
}

// configSnapshot returns a Config snapshot of the coordinator's current state.
// Used by PlaceWithRetry and tests.
func (c *Coordinator) configSnapshot() Config {
	return Config{
		SelfIdentity:     c.selfIdentity,
		SelfSSH:          c.selfSSH,
		SelfDaemon:       c.selfDaemon,
		SelfLoad:         c.selfLoad,
		RequiredMemBytes: c.requiredMemBytes,
		RetryInterval:    c.retryInterval,
		NoSelf:           c.noSelf,
		Strategy:         c.strategy,
	}
}
