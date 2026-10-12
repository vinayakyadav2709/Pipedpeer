package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
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
	DegradedMode bool                `json:"degraded_mode"`
	Candidates   []ScoredNode        `json:"candidates"`
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
	jobName          string
	requiredMemBytes int64
	requiredCores    int
	requiredGPUMem   int64
	retryInterval    time.Duration
	execBackoffBase  time.Duration
	noSelf           bool
	requireGPU       bool
	preferGPU        bool
	strategy         string
	roundRobinFn     func(count int) int // external round-robin counter, nil = in-memory

	mu          sync.Mutex
	onPlacement func(*LeaseResult)
	cachedNodes []registry.NodeRecord
	cacheTime   time.Time
	rrCounter   int // round-robin counter
	// refused maps a node that refused this job's closure to its fix line:
	// it would refuse every retry, so placement skips it from then on.
	refused map[string]string
	// tried counts executions per node, so a retry goes to a node not yet
	// tried when there is one.
	tried map[string]int

	selfCeilingOnce  sync.Once
	selfCeilingBytes int64
}

// Config for the coordinator.
type Config struct {
	RegistryURL      string       // empty = no registry
	Bus              *natsbus.Bus // optional: NATS bus for inter-node transport (nil = HTTP only)
	SelfIdentity     identity.NodeIdentity
	SelfSSH          string              // "root@localhost:22"
	SelfDaemon       int                 // daemon port
	SelfLoad         registry.LoadInfo   // current self load (from CollectLoad)
	DiscoverFn       DiscoveryFunc       // mDNS or nil
	MaxCacheAge      time.Duration       // default 2m
	JobName          string              // optional job name, surfaced in the cluster task view
	RequiredMemBytes int64               // estimated memory requirement for this job (0 = no check)
	RequiredCores    int                 // minimum free CPU cores the node must have (0 = no check)
	RequiredGPUMem   int64               // minimum free VRAM on a single GPU (0 = no check)
	RetryInterval    time.Duration       // how long to wait between queue retries (default: 30s)
	ExecBackoffBase  time.Duration       // first wait after a failed execution, doubling per failure (default: 2s)
	NoSelf           bool                // exclude self-node from placement
	RequireGPU       bool                // only GPU-capable nodes are eligible
	PreferGPU        bool                // try GPU nodes first, fall back to CPU nodes
	Strategy         string              // "smart" (default) or "round-robin"
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
	GPUIndex   int    // GPU device index reserved by the node (-1 = none)
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
		jobName:          cfg.JobName,
		requiredMemBytes: cfg.RequiredMemBytes,
		requiredCores:    cfg.RequiredCores,
		requiredGPUMem:   cfg.RequiredGPUMem,
		retryInterval:    cfg.RetryInterval,
		execBackoffBase:  cfg.ExecBackoffBase,
		noSelf:           cfg.NoSelf,
		requireGPU:       cfg.RequireGPU,
		preferGPU:        cfg.PreferGPU,
		strategy:         cfg.Strategy,
		roundRobinFn:     cfg.RoundRobinFn,
	}
}

// WaitProgressEvery is how often FindNodeWithin reports while it waits.
var WaitProgressEvery = 30 * time.Second

// FindNodeWithin looks for a node repeatedly until one is chosen, the window
// passes, or ctx ends, and returns the last decision either way.
//
// One look is not enough straight after a daemon starts or a peer joins: the
// health poller and discovery have not caught up, and the cluster looks empty
// for a few seconds although it is not. Nor when every node is busy: the
// window is minutes, so waiting is called on the first miss (waited 0) and
// every WaitProgressEvery after, with the decision that says why each node
// is busy.
func (c *Coordinator) FindNodeWithin(ctx context.Context, window, interval time.Duration, waiting func(d PlacementDecision, waited time.Duration)) PlacementDecision {
	start := time.Now()
	deadline := start.Add(window)
	nextReport := start
	decision := c.FindNode()
	for decision.ChosenNode.NodeID == "" && time.Now().Before(deadline) {
		if waiting != nil && !time.Now().Before(nextReport) {
			waiting(decision, time.Since(start).Truncate(time.Second))
			nextReport = time.Now().Add(WaitProgressEvery)
		}
		select {
		case <-ctx.Done():
			return decision
		case <-time.After(interval):
		}
		decision = c.FindNode()
	}
	return decision
}

// RejectionSummary lists why each candidate was turned down, one per line,
// for an error message that would otherwise only say "no eligible node".
func (d PlacementDecision) RejectionSummary() string {
	var b strings.Builder
	for _, cand := range d.Candidates {
		if !cand.Rejected {
			continue
		}
		name, id := nodeName(cand.Node), cand.Node.NodeID[:min(8, len(cand.Node.NodeID))]
		if name != id {
			name += " (" + id + ")"
		}
		fmt.Fprintf(&b, "  %s: %s\n", name, cand.RejectReason)
	}
	if b.Len() == 0 {
		// Without this line the caller printed a bare "nodes turned down:".
		return "  none: no other node has reported in (check `pipedpeer nodes`)\n"
	}
	return b.String()
}

// FindNode selects the best node for a task.
// Called per-task — always fetches fresh data.
func (c *Coordinator) FindNode() PlacementDecision {
	var candidates []ScoredNode
	degraded := false

	// 1. The local daemon's node store is the primary source: it is long-lived,
	//    so it has been discovering and health-checking peers in the background
	//    while this short-lived CLI process did not exist.
	for _, n := range c.queryDaemonNodes() {
		if n.State == "healthy" {
			candidates = mergeNode(candidates, ScoredNode{Node: n.NodeRecord, Source: nodeSource(n.Source)})
		}
	}

	// 2. Direct LAN discovery, for the cold-start case where the daemon has
	//    only just come up and its first poll has not landed yet.
	if c.discoverFn != nil {
		for _, n := range c.discoverFn() {
			candidates = mergeNode(candidates, ScoredNode{Node: n, Source: SourceDiscovery})
		}
	}

	// 3. Registry, when one is configured. If it is unreachable we are in
	//    degraded mode and fall back to the last known good response.
	if c.registryURL != "" {
		regNodes, err := c.queryRegistry()
		if err == nil {
			c.updateCache(regNodes)
			for _, n := range regNodes {
				candidates = mergeNode(candidates, ScoredNode{Node: n, Source: SourceRegistry})
			}
		} else {
			degraded = true
			for _, n := range c.getCachedNodes() {
				candidates = mergeNode(candidates, ScoredNode{Node: n, Source: SourceCache})
			}
		}
	}

	// 4. Self is always a candidate unless explicitly excluded — a single
	//    machine with no peers must still be able to run its own work.
	if !c.noSelf {
		candidates = mergeNode(candidates, ScoredNode{Node: c.buildSelfNode(), Source: SourceSelf})
	}

	// 4b. --no-self must exclude the local machine regardless of which source it
	// surfaced through. buildSelfNode is one path, but the local daemon also
	// lists itself in /v1/nodes (queryDaemonNodes) and LAN discovery can return
	// it; filter by node id so "distribute to OTHER devices" is honoured.
	if c.noSelf && c.selfIdentity.NodeID != "" {
		filtered := candidates[:0]
		for _, cnd := range candidates {
			if cnd.Node.NodeID == c.selfIdentity.NodeID {
				continue
			}
			filtered = append(filtered, cnd)
		}
		candidates = filtered
	}

	// 4c. A node that refused this job's closure refuses it again.
	c.mu.Lock()
	for i := range candidates {
		if fix, ok := c.refused[candidates[i].Node.NodeID]; ok {
			candidates[i].Rejected = true
			candidates[i].RejectReason = fix
		}
	}
	c.mu.Unlock()

	// 5. Arch compatibility filter — closure is built for local arch
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

	// 6. GPU capability filter — reject nodes without a GPU when one is required
	if c.requireGPU {
		for i := range candidates {
			if candidates[i].Rejected {
				continue
			}
			if !hasGPU(candidates[i].Node) {
				candidates[i].Rejected = true
				candidates[i].RejectReason = "GPU required but node has no GPU"
			}
		}
	}

	// 7. Resource filter — reject nodes that cannot handle the load right now
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		n := candidates[i].Node

		if c.requiredMemBytes > 0 && n.Load.AvailableMemBytes > 0 &&
			n.Load.AvailableMemBytes < c.requiredMemBytes {
			candidates[i].Rejected = true
			candidates[i].RejectReason = fmt.Sprintf(
				"insufficient memory: need %s, available %s",
				humanize.IBytes(uint64(c.requiredMemBytes)), humanize.IBytes(uint64(n.Load.AvailableMemBytes)))
			continue
		}

		if c.requiredCores > 0 {
			if free := freeCores(n); free > 0 && free < float64(c.requiredCores) {
				candidates[i].Rejected = true
				candidates[i].RejectReason = fmt.Sprintf(
					"insufficient cores: need %d, free %.1f", c.requiredCores, free)
				continue
			}
		}

		if c.requiredGPUMem > 0 && len(n.Load.GPUs) > 0 {
			if bestFreeVRAM(n) < c.requiredGPUMem {
				candidates[i].Rejected = true
				candidates[i].RejectReason = fmt.Sprintf(
					"insufficient VRAM: need %s, best free %s",
					humanize.IBytes(uint64(c.requiredGPUMem)), humanize.IBytes(uint64(bestFreeVRAM(n))))
				continue
			}
		}
	}

	// 8. Pick node — separate codepaths for deterministic round-robin vs scored smart
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
				if c.preferGPU || c.requireGPU {
					candidates[i].Score += scoreGPUNode(candidates[i].Node)
				}
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

// RejectionLine is every turned-down node and why, on one line.
func (d PlacementDecision) RejectionLine() string { return rejectionLine(d) }

// nodeName names a node the way its owner knows it.
func nodeName(n registry.NodeRecord) string {
	if h := n.Capabilities["hostname"]; h != "" {
		return h
	}
	return n.NodeID[:min(8, len(n.NodeID))]
}

// rejectionLine is every rejected candidate on one line: "pc1: reason; ...".
func rejectionLine(d PlacementDecision) string {
	var parts []string
	for _, cand := range d.Candidates {
		if cand.Rejected {
			parts = append(parts, nodeName(cand.Node)+": "+cand.RejectReason)
		}
	}
	if len(parts) == 0 {
		return "no node reachable"
	}
	return strings.Join(parts, "; ")
}

// neverFits fails a job no node in the cluster could ever hold, whatever
// frees up: more memory than any node's ceiling, a GPU where none has one,
// or more VRAM than any GPU has. Without it such a job queued forever,
// retrying every 30 s. A node whose ceiling or VRAM is unknown (an older
// build, or this machine) might fit, so it keeps the job queued as before.
func (c *Coordinator) neverFits(d PlacementDecision) error {
	if len(d.Candidates) == 0 {
		return nil
	}
	var why []string
	sized := false // some node is too small, rather than unable to run it at all
	for _, cand := range d.Candidates {
		n := cand.Node
		if cand.Rejected && (strings.HasPrefix(cand.RejectReason, "arch mismatch") || c.isRefused(n.NodeID)) {
			why = append(why, nodeName(n)+": "+cand.RejectReason)
			continue
		}
		switch {
		case c.requireGPU && !hasGPU(n):
			why = append(why, nodeName(n)+" has no GPU")
		case c.requiredMemBytes > 0 && n.Load.MemCeilingBytes > 0 && n.Load.MemCeilingBytes < c.requiredMemBytes:
			why = append(why, fmt.Sprintf("%s allows at most %s", nodeName(n), humanize.IBytes(uint64(n.Load.MemCeilingBytes))))
		case c.requiredGPUMem > 0 && maxVRAM(n) > 0 && maxVRAM(n) < c.requiredGPUMem:
			why = append(why, fmt.Sprintf("%s's largest GPU has %s", nodeName(n), humanize.IBytes(uint64(maxVRAM(n)))))
		default:
			return nil // this one might hold it once something frees up
		}
		sized = true
	}
	var need []string
	if c.requiredMemBytes > 0 {
		need = append(need, humanize.IBytes(uint64(c.requiredMemBytes))+" of memory")
	}
	if c.requireGPU {
		need = append(need, "a GPU")
	}
	if c.requiredGPUMem > 0 {
		need = append(need, humanize.IBytes(uint64(c.requiredGPUMem))+" of VRAM")
	}
	if len(need) == 0 || !sized {
		return Permanent(fmt.Errorf("no node in the cluster can take this job: %s", strings.Join(why, "; ")))
	}
	return Permanent(fmt.Errorf("no node in the cluster can ever hold this job: it needs %s, and %s",
		strings.Join(need, ", "), strings.Join(why, "; ")))
}

// maxVRAM is the node's largest GPU memory in bytes; 0 when unknown.
func maxVRAM(n registry.NodeRecord) int64 {
	best := n.Load.GPUMemBytes
	for _, g := range n.Load.GPUs {
		best = max(best, g.MemoryTotalBytes)
	}
	return best
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
		if err := c.neverFits(decision); err != nil {
			return nil, err
		}

		// Round-robin: try the chosen node, no fallback to other candidates
		if c.strategy == "round-robin" {
			if decision.ChosenNode.NodeID != "" {
				result, ok := c.tryCandidate(&decision.ChosenNode, &decision, submitterNode, cfg, statusFn)
				if ok {
					return result, nil
				}
			}
			// Chosen node rejected — let ExecuteWithRetry handle reschedule on next attempt
			if statusFn != nil {
				if decision.ChosenNode.NodeID == "" {
					statusFn(fmt.Sprintf("No node has room right now (%s) — queued (attempt %d)", rejectionLine(decision), attempt+1))
				} else {
					statusFn(fmt.Sprintf("Round-robin node %s rejected — queued (attempt %d)", nodeName(decision.ChosenNode), attempt+1))
				}
			}
		} else {
			// Smart: try candidates in score order. When GPU is wanted, GPU-capable
			// nodes are tried first; --gpu=prefer then falls back to CPU nodes.
			var gpuGroup, cpuGroup []*ScoredNode
			for i := range decision.Candidates {
				cand := &decision.Candidates[i]
				if cand.Rejected {
					continue
				}
				if hasGPU(cand.Node) {
					gpuGroup = append(gpuGroup, cand)
				} else {
					cpuGroup = append(cpuGroup, cand)
				}
			}
			byScore := func(g []*ScoredNode) {
				sort.SliceStable(g, func(i, j int) bool { return g[i].Score > g[j].Score })
			}
			byScore(gpuGroup)
			byScore(cpuGroup)

			var order []*ScoredNode
			switch {
			case c.requireGPU:
				// GPU only — never fall back to a CPU-only node.
				order = gpuGroup
			case c.preferGPU:
				// GPU first, CPU nodes as fallback.
				order = append(append(order, gpuGroup...), cpuGroup...)
			default:
				// No GPU preference — pure score order.
				order = append(append(order, gpuGroup...), cpuGroup...)
				byScore(order)
			}
			// A retry goes to a node not yet tried, when there is one.
			c.mu.Lock()
			sort.SliceStable(order, func(i, j int) bool { return c.tried[order[i].Node.NodeID] < c.tried[order[j].Node.NodeID] })
			c.mu.Unlock()

			for _, cand := range order {
				result, ok := c.tryCandidate(&cand.Node, &decision, submitterNode, cfg, statusFn)
				if ok {
					return result, nil
				}
			}

			// No node accepted — will retry (unless context cancelled)
			if statusFn != nil {
				why := ""
				if len(order) == 0 {
					why = " (" + rejectionLine(decision) + ")"
				}
				statusFn(fmt.Sprintf("No node has capacity%s — task queued (attempt %d)", why, attempt+1))
			}
		}
	}
}

// tryCandidate attempts to place a task on a specific node.
// Returns the lease result and true if successful.
func (c *Coordinator) tryCandidate(node *registry.NodeRecord, decision *PlacementDecision, submitterNode string, cfg Config, statusFn func(string)) (*LeaseResult, bool) {
	if node.NodeID == "" {
		return nil, false
	}

	// Resolve real UUID + load via health endpoint (needed for manual peers)
	host := extractHost(node.SSHEndpoint)
	healthURL := fmt.Sprintf("http://%s:%d/health", host, node.DaemonPort)
	resp, err := c.httpClient.Get(healthURL)
	if err == nil {
		var h struct {
			NodeID       string             `json:"node_id"`
			AvailableMem int64              `json:"available_mem"`
			ActiveJobs   int                `json:"active_jobs"`
			ReservedMem  int64              `json:"reserved_mem"`
			Capabilities map[string]string  `json:"capabilities"`
			Load         *registry.LoadInfo `json:"load"`
		}
		if json.NewDecoder(resp.Body).Decode(&h) == nil && h.NodeID != "" {
			node.NodeID = h.NodeID
			if h.Load != nil {
				node.Load = *h.Load
			}
			if len(h.Capabilities) > 0 {
				node.Capabilities = h.Capabilities
			}
			node.Load.AvailableMemBytes = h.AvailableMem
			node.Load.ActiveJobs = h.ActiveJobs
			node.Load.ReservedMemBytes = h.ReservedMem
			node.HealthScore = 0.8
		}
		resp.Body.Close()
	}

	endpoint := c.resolveEndpointForNode(*node)
	daemonURL := fmt.Sprintf("http://%s:%d", extractHost(endpoint), node.DaemonPort)

	leaseID, expiresAt, gpuIndex, err := c.requestLease(daemonURL, acceptReq{
		TargetID:         node.NodeID,
		JobName:          cfg.JobName,
		SubmitterNode:    submitterNode,
		RequiredMemBytes: cfg.RequiredMemBytes,
		RequiredCores:    cfg.RequiredCores,
		// Only ask for a GPU reservation on nodes that actually have one, so a
		// --gpu=prefer fallback onto a CPU-only node is not rejected.
		RequireGPU:     (cfg.RequireGPU || cfg.PreferGPU) && hasGPU(*node),
		RequiredGPUMem: cfg.RequiredGPUMem,
	})
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
		GPUIndex:   gpuIndex,
	}, true
}

// acceptReq is the admission request sent to a target daemon's /v1/accept.
// The node replies confirming it actually reserved the resources; placement is
// never assumed to have succeeded without that confirmation.
type acceptReq struct {
	TargetID         string `json:"target_id"`
	JobName          string `json:"job_name,omitempty"`
	SubmitterNode    string `json:"submitter_node"`
	RequiredMemBytes int64  `json:"required_mem_bytes,omitempty"`
	RequiredCores    int    `json:"required_cores,omitempty"`
	RequireGPU       bool   `json:"require_gpu,omitempty"`
	RequiredGPUMem   int64  `json:"required_gpu_mem_bytes,omitempty"`
}

type acceptResp struct {
	Accepted  bool   `json:"accepted"`
	LeaseID   string `json:"lease_id"`
	ExpiresAt string `json:"expires_at"`
	GPUIndex  int    `json:"gpu_index"`
	Reason    string `json:"reason"`
}

// requestLease sends an accept request to a daemon via NATS (preferred) or HTTP (fallback).
func (c *Coordinator) requestLease(daemonURL string, req acceptReq) (string, string, int, error) {
	var result acceptResp
	result.GPUIndex = -1

	// Prefer NATS if available
	if c.bus != nil {
		subject := fmt.Sprintf("pipedpeer.daemon.%s.accept", req.TargetID)
		if err := c.bus.RequestJSON(subject, req, &result, 5*time.Second); err != nil {
			return "", "", -1, fmt.Errorf("nats request: %w", err)
		}
		if !result.Accepted {
			return "", "", -1, fmt.Errorf("rejected: %s", result.Reason)
		}
		return result.LeaseID, result.ExpiresAt, result.GPUIndex, nil
	}

	// Fallback to HTTP
	body, err := json.Marshal(req)
	if err != nil {
		return "", "", -1, fmt.Errorf("encode request: %w", err)
	}
	resp, err := c.httpClient.Post(daemonURL+"/v1/accept", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", "", -1, fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", -1, fmt.Errorf("decode response: %w", err)
	}
	if !result.Accepted {
		return "", "", -1, fmt.Errorf("rejected: %s", result.Reason)
	}
	return result.LeaseID, result.ExpiresAt, result.GPUIndex, nil
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

// RenewLease tells the holding node this submitter is still alive. Without it
// the node cannot tell a long-running job from an orchestrator that crashed,
// and would have to choose between reaping live work or leaking slots forever.
func (c *Coordinator) RenewLease(daemonURL, leaseID string, nodeID ...string) error {
	req := struct {
		LeaseID string `json:"lease_id"`
	}{leaseID}

	if c.bus != nil && len(nodeID) > 0 && nodeID[0] != "" {
		var resp map[string]interface{}
		subject := fmt.Sprintf("pipedpeer.daemon.%s.renew", nodeID[0])
		return c.bus.RequestJSON(subject, req, &resp, 5*time.Second)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Post(daemonURL+"/v1/renew", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("renew rejected: %d", resp.StatusCode)
	}
	return nil
}

// keepLeaseAlive renews the lease periodically until the returned stop function
// is called. Renewals are best-effort: a failed one is not fatal on its own,
// since the node only reaps after several missed intervals.
func (c *Coordinator) keepLeaseAlive(daemonURL, leaseID, nodeID string) (stop func()) {
	done := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = c.RenewLease(daemonURL, leaseID, nodeID)
			}
		}
	}()

	return func() { once.Do(func() { close(done) }) }
}

// leaseRenewInterval is well under the node's running-lease TTL, so a few
// dropped renewals do not cost a live job its slot.
const leaseRenewInterval = 30 * time.Second

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
//
//	place → commit → execute → succeed or fail → reschedule if failed.
//
// A failed execution completes the lease as "failed" and retries placement on
// a node not yet tried where there is one, at most MaxExecAttempts times in
// all; then it returns the last error. A failure that would follow the task
// anywhere (Permanent) returns at once. A node that refused the closure is
// left out from then on without counting as an attempt; the run fails only
// when no node is left. A failed commit (resources changed) retries placement
// immediately. ctx cancellation (user ^C) stops the loop.
func (c *Coordinator) ExecuteWithRetry(ctx context.Context, executor ExecutorFunc, statusFn StatusFunc) (*LeaseResult, error) {
	cfg := c.configSnapshot()
	failures := 0
	var failedOn []string

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
		c.notifyPlacement(result)

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

		// 3. Execute, renewing the lease throughout so the node can tell this
		//    submitter is still alive.
		stopRenew := c.keepLeaseAlive(daemonURL, result.LeaseID, result.NodeID)
		execErr := executor(extractHost(result.Endpoint), result.DaemonPort, result.NodeID)
		stopRenew()

		// 4. Complete the lease
		if execErr == nil {
			_ = c.CompleteLease(daemonURL, result.LeaseID, "succeeded", result.NodeID)
			return result, nil // success!
		}

		// Detached: the job keeps running on the node, which releases the
		// lease when the job ends. No retry.
		if errors.Is(execErr, daemonctl.ErrDetached) {
			return result, execErr
		}

		// Cancelled by the caller: no "failed" lease, no backoff, no retry.
		if ctx.Err() != nil {
			_ = c.CompleteLease(daemonURL, result.LeaseID, "cancelled", result.NodeID)
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		}

		// Execution failed — complete as failed and reschedule
		_ = c.CompleteLease(daemonURL, result.LeaseID, "failed", result.NodeID)
		name := placedName(result)

		// A node that refused the closure refuses it on every retry, but
		// another node may take it: leave this one out and place again.
		if ClosureRefused(execErr) {
			fix := fmt.Sprintf("refused this job's closure (%v); fix: run `pipedpeer trust` on %s", execErr, name)
			c.mu.Lock()
			if c.refused == nil {
				c.refused = map[string]string{}
			}
			c.refused[result.NodeID] = fix
			c.mu.Unlock()
			if statusFn != nil {
				statusFn(fmt.Sprintf("%s %s — placing the job on another node", name, fix))
			}
			continue
		}

		// Unless the failure would follow the task to any node.
		var pe *permanentError
		if errors.As(execErr, &pe) {
			return nil, pe.err
		}

		c.mu.Lock()
		if c.tried == nil {
			c.tried = map[string]int{}
		}
		c.tried[result.NodeID]++
		c.mu.Unlock()
		failedOn = append(failedOn, name)
		failures++
		if failures >= MaxExecAttempts {
			return nil, fmt.Errorf("gave up after %d failed attempts (on %s); last error: %w",
				failures, strings.Join(failedOn, ", "), execErr)
		}

		// Back off before trying again. Some failures are not the node's fault
		// (a full disk, a script that always crashes) and retrying flat out
		// makes them worse: each attempt re-does the closure build and export.
		delay := execRetryBackoff(c.execBackoffBase, failures)

		if statusFn != nil {
			statusFn(fmt.Sprintf("Execution failed on %s: %v — retrying in %s",
				result.NodeID[:min(8, len(result.NodeID))], execErr, delay))
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		case <-time.After(delay):
		}

		// Loop back to placement
	}
}

// MaxExecAttempts is how many failed executions ExecuteWithRetry takes before
// it gives up with the last error.
const MaxExecAttempts = 3

// ClosureRefused reports whether err is a node refusing the job's closure -
// a key it does not trust, or an archive it cannot read. The string covers an
// older node, whose job import says the same in its own words.
func ClosureRefused(err error) bool {
	var ce *daemonctl.ClosureRefusedError
	return errors.As(err, &ce) || (err != nil && strings.Contains(err.Error(), "lacks a signature by a trusted key"))
}

func (c *Coordinator) isRefused(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.refused[nodeID]
	return ok
}

// placedName names the node a placement chose, the way its owner knows it.
func placedName(r *LeaseResult) string {
	for _, cand := range r.Decision.Candidates {
		if cand.Node.NodeID == r.NodeID {
			return nodeName(cand.Node)
		}
	}
	if r.Decision.ChosenNode.NodeID == r.NodeID {
		return nodeName(r.Decision.ChosenNode)
	}
	return r.NodeID[:min(8, len(r.NodeID))]
}

// permanentError marks an execution failure that rescheduling cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as one that would recur on any node, so ExecuteWithRetry
// returns it instead of placing the task again. A nil err stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// DefaultExecBackoffBase is the first wait after a failed execution.
const DefaultExecBackoffBase = 2 * time.Second

// execRetryBackoff grows the wait after each consecutive execution failure, up
// to a 2-minute ceiling, so a stuck task keeps retrying forever without
// hammering the cluster.
func execRetryBackoff(base time.Duration, consecutiveFailures int) time.Duration {
	const max = 2 * time.Minute
	if base <= 0 {
		base = DefaultExecBackoffBase
	}
	d := base
	for i := 1; i < consecutiveFailures && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// SetOnPlacement registers a callback fired after each successful placement,
// before the task executes. It is how the caller learns node-assigned details
// — currently the reserved GPU index — for the attempt about to run.
func (c *Coordinator) SetOnPlacement(fn func(*LeaseResult)) {
	c.mu.Lock()
	c.onPlacement = fn
	c.mu.Unlock()
}

func (c *Coordinator) notifyPlacement(r *LeaseResult) {
	c.mu.Lock()
	fn := c.onPlacement
	c.mu.Unlock()
	if fn != nil {
		fn(r)
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
	// The CLI's own cgroup says nothing about its daemon's ceiling (a CLI
	// run inside a capped scope would read that scope's): ask the daemon.
	load := c.selfLoad
	load.MemCeilingBytes = c.selfCeiling()
	return registry.NodeRecord{
		NodeID:      c.selfIdentity.NodeID,
		SSHEndpoint: c.selfSSH,
		DaemonPort:  c.selfDaemon,
		Capabilities: map[string]string{
			"arch":     c.selfIdentity.Arch,
			"hostname": c.selfIdentity.Hostname,
		},
		Load:        load,
		State:       "healthy",
		HealthScore: 1.0,
	}
}

// selfCeiling is the most this machine's daemon will grant one job, from its
// /health, read once per run. 0 (unknown: no daemon, or an older one) never
// makes a job "too big for anyone".
func (c *Coordinator) selfCeiling() int64 {
	c.selfCeilingOnce.Do(func() {
		if c.selfDaemon <= 0 {
			return
		}
		url := fmt.Sprintf("http://127.0.0.1:%d/health", c.selfDaemon)
		resp, err := c.httpClient.Get(url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[coordinator] warning: could not ask this machine's daemon for its memory ceiling (%v); treating it as unknown\n", err)
			return
		}
		defer resp.Body.Close()
		var h struct {
			Load *registry.LoadInfo `json:"load"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&h); err != nil || h.Load == nil {
			fmt.Fprintf(os.Stderr, "[coordinator] warning: %s gave no memory ceiling (%v); treating it as unknown\n", url, err)
			return
		}
		c.selfCeilingBytes = h.Load.MemCeilingBytes
	})
	return c.selfCeilingBytes
}

// queryDaemonNodes reads cluster membership from the local daemon. A daemon
// that is not running is not an error: the CLI then falls back to direct
// discovery and self.
func (c *Coordinator) queryDaemonNodes() []storedNode {
	if c.selfDaemon <= 0 {
		return nil
	}
	resp, err := c.httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", c.selfDaemon))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var nodes []storedNode
	if json.NewDecoder(resp.Body).Decode(&nodes) != nil {
		return nil
	}
	return nodes
}

// storedNode is a node record as served by the daemon, which additionally
// reports how it learned about the node.
type storedNode struct {
	registry.NodeRecord
	Source string `json:"source"`
}

// nodeSource maps the store's source string onto a NodeSource for display and
// for merge precedence.
func nodeSource(s string) NodeSource {
	switch {
	case s == "":
		return SourceDiscovery
	case strings.Contains(s, "self"):
		return SourceSelf
	case strings.Contains(s, "manual"):
		return SourceManual
	case strings.Contains(s, "registry"):
		return SourceRegistry
	default:
		return SourceDiscovery
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

// scoreNode ranks a node on current headroom and raw capability.
//
// Utilisation terms (CPU%, mem%, active jobs) push work away from busy nodes;
// capability terms (free cores, core count, clock speed) prefer machines that
// will actually finish the work faster. Everything is scaled by HealthScore so
// a flaky node loses to a healthy one at equal load.
func scoreNode(n registry.NodeRecord) float64 {
	// Headroom: how loaded the node is right now. This dominates — a fast
	// machine that is already saturated is a worse choice than an idle slower
	// one.
	score := 1.0
	score -= n.Load.CPUPercent / 200.0
	score -= n.Load.MemPercent / 200.0
	score -= float64(n.Load.ActiveJobs) * 0.05

	// Raw capability: a smaller modifier that breaks ties between nodes of
	// similar load. Each term is normalised to [0,1] and defaults to 0.5 when
	// the node does not report it, so a node with sparse telemetry lands in
	// the middle instead of being starved of work.
	cores := normalized(float64(totalCores(n)), 32.0)
	mhz := normalized(capFloat(n, "cpu_mhz"), 4000.0)
	freeRAM := normalized(float64(n.Load.AvailableMemBytes)/(1<<30), 32.0)
	freeCoreRatio := 0.5
	if c := totalCores(n); c > 0 {
		freeCoreRatio = freeCores(n) / float64(c)
	}

	// Centred on zero: an average node gets no adjustment either way.
	score += (cores - 0.5) * 0.10
	score += (mhz - 0.5) * 0.06
	score += (freeRAM - 0.5) * 0.10
	score += (freeCoreRatio - 0.5) * 0.10

	score *= n.HealthScore
	if score < 0 {
		score = 0
	}
	return score
}

// normalized maps v onto [0,1] against a saturation point, returning the
// neutral midpoint when the value is missing (non-positive).
func normalized(v, saturate float64) float64 {
	if v <= 0 {
		return 0.5
	}
	return minFloat(v/saturate, 1.0)
}

// scoreGPUNode adds a GPU bonus based on VRAM, device count, compute
// capability and current utilisation. Returns 0 for nodes with no GPU.
func scoreGPUNode(n registry.NodeRecord) float64 {
	if !hasGPU(n) {
		return 0
	}
	score := 0.0

	// Total VRAM, saturating at 32 GiB (+0.3 max).
	if n.Load.GPUMemBytes > 0 {
		vramGB := float64(n.Load.GPUMemBytes) / (1 << 30)
		score += minFloat(vramGB/32.0, 1.0) * 0.3
	}

	// Multi-GPU capacity (+0.1 per extra device).
	if gpuCount := len(n.Load.GPUs); gpuCount > 1 {
		score += float64(gpuCount-1) * 0.1
	}

	// Free VRAM on the best device relative to its total (+0.2 max).
	if free := bestFreeVRAM(n); free > 0 && n.Load.GPUMemBytes > 0 {
		score += minFloat(float64(free)/float64(n.Load.GPUMemBytes), 1.0) * 0.2
	}

	// Compute capability — an sm_90 card beats an sm_61 card at equal VRAM
	// (+0.2 max, saturating at 9.0).
	if cc := capFloat(n, "gpu_compute_cap"); cc > 0 {
		score += minFloat(cc/9.0, 1.0) * 0.2
	}

	// Utilisation penalty (-0.1 max).
	score -= (n.Load.GPUUtilPercent / 100.0) * 0.1

	if score > 0.8 {
		score = 0.8
	}
	if score < 0 {
		score = 0
	}
	return score
}

// hasGPU reports whether a node advertises a usable GPU.
func hasGPU(n registry.NodeRecord) bool {
	if n.Capabilities["gpu"] != "" && n.Capabilities["gpu"] != string("none") {
		return true
	}
	return n.Load.GPUModel != "" || len(n.Load.GPUs) > 0
}

// totalCores returns the node's core count, preferring the static capability
// over the load sample.
func totalCores(n registry.NodeRecord) int {
	if c := capInt(n, "cpu_cores"); c > 0 {
		return c
	}
	return n.Load.TotalCPUs
}

// freeCores estimates cores not currently consumed, from total cores and CPU%.
func freeCores(n registry.NodeRecord) float64 {
	cores := totalCores(n)
	if cores <= 0 {
		return 0
	}
	free := float64(cores) * (1.0 - n.Load.CPUPercent/100.0)
	if free < 0 {
		return 0
	}
	return free
}

// bestFreeVRAM returns the largest free VRAM across the node's GPUs. Falls back
// to the aggregate total-minus-used figure when per-device stats are absent.
func bestFreeVRAM(n registry.NodeRecord) int64 {
	var best int64
	for _, g := range n.Load.GPUs {
		if g.MemoryFreeBytes > best {
			best = g.MemoryFreeBytes
		}
	}
	if best == 0 && n.Load.GPUMemBytes > 0 {
		best = n.Load.GPUMemBytes - n.Load.GPUMemUsedBytes
	}
	if best < 0 {
		return 0
	}
	return best
}

func capInt(n registry.NodeRecord, key string) int {
	v, err := strconv.Atoi(n.Capabilities[key])
	if err != nil {
		return 0
	}
	return v
}

func capFloat(n registry.NodeRecord, key string) float64 {
	v, err := strconv.ParseFloat(n.Capabilities[key], 64)
	if err != nil {
		return 0
	}
	return v
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
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
		JobName:          c.jobName,
		RequiredMemBytes: c.requiredMemBytes,
		RequiredCores:    c.requiredCores,
		RequiredGPUMem:   c.requiredGPUMem,
		RetryInterval:    c.retryInterval,
		ExecBackoffBase:  c.execBackoffBase,
		NoSelf:           c.noSelf,
		RequireGPU:       c.requireGPU,
		PreferGPU:        c.preferGPU,
		Strategy:         c.strategy,
	}
}
