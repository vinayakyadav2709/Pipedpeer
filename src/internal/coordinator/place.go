package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// DefaultPlacementWindow is how long PlaceWithRetry keeps looking for a node
// before it gives up: the same ten minutes a --remote run waits for one.
const DefaultPlacementWindow = 10 * time.Minute

// PlaceWithRetry attempts to place a task on a node with a lease.
// If no node can accept, it queues the task and retries every RetryInterval
// until placement succeeds, the placement window (cfg.PlacementWindow) passes,
// or ctx is cancelled. Past the window it returns a plain error naming why
// each node turned the job down; before, a rescheduled job with no reachable
// node retried every 30 s forever (FIX-REQUEUE).
// Returns the lease result including the lease_id and daemon endpoint.
func (c *Coordinator) PlaceWithRetry(ctx context.Context, cfg Config, statusFn func(string)) (*LeaseResult, error) {
	retryInterval := cfg.RetryInterval
	if retryInterval == 0 {
		retryInterval = 30 * time.Second
	}
	window := cfg.PlacementWindow
	if window == 0 {
		window = DefaultPlacementWindow
	}
	deadline := time.Now().Add(window)
	submitterNode := cfg.SelfIdentity.NodeID
	var last PlacementDecision

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			left := time.Until(deadline)
			if left <= 0 {
				return nil, fmt.Errorf("no node took this job within %s (%s)", window, rejectionLine(last))
			}
			wait := min(retryInterval, left)
			if statusFn != nil {
				statusFn(fmt.Sprintf("Queued — retrying in %s (attempt %d)...", wait.Round(time.Second), attempt+1))
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("placement cancelled: %w", ctx.Err())
			case <-time.After(wait):
			}
		}

		decision := c.FindNode()
		last = decision
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

// requestLease sends an accept request to a daemon.
func (c *Coordinator) requestLease(daemonURL string, req acceptReq) (string, string, int, error) {
	var result acceptResp
	result.GPUIndex = -1

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

// CommitLease sends a commit request. nodeID is accepted and unused (it
// addressed the removed NATS transport); callers still pass it.
func (c *Coordinator) CommitLease(daemonURL, leaseID string, nodeID ...string) error {
	var result struct {
		Committed bool   `json:"committed"`
		Reason    string `json:"reason"`
	}

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

// CompleteLease sends a complete request (status succeeded, failed or
// cancelled; a cancel goes this way too). nodeID is accepted and unused.
func (c *Coordinator) CompleteLease(daemonURL, leaseID, status string, nodeID ...string) error {
	body := fmt.Sprintf(`{"lease_id":%q,"status":%q}`, leaseID, status)
	resp, err := c.httpClient.Post(daemonURL+"/v1/complete", "application/json", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()
	return nil
}
