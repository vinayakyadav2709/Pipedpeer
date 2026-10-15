package coordinator

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// FindNode selects the best node for a task.
// Called per-task — always fetches fresh data.
func (c *Coordinator) FindNode() PlacementDecision {
	var candidates []ScoredNode

	// 1. The local daemon's node store is the primary source: it is long-lived,
	//    so it has been discovering and health-checking peers in the background
	//    while this short-lived CLI process did not exist.
	for _, n := range c.queryDaemonNodes() {
		if n.State == "healthy" {
			candidates = mergeNode(candidates, ScoredNode{Node: n.NodeRecord, Source: nodeSource(n.Source)})
		}
	}

	// 2. Extra candidates from the caller (tests hand in a cluster here).
	if c.discoverFn != nil {
		for _, n := range c.discoverFn() {
			candidates = mergeNode(candidates, ScoredNode{Node: n, Source: SourceDiscovery})
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
	if (c.noSelf && c.selfIdentity.NodeID != "") || c.excludeMachine != "" {
		filtered := candidates[:0]
		for _, cnd := range candidates {
			if c.noSelf && cnd.Node.NodeID == c.selfIdentity.NodeID {
				continue
			}
			if heartbeat.SameMachine(cnd.Node.Capabilities[heartbeat.MachineCapability], c.excludeMachine) {
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

	// 4d. A node with a role (pool-only, introducer-only) takes no jobs.
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		if role := candidates[i].Node.Capabilities[heartbeat.RoleCapability]; role != "" {
			candidates[i].Rejected = true
			candidates[i].RejectReason = roleReason(role)
		}
	}

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
	var idleShort []int // memory was the only reason, and the node runs nothing
	for i := range candidates {
		if candidates[i].Rejected {
			continue
		}
		n := candidates[i].Node

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

		if c.requiredMemBytes > 0 && n.Load.AvailableMemBytes > 0 &&
			n.Load.AvailableMemBytes < c.requiredMemBytes {
			candidates[i].Rejected = true
			candidates[i].RejectReason = fmt.Sprintf(
				"insufficient memory: need %s, available %s",
				humanize.IBytes(uint64(c.requiredMemBytes)), humanize.IBytes(uint64(n.Load.AvailableMemBytes)))
			if n.Load.ActiveJobs == 0 {
				idleShort = append(idleShort, i)
			}
		}
	}
	// The memory need is an estimate. When it fits nowhere, an idle node
	// takes the job instead of it waiting for memory that an idle node will
	// not free (W10-EST); the job's memory cap still catches a real overrun.
	if len(idleShort) > 0 && !slices.ContainsFunc(candidates, func(s ScoredNode) bool { return !s.Rejected }) {
		for _, i := range idleShort {
			n := candidates[i].Node
			if resourceest.Admit(c.requiredMemBytes, n.Load.AvailableMemBytes, true, "node "+n.NodeID) {
				candidates[i].Rejected, candidates[i].RejectReason = false, ""
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
		ChosenNode: chosen,
		Source:     chosenSource,
		Candidates: candidates,
		Reason:     reason,
	}
}

// roleReason is why a node with a role is never given a job.
func roleReason(role string) string {
	return "takes no jobs (" + role + " node)"
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
		if cand.Rejected && (strings.HasPrefix(cand.RejectReason, "arch mismatch") || c.isRefused(n.NodeID) ||
			n.Capabilities[heartbeat.RoleCapability] != "") {
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
