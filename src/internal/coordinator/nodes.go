package coordinator

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

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
	caps := map[string]string{
		"arch":     c.selfIdentity.Arch,
		"hostname": c.selfIdentity.Hostname,
	}
	// Self replaces the daemon's own /v1/nodes entry (mergeNode), so the
	// daemon's role must ride along: a chunks-only phone takes no jobs.
	if c.selfRole != "" {
		caps[heartbeat.RoleCapability] = c.selfRole
	}
	return registry.NodeRecord{
		NodeID:       c.selfIdentity.NodeID,
		SSHEndpoint:  c.selfSSH,
		DaemonPort:   c.selfDaemon,
		Capabilities: caps,
		Load:         load,
		State:        "healthy",
		HealthScore:  1.0,
	}
}

// selfCeiling is the most this machine's daemon will grant one job, from its
// /health, read once per run. 0 (unknown: no daemon, or an older one) never
// makes a job "too big for anyone". The same read records the daemon's role.
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
			Load         *registry.LoadInfo `json:"load"`
			Capabilities map[string]string  `json:"capabilities"`
		}
		err = json.NewDecoder(resp.Body).Decode(&h)
		c.selfRole = h.Capabilities[heartbeat.RoleCapability]
		if err != nil || h.Load == nil {
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
		fmt.Fprintf(os.Stderr, "[coordinator] warning: cannot ask this machine's daemon for its node list (%v); placing without it\n", err)
		return nil
	}
	defer resp.Body.Close()

	var nodes []storedNode
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		fmt.Fprintf(os.Stderr, "[coordinator] warning: this machine's daemon gave no node list (HTTP %d: %v); placing without it\n", resp.StatusCode, err)
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
	default:
		return SourceDiscovery
	}
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

// mergeNode adds a node to the list, deduplicating by NodeID; the more
// authoritative source wins.
func mergeNode(list []ScoredNode, newNode ScoredNode) []ScoredNode {
	for i, existing := range list {
		if existing.Node.NodeID == newNode.Node.NodeID {
			// Keep the more authoritative source
			priority := map[NodeSource]int{
				SourceSelf:      3,
				SourceManual:    2,
				SourceDiscovery: 1,
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
		PlacementWindow:  c.placementWindow,
		ExecBackoffBase:  c.execBackoffBase,
		NoSelf:           c.noSelf,
		ExcludeMachine:   c.excludeMachine,
		RequireGPU:       c.requireGPU,
		PreferGPU:        c.preferGPU,
		Strategy:         c.strategy,
	}
}
