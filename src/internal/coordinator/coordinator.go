package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// NodeSource describes where candidate nodes came from.
type NodeSource string

const (
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
	ChosenNode registry.NodeRecord `json:"chosen_node"`
	Source     NodeSource          `json:"source"`
	// DegradedMode is always false since the registry went; kept because
	// run records and their readers carry it.
	DegradedMode bool         `json:"degraded_mode"`
	Candidates   []ScoredNode `json:"candidates"`
	Reason       string       `json:"reason"`
}

// DiscoveryFunc supplies extra candidate nodes per FindNode call. The CLI
// leaves it nil (the local daemon's /v1/nodes is the source); tests use it to
// hand the coordinator a cluster.
type DiscoveryFunc func() []registry.NodeRecord

// Coordinator handles node selection and placement.
type Coordinator struct {
	httpClient       *http.Client
	selfIdentity     identity.NodeIdentity
	selfSSH          string
	selfDaemon       int
	selfLoad         registry.LoadInfo
	discoverFn       DiscoveryFunc
	jobName          string
	requiredMemBytes int64
	requiredCores    int
	requiredGPUMem   int64
	retryInterval    time.Duration
	placementWindow  time.Duration
	execBackoffBase  time.Duration
	noSelf           bool
	excludeMachine   string
	requireGPU       bool
	preferGPU        bool
	strategy         string
	roundRobinFn     func(count int) int // external round-robin counter, nil = in-memory

	mu          sync.Mutex
	onPlacement func(*LeaseResult)
	rrCounter   int // round-robin counter
	// refused maps a node that refused this job's closure to its fix line:
	// it would refuse every retry, so placement skips it from then on.
	refused map[string]string
	// tried counts executions per node, so a retry goes to a node not yet
	// tried when there is one.
	tried map[string]int

	selfCeilingOnce  sync.Once
	selfCeilingBytes int64
	selfRole         string // the local daemon's heartbeat.Role, from /health
}

// Config for the coordinator.
type Config struct {
	SelfIdentity     identity.NodeIdentity
	SelfSSH          string              // "root@localhost:22"
	SelfDaemon       int                 // daemon port
	SelfLoad         registry.LoadInfo   // current self load (from CollectLoad)
	DiscoverFn       DiscoveryFunc       // extra candidates (tests); nil in the CLI
	JobName          string              // optional job name, surfaced in the cluster task view
	RequiredMemBytes int64               // estimated memory requirement for this job (0 = no check)
	RequiredCores    int                 // minimum free CPU cores the node must have (0 = no check)
	RequiredGPUMem   int64               // minimum free VRAM on a single GPU (0 = no check)
	RetryInterval    time.Duration       // how long to wait between queue retries (default: 30s)
	PlacementWindow  time.Duration       // how long PlaceWithRetry keeps retrying before it fails (default: DefaultPlacementWindow)
	ExecBackoffBase  time.Duration       // first wait after a failed execution, doubling per failure (default: 2s)
	NoSelf           bool                // exclude self-node from placement
	ExcludeMachine   string              // run --offload: drop every node with this machine key, a second daemon here included
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
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 30 * time.Second
	}
	if cfg.PlacementWindow == 0 {
		cfg.PlacementWindow = DefaultPlacementWindow
	}
	return &Coordinator{
		httpClient:       &http.Client{Timeout: 5 * time.Second},
		selfIdentity:     cfg.SelfIdentity,
		selfSSH:          cfg.SelfSSH,
		selfDaemon:       cfg.SelfDaemon,
		selfLoad:         cfg.SelfLoad,
		discoverFn:       cfg.DiscoverFn,
		jobName:          cfg.JobName,
		requiredMemBytes: cfg.RequiredMemBytes,
		requiredCores:    cfg.RequiredCores,
		requiredGPUMem:   cfg.RequiredGPUMem,
		retryInterval:    cfg.RetryInterval,
		placementWindow:  cfg.PlacementWindow,
		execBackoffBase:  cfg.ExecBackoffBase,
		noSelf:           cfg.NoSelf,
		excludeMachine:   cfg.ExcludeMachine,
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
