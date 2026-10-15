package daemonapi

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
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
	// JobID is the job running under this lease (set by bindLease when the
	// job starts). While that job lives the sweeper keeps the lease without
	// renewals, so a detached or crashed submitter cannot free a busy slot.
	JobID string `json:"job_id,omitempty"`
}

// Server is the daemon API server that manages job leases and resource reservations.
type Server struct {
	nodeID string
	// recipes maps an uploaded store path to its nixgen.Recipe, for peers of
	// another system (offerRecipes). ponytail: never pruned; a few hundred
	// bytes per environment.
	recipes         sync.Map
	router          chi.Router
	leaseDuration   time.Duration
	gracePeriod     time.Duration
	sweepInterval   time.Duration
	runningLeaseTTL time.Duration

	// nodesState holds the node-store/poller/self-description fields
	// (nodes.go owns it).
	nodesState

	mu     sync.Mutex
	leases map[string]*Lease // lease_id → Lease
	// stats caches the /v1/stats snapshot briefly.
	stats statsCache

	stopSweep chan struct{}

	// Job execution tracking
	jobDir string
	jobsMu sync.Mutex

	lastJobPrune time.Time
	jobs         map[string]*JobRecord

	// Round-robin counter for cross-invocation persistence
	rrCounter atomic.Int64
	ddp       *ddpBoard

	// Maximum concurrent tasks this node will hold (0 = unlimited)
	maxConcurrent atomic.Int64

	// Serial closure fan-out: imports closures on peers one at a time.
	// Only needed when every peer shares one physical host (docker demo rig);
	// real machines import into their own RAM, so default stays parallel.
	serialImports atomic.Bool

	// seeds are this node's closure sends in flight, keyed by seedKey, so a
	// second run of the same environment waits on the send already going
	// instead of starting another one to the same peer.
	seedsMu sync.Mutex
	seeds   map[string]*seed

	// importing counts the closure imports this node is receiving, by store
	// path; /v1/store reports it so no other node sends the same closure.
	importingMu sync.Mutex
	importing   map[string]int

	// Node store (SQLite — single source of truth for all peers)
	store *nodestore.Store

	// narCache content-addresses imported Nix closures (see narcache.go)
	narCache *narCache

	// pool owns the warm worker processes for intercepted pool maps
	// (see pool.go). One persistent closure subprocess per store path.
	pool *poolManager

	// state persists leases + jobs across restarts (see state.go)
	state *state

	// Peer health cache (populated by background poller)
	peersMu     sync.RWMutex
	peerHealths map[string]*PeerHealth // key: "host:port"

	// What daemons sharing this machine have reserved, and when that was
	// last asked. Guarded separately from peerHealths because it is read on
	// the admission path and refreshed by an HTTP call.
	coResMu    sync.Mutex
	coResBytes int64
	coResAt    time.Time

	// When each cluster key is next checked against system nix (see
	// warnUntrustedKeys).
	trustMu   sync.Mutex
	trustNext map[string]time.Time
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

// PeerHealth represents the live state of a peer worker.
type PeerHealth struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	NodeID       string `json:"node_id"`
	Status       string `json:"status"` // "healthy", "unreachable"
	ActiveJobs   int    `json:"active_jobs"`
	AvailableMem int64  `json:"available_mem"`
	Source       string `json:"source"` // "manual", "discovery", "self"
	// TotalCPUs is the peer's core count, used only as the prior for how much
	// pool work to give it before anything has actually been measured there.
	TotalCPUs int `json:"total_cpus"`
	// ReservedMem is what the peer has already promised to jobs of its own.
	// Only meaningful alongside Machine: it is what this daemon must also
	// subtract when the peer turns out to be sharing this machine's RAM.
	ReservedMem int64 `json:"reserved_mem"`
	// Machine identifies the physical machine the peer runs on, so a peer
	// that shares this one can be recognised.
	Machine string `json:"machine"`
	// ClosureFormats is what this peer can receive a closure as. Empty for a
	// peer old enough to predate the question, which is exactly the peer
	// that must keep being sent the old format.
	ClosureFormats string `json:"closure_formats"`
	// GPU is the peer's GPU vendor ("" when it has none), so a CUDA closure
	// is not pushed to a node that cannot use it.
	GPU string `json:"gpu,omitempty"`
	// Role is the peer's heartbeat.RoleCapability ("" = full node). Pool
	// spill skips introducer-only peers; the poller fills it from /health.
	Role string `json:"role,omitempty"`
	// Arch is the peer's nix system ("aarch64-linux"; "" from an old peer).
	// A closure of another system cannot run there, so it is not sent one.
	Arch string `json:"arch,omitempty"`
}

// takesPoolWork reports whether pool spill may send this peer work: every
// node but an introducer-only one.
func (ph *PeerHealth) takesPoolWork() bool {
	return ph.Role != heartbeat.RoleIntroducerOnly
}

// TakesSignedClosures reports whether this peer can receive a closure in the
// form that keeps its signatures.
func (ph *PeerHealth) TakesSignedClosures() bool {
	for _, f := range strings.Split(ph.ClosureFormats, ",") {
		if strings.TrimSpace(f) == narpack.FormatName {
			return true
		}
	}
	return false
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
	Status  string `json:"status"` // "succeeded", "failed" or "cancelled"
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
		ddp:             newDDPBoard(),
		runningLeaseTTL: DefaultRunningLeaseTTL,
		leases:          make(map[string]*Lease),
		stopSweep:       make(chan struct{}),
		jobDir:          defaultJobDir(),
		jobs:            make(map[string]*JobRecord),
		store:           store,
		narCache:        newNarCache(),
		pool:            newPoolManager(),
	}
	s.buildRouter()
	return s
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

// SetSerialClosureImports makes closure fan-out import on peers one at a time.
// Off by default (parallel, correct on separate machines); the docker demo rig
// sets it on because all peers share one host's RAM.
func (s *Server) SetSerialClosureImports(on bool) {
	s.serialImports.Store(on)
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) ListenAndServe(port int) error {
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// Serve TLS and plain HTTP on the same port. The token says who may talk
	// to this daemon but nothing about who may listen, and it travels in the
	// request itself - so on any network the operator does not own, which is
	// the point of running beyond a LAN at all, an observer collects it and
	// becomes a member. Accepting both means a cluster encrypts itself as its
	// nodes are updated, instead of every daemon having to switch at the same
	// moment with anything missed simply going silent.
	cert, certErr := tlsid.EnsureCert()
	if certErr != nil {
		log.Warn().Err(certErr).Msg("no TLS certificate; serving plain HTTP only")
		return (&http.Server{Handler: s.Handler()}).Serve(ln)
	}
	return (&http.Server{Handler: s.Handler()}).Serve(tlsid.NewListener(ln, &cert))
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
				s.ddp.sweep(10 * time.Minute)
				// Finished job dirs accumulate on long-lived daemons; prune
				// them hourly (see PruneCompletedJobDirs for what goes when).
				if time.Since(s.lastJobPrune) > time.Hour {
					s.lastJobPrune = time.Now()
					n, err := s.PruneCompletedJobDirs(JobRetention)
					switch {
					case err != nil:
						log.Warn().Err(err).Int("pruned", n).Msg("could not prune old job dirs")
					case n > 0:
						log.Info().Int("n", n).Msg("pruned stale job dir(s)")
					}
				}
			case <-s.stopSweep:
				return
			}
		}
	}()
}

// JobRetention is how long a job directory with no record (a daemon restarted
// without its state, or a job from an older build) survives after the newest
// file in it was last written.
const JobRetention = 7 * 24 * time.Hour

// FinishedJobRetention is how long a finished job keeps its dir after it
// finished. Long enough for `pipedpeer attach` to collect a detached job's
// output and results; a non-detached submitter fetched its results before the
// job's lease was even released, so its dir is only disk use from then on
// (Finding 25: 1.3 GiB of finished workspaces after one demo day).
const FinishedJobRetention = 24 * time.Hour

// PruneCompletedJobDirs removes job directories (and their in-memory records)
// nobody needs any more:
//   - a job this daemon still runs (or is importing) is never touched, however
//     old its dir;
//   - a finished job goes FinishedJobRetention after it finished;
//   - a dir with no record goes when the newest file anywhere inside it is
//     older than olderThan. The dir's own mtime is not that: appending to
//     output.jsonl does not touch it, so a week-long job lost its dir mid-run
//     (PRUNE-EDGE).
//
// The directory is shared with the submitter's job history and holds the NAR
// cache, so only entries named like a daemon job ID (a UUID) are candidates;
// anything else belongs to someone else.
func (s *Server) PruneCompletedJobDirs(olderThan time.Duration) (int, error) {
	entries, err := os.ReadDir(s.jobDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := uuid.Parse(e.Name()); err != nil {
			continue
		}
		dir := filepath.Join(s.jobDir, e.Name())
		s.jobsMu.Lock()
		job := s.jobs[e.Name()]
		var finished time.Time
		running := false
		if job != nil {
			finished = job.FinishedAt
			running = finished.IsZero() && !jobStatusFinal(job.Status)
		}
		s.jobsMu.Unlock()
		switch {
		case running:
			continue
		case !finished.IsZero():
			if time.Since(finished) < FinishedJobRetention {
				continue
			}
		default:
			newest, err := newestModTime(dir)
			if err != nil {
				log.Warn().Err(err).Str("dir", e.Name()).Msg("cannot read job dir; not pruning it")
				continue
			}
			if !newest.Before(cutoff) {
				continue
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			return removed, err
		}
		s.jobsMu.Lock()
		delete(s.jobs, e.Name())
		s.jobsMu.Unlock()
		jobLogs.Delete(filepath.Join(dir, jobOutputFile))
		removed++
	}
	return removed, nil
}

// jobStatusFinal reports whether a job status is one a job never leaves.
func jobStatusFinal(status string) bool {
	switch status {
	case "done", "failed", "cancelled":
		return true
	}
	return false
}

// newestModTime is the latest modification time of dir or anything under it.
func newestModTime(dir string) (time.Time, error) {
	var newest time.Time
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if t := info.ModTime(); t.After(newest) {
			newest = t
		}
		return nil
	})
	return newest, err
}

// StopSweeper stops the background lease sweeper.
func (s *Server) StopSweeper() {
	select {
	case s.stopSweep <- struct{}{}:
	default:
	}
}

// StopWarmWorkers terminates all warm pool worker processes. Safe to call on
// shutdown; warm workers are opportunistic and will be re-spawned on demand.
func (s *Server) StopWarmWorkers() {
	if s.pool != nil {
		s.pool.stopAll()
	}
}

// EnablePoolSpill lets a node forward pool-map sub-chunks to healthy peers for
// the same store, in addition to running locally. It is opt-in: single-node
// setups and tests leave it off, so behaviour is unchanged by default.
//
// The returned list is ranked best-first: fewer active jobs, then more
// available memory. runChunk fans out to the best nodes first and falls
// through to the next best on failure, so the k best nodes carry the work.
func (s *Server) EnablePoolSpill() {
	// The prior for a peer nobody has measured yet. Replaced by an actual
	// measurement as soon as one part comes back from it, which is why a
	// rough answer here is enough: it only has to be good enough to justify
	// a first dispatch.
	s.pool.coresFn = func(peer string) int {
		s.peersMu.RLock()
		defer s.peersMu.RUnlock()
		for addr, ph := range s.peerHealths {
			if addr == peer {
				return ph.TotalCPUs
			}
		}
		return 0
	}
	s.pool.machineFn = func(peer string) string {
		s.peersMu.RLock()
		defer s.peersMu.RUnlock()
		for _, ph := range s.peerHealths {
			if fmt.Sprintf("%s:%d", ph.Host, ph.Port) == peer {
				return ph.Machine
			}
		}
		return ""
	}
	s.pool.SetPeerFn(func(storePath, submitter string, need http.Header) []string {
		s.peersMu.RLock()
		defer s.peersMu.RUnlock()
		type cand struct {
			peer       string
			activeJobs int
			availMem   int64
		}
		var out []cand
		for _, ph := range s.peerHealths {
			if ph.Status != "healthy" {
				continue
			}
			if ph.Host == "" || ph.Port == 0 || !ph.takesPoolWork() {
				continue
			}
			// Only forward to peers that already imported this closure, else
			// the remote POST fails with a missing-store 400. Checking the
			// store endpoint is cheap (local cache) and keeps spill correct.
			if !s.peerHasStore(ph, storePath, need) {
				continue
			}
			out = append(out, cand{peer: fmt.Sprintf("%s:%d", ph.Host, ph.Port), activeJobs: ph.ActiveJobs, availMem: ph.AvailableMem})
		}
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].activeJobs != out[j].activeJobs {
				return out[i].activeJobs < out[j].activeJobs
			}
			return out[i].availMem > out[j].availMem
		})
		res := make([]string, 0, len(out))
		for _, c := range out {
			res = append(res, c.peer)
		}
		return sinkPeerLast(res, submitter)
	})
}
