package daemonapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
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
	nodeID          string
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

// peerHasStore asks a peer whether it can actually run the given closure: the
// store must be materialised (<storePath>/bin/run exists), not merely have its
// NAR cached — a cached but un-imported closure would fail at pool-map time.
// need (phone.Need, may be nil) lets a pool-only phone answer for its host
// python instead of a closure.
func (s *Server) peerHasStore(ph *PeerHealth, storePath string, need http.Header) bool {
	u := fmt.Sprintf("http://%s:%d/v1/store?path=%s&runnable=1", ph.Host, ph.Port, url.QueryEscape(storePath))
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		log.Warn().Err(err).Str("peer", ph.Host).Msg("cannot build the runnable check")
		return false
	}
	for k, v := range need {
		req.Header[k] = v
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var r struct {
		Cached bool `json:"cached"`
	}
	if json.NewDecoder(resp.Body).Decode(&r) != nil {
		return false
	}
	return r.Cached
}

// healthyPeers returns the peers currently known to be up that take pool
// work (introducer-only peers are left out), and how many peers
// the poller has any opinion about at all. The second number distinguishes
// "everyone is down" from "we have not looked yet".
func (s *Server) healthyPeers() ([]PeerHealth, int) {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	var targets []PeerHealth
	for _, ph := range s.peerHealths {
		if ph.Status == "healthy" && ph.Host != "" && ph.Port != 0 && ph.takesPoolWork() {
			targets = append(targets, *ph)
		}
	}
	return targets, len(s.peerHealths)
}

// broadcastClosure pushes a cached closure NAR to every healthy peer that does
// not have it yet, so pool spill can fan a single task out to multiple nodes.
// Best-effort and parallel: a peer that fails to import is simply not offered
// spill work (runChunk always keeps the local part, so nothing is lost).
//
// It returns one line per peer that refused the closure, for the submitter.
// Logged here alone, a refusal left the user with "local 80 items" and no
// reason: the shim only sees that no peer holds the closure.
func (s *Server) broadcastClosure(storePath string) []string {
	targets, known := s.healthyPeers()
	if len(targets) == 0 && known == 0 {
		// The poller has not finished its first cycle. A daemon that has just
		// started still answers uploads, and /v1/nodes serves peer states
		// persisted from a previous run, so both the submitter and the
		// operator can see a healthy cluster while this map is still empty —
		// and the closure then reaches nobody. Poll once, synchronously,
		// rather than silently declining to broadcast.
		s.pollAllNodes()
		targets, known = s.healthyPeers()
	}
	// A pool-only phone runs chunks on its host python and has no Nix to
	// import a closure into.
	targets = slices.DeleteFunc(targets, func(ph PeerHealth) bool { return ph.Role == heartbeat.RolePoolOnly })
	if len(targets) == 0 {
		// Usually a timing problem: the health poller has not run since the
		// peers were added, so they are known but not yet marked healthy.
		log.Warn().Int("known_peers", known).
			Msg("no healthy peers to broadcast the closure to; pool work will stay local")
		return nil
	}
	// A CUDA closure is several GB and only a GPU node can use it. The
	// broadcast is a warm-up for pool spill, and the node a GPU job is placed
	// on gets the closure with the upload, so pushing it to CPU-only peers
	// only costs minutes of transfer and their memory (rehearsal: 260 s and
	// three OOM-killed receivers for a 4 s GPU script).
	if paths, err := closurePaths(storePath); err != nil {
		log.Warn().Err(err).Str("store", storePath).
			Msg("cannot list the closure to tell whether it needs a GPU; broadcasting to every peer")
	} else if closureNeedsGPU(paths) {
		var skipped int
		targets, skipped = gpuPeersOnly(targets)
		if skipped > 0 {
			log.Info().Int("skipped", skipped).Str("store", storePath).
				Msg("not broadcasting a GPU closure to peers without a GPU")
		}
		if len(targets) == 0 {
			return nil
		}
	}
	// Who gets it is settled before anything is packed. Packing first cost
	// every GPU run 3m24s (V2 95): the whole CUDA closure was exported and
	// then sent to nobody, because no peer had a GPU.
	var need []PeerHealth
	for i := range targets {
		if !s.peerHasStore(&targets[i], storePath, nil) {
			need = append(need, targets[i])
		}
	}
	if len(need) == 0 {
		log.Info().Int("peers", len(targets)).Str("store", storePath).
			Msg("every peer already holds the closure; nothing to broadcast")
		return nil
	}
	targets = need
	log.Info().Int("peers", len(targets)).Str("store", storePath).
		Msg("broadcasting closure to peers")

	// The old-format archive, exported from our own store when no upload
	// filled the cache (a job on the machine that built its closure uploads
	// none). Only a peer that cannot take the signed format needs it, so it
	// is made at most once and only when such a peer asks.
	legacy := sync.OnceValues(func() (string, bool) { return localArchive(s.narCache, storePath) })

	var refusedMu sync.Mutex
	var refused []string
	refuse := func(ph *PeerHealth, err error) {
		peer := fmt.Sprintf("%s:%d", ph.Host, ph.Port)
		log.Warn().Err(err).Str("peer", peer).Msg("closure broadcast failed")
		refusedMu.Lock()
		refused = append(refused, fmt.Sprintf("peer %s did not take this environment, so it gets no pool work: %v", peer, err))
		refusedMu.Unlock()
	}

	// Serial mode imports one peer at a time (shared-RAM rig); default parallel.
	if s.serialImports.Load() {
		for i := range targets {
			ph := targets[i]
			if err := s.importStoreOnPeer(&ph, storePath, legacy); err != nil {
				refuse(&ph, err)
			}
		}
		sort.Strings(refused)
		return refused
	}

	var wg sync.WaitGroup
	for i := range targets {
		ph := targets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.importStoreOnPeer(&ph, storePath, legacy); err != nil {
				refuse(&ph, err)
			}
		}()
	}
	wg.Wait()
	sort.Strings(refused)
	return refused
}

// gpuPeersOnly keeps the peers that report a GPU, and says how many it dropped.
func gpuPeersOnly(peers []PeerHealth) ([]PeerHealth, int) {
	var keep []PeerHealth
	for _, ph := range peers {
		if ph.GPU != "" {
			keep = append(keep, ph)
		}
	}
	return keep, len(peers) - len(keep)
}

// localArchive is narCache.ensureLocal, as a variable so a test can see
// whether a broadcast packed anything.
var localArchive = (*narCache).ensureLocal

// importStoreOnPeer sends a closure NAR to a peer's /v1/store/import. legacy
// yields the whole old-format archive, made only if this peer needs it.
// A peer whose own binary-cache fetch lost to us (409) has cancelled it and
// stopped substituting, so one more send carries every path it lacks.
func (srv *Server) importStoreOnPeer(ph *PeerHealth, storePath string, legacy func() (string, bool)) error {
	err := srv.sendClosureOnce(ph, storePath, legacy)
	if errors.Is(err, errFetchSlow) {
		log.Warn().Err(err).Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
			Msg("peer's binary cache fetch was too slow; sending it every missing path")
		err = srv.sendClosureOnce(ph, storePath, legacy)
	}
	return err
}

func (srv *Server) sendClosureOnce(ph *PeerHealth, storePath string, legacy func() (string, bool)) error {
	// Send only what this peer lacks. The whole-closure archive is keyed on
	// the top-level store path, so two environments differing by one package
	// share none of it and every byte of python, numpy and the interpreter
	// tree crosses the wire again. Store paths are content-addressed, which
	// makes them exactly the units it is safe to skip.
	//
	// Best-effort throughout: any failure here falls back to the whole
	// archive, which is correct, just larger.
	partial := false
	signed := false
	var narPath string
	if ph.TakesSignedClosures() {
		// The form that keeps signatures, so the peer can require them
		// instead of being configured to stop asking. It carries its own
		// per-path metadata, so there is no "partial" flag to set: the
		// archive says what it contains.
		if pack, cleanup, ok := buildSignedClosure(srv, ph, storePath); ok {
			defer cleanup()
			narPath, signed = pack, true
		}
	}
	if !signed {
		if diff, ok := srv.diffNARFor(ph, storePath); ok {
			defer os.RemoveAll(filepath.Dir(diff))
			narPath, partial = diff, true
		} else if whole, ok := legacy(); ok {
			narPath = whole
		} else {
			// Nothing to send means this peer can never be offered spill
			// work, and the run would look healthy while staying local.
			return fmt.Errorf("no closure archive for %s: it is in neither this node's store nor its cache", storePath)
		}
	}

	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		if err := mp.WriteField("store_path", storePath); err != nil {
			return
		}
		if partial {
			// The receiver must import this without caching it as the
			// closure, or it would forward a fragment to the next node.
			if err := mp.WriteField("partial", "1"); err != nil {
				return
			}
		}
		if signed {
			// Said as well as sniffed. The receiver decides by looking at
			// the bytes, because that is what stays right when the two ends
			// are different versions; this field is what makes a mismatch
			// legible in a log rather than mysterious.
			if err := mp.WriteField("format", narpack.FormatName); err != nil {
				return
			}
		}
		w, err := mp.CreateFormFile("nar", "closure.nar")
		if err != nil {
			return
		}
		_, _ = io.Copy(w, f)
		mp.Close()
	}()

	url := fmt.Sprintf("http://%s:%d/v1/store/import", ph.Host, ph.Port)
	req, err := http.NewRequest("POST", url, pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mp.FormDataContentType())
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("import %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		// The peer answers {"error": ...}; the bare text reads better on the
		// submitter's terminal than escaped JSON.
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			if resp.StatusCode == http.StatusConflict {
				return fmt.Errorf("%w (peer said: %s)", errFetchSlow, e.Error)
			}
			return fmt.Errorf("import returned %d: %s", resp.StatusCode, e.Error)
		}
		return fmt.Errorf("import %s returned %d: %s", url, resp.StatusCode, string(body))
	}
	return nil
}

// EnablePersistence turns on cross-restart persistence of leases and jobs.
// It is opt-in so the constructor stays side-effect-free for tests; the real
// daemon calls it at startup. On enable it loads any previously saved state.
func (s *Server) EnablePersistence() {
	s.state = newState()
	s.state.load(s.leases, s.jobs)
	s.failOrphanedJobs()
}

// persist snapshots the current leases and jobs to disk. Call after any
// mutation so a crash or restart starts from the latest state.
// persist writes the daemon's leases and jobs to disk.
//
// The maps are copied under the locks that guard them before being handed to
// save, which marshals them. Passing the live maps meant JSON iterating one
// while another request wrote to it, and Go does not treat that as a race to
// be detected later - it kills the process on the spot with "concurrent map
// iteration and map write".
//
// It took two uploads arriving at one daemon at the same moment, which never
// happened while each node hosted a single rank. Giving a node one rank per
// accelerator made it ordinary, and the daemon died mid-run.
func (s *Server) persist() {
	if s.state == nil {
		return
	}
	s.mu.Lock()
	leases := make(map[string]*Lease, len(s.leases))
	for k, v := range s.leases {
		if v != nil {
			copy := *v
			leases[k] = &copy
		}
	}
	s.mu.Unlock()

	s.jobsMu.Lock()
	jobs := make(map[string]*JobRecord, len(s.jobs))
	for k, v := range s.jobs {
		if v != nil {
			copy := *v
			jobs[k] = &copy
		}
	}
	s.jobsMu.Unlock()

	s.state.save(leases, jobs)
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
	// Which bound jobs are still alive, read before s.mu so the two locks are
	// never held together.
	alive := s.liveJobIDs()
	type expired struct{ id, state string }
	var gone []expired
	s.mu.Lock()
	for id, lease := range s.leases {
		switch lease.State {
		case LeaseReserved:
			if now.After(lease.ExpiresAt.Add(s.gracePeriod)) {
				delete(s.leases, id)
				gone = append(gone, expired{id, string(lease.State)})
			}
		case LeaseRunning:
			// A lease follows its job: while the job runs, no renewal is
			// needed (the submitter may have detached or died).
			if lease.JobID != "" && alive[lease.JobID] {
				continue
			}
			last := lease.RenewedAt
			if last.IsZero() {
				last = lease.CreatedAt
			}
			if s.runningLeaseTTL > 0 && now.Sub(last) > s.runningLeaseTTL {
				delete(s.leases, id)
				gone = append(gone, expired{id, string(lease.State)})
			}
		}
	}
	// Released before persisting: persist takes this lock to copy the maps
	// before marshalling them, and holding it here would deadlock against
	// itself.
	s.mu.Unlock()
	s.persist()
	for _, g := range gone {
		log.Warn().Str("lease", g.id).Str("state", g.state).Msg("lease expired")
	}
}

// liveJobIDs is the set of jobs that have not finished yet (uploaded,
// importing or running).
func (s *Server) liveJobIDs() map[string]bool {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	alive := make(map[string]bool)
	for id, j := range s.jobs {
		switch j.Status {
		case "done", "failed", "cancelled":
		default:
			alive[id] = true
		}
	}
	return alive
}

// bindLease ties a lease to the job running under it, so the lease lives
// as long as the job (see sweepExpired) and ends with it (releaseJobLease).
// A lease that already expired (its submitter stopped renewing before the
// job arrived) is taken again, bound to the job, so a running job is never
// invisible to admission; its memory and GPU reservation are gone with it.
func (s *Server) bindLease(leaseID, jobID string) {
	if leaseID == "" {
		return
	}
	s.mu.Lock()
	lease, ok := s.leases[leaseID]
	if ok {
		lease.JobID = jobID
	} else {
		now := time.Now()
		s.leases[leaseID] = &Lease{LeaseID: leaseID, JobID: jobID, State: LeaseRunning, GPUIndex: -1,
			CreatedAt: now, ExpiresAt: now, RenewedAt: now}
	}
	s.mu.Unlock()
	if !ok {
		log.Warn().Str("lease", leaseID).Str("job", jobID).Msg("job names a lease that expired before it started; holding a new one for it with no reservation")
	}
	s.persist()
}

// releaseJobLease frees the lease bound to jobID when the job ends. A lease
// the submitter already completed is simply gone; nothing is logged then.
func (s *Server) releaseJobLease(jobID, status string) {
	if jobID == "" {
		return
	}
	var released []string
	s.mu.Lock()
	for id, l := range s.leases {
		if l.JobID == jobID {
			delete(s.leases, id)
			released = append(released, id)
		}
	}
	s.mu.Unlock()
	if len(released) == 0 {
		return
	}
	s.persist()
	for _, id := range released {
		log.Info().Str("lease", id).Str("status", status).Msg("lease released")
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

// QueueDepth is how many leases are reserved but not yet running: work this
// node has promised to do and has not started.
//
// It has been in the wire format since the registry was written and populated
// nowhere, so every node reported zero and the number told a reader nothing.
// A node with three jobs queued behind the one it is running is a worse
// choice than an idle one with the same core count, and this is what says so.
func (s *Server) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.leases {
		if l.State == LeaseReserved {
			n++
		}
	}
	return n
}

// RecentFailures is how many jobs have failed here lately.
//
// The registry's health score has subtracted 0.1 per recent failure since it
// was written, against a field nothing ever set - so the term was always
// zero and a node that failed every job scored the same as one that failed
// none. Counted over a window, because a node that failed an hour ago and has
// been fine since is not a node to avoid.
func (s *Server) RecentFailures() int {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	cutoff := time.Now().Add(-recentFailureWindow)
	n := 0
	for _, j := range s.jobs {
		if j != nil && j.Status == "failed" && j.CreatedAt.After(cutoff) {
			n++
		}
	}
	return n
}

// recentFailureWindow is how far back a failure still counts against a node.
const recentFailureWindow = 10 * time.Minute

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
// AvailableForJob is how much memory this daemon may promise to a new job.
//
// Free memory is a property of the machine, not of the daemon, so anything
// else on this machine that has already promised memory has to be subtracted
// too. Two daemons on one host each reading the host's free memory and each
// subtracting only their own reservations both conclude they have all of it,
// and both admit work that between them it cannot hold - measured: a forced
// spill drove a 14 GB machine to 194 MB free and the kernel killed its
// desktop shell. A container worker beside its host's daemon is an ordinary
// arrangement, and the lab scripts here build exactly it.
//
// Peers on other machines are not subtracted: their memory is their own.
func (s *Server) AvailableForJob() int64 {
	reserved := s.ReservedMem()
	return s.grantableFrom(heartbeat.CollectLoad(s.ActiveJobs(), reserved), reserved)
}

// availableForOwnJob is AvailableForJob for pool work posted by a job running
// on this node: that job's own lease is the memory the work runs in, so it is
// not counted against it. Measured before (emulated lab, 1 GB nodes): a 600 MB
// job lease left no room for its own 104-byte joblib chunks, so the origin
// forwarded every one away and the peer split parts straight back to it.
func (s *Server) availableForOwnJob() int64 {
	reserved := s.ReservedMem() - s.ownJobLease()
	return s.grantableFrom(heartbeat.CollectLoad(s.ActiveJobs(), reserved), reserved)
}

// ownJobLease is the reservation of the job whose shim is asking. The shim
// names no job, so it is the smallest lease bound to a job running here: exact
// for one job, never more than the asker's own with several.
// ponytail: smallest bound lease; a job id header from the shim makes it exact.
func (s *Server) ownJobLease() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var own int64 = -1
	for _, l := range s.leases {
		if l.JobID != "" && (own < 0 || l.MemBytes < own) {
			own = l.MemBytes
		}
	}
	return max(own, 0)
}

// grantableFrom is AvailableForJob for a load already collected with the
// given reservations, so a caller that needs the load too reads it once.
func (s *Server) grantableFrom(load registry.LoadInfo, reserved int64) int64 {
	avail := load.AvailableMemBytes - s.reservedOnThisMachine()
	if avail < 0 {
		avail = 0
	}

	// Never promise more than this daemon's own share of the machine, even
	// when more happens to be free. Free memory is what the machine has;
	// the budget is what this daemon may take of it, and the difference is
	// the room left for everything that is not pipedpeer.
	//
	// This matters most where the kernel is not enforcing anything. A daemon
	// in a container has no systemd to place it in a scope, and no memory
	// limit at all unless docker was given one - so it ran unbounded beside a
	// host daemon that was correctly capped at half the machine, and the
	// kernel went looking for a victim outside either cgroup. Honouring the
	// budget at admission makes the unpoliced node behave like the policed
	// one.
	if b := cgroups.SelfBudget(); b.Total > 0 {
		// Reservations are promises against this budget too. The free-memory
		// reading above already has them subtracted; leaving them in here
		// mixed two accounting systems, so a daemon whose budget was the
		// tighter bound reported the same headroom however much it had
		// already promised - and admitted work twice over. Caught by a
		// stacked-reservation test that passed on one machine and failed on
		// the other, which is the difference between the two bounds binding.
		r := b.Remaining() - reserved
		if r < 0 {
			r = 0
		}
		if r < avail {
			avail = r
		}
	}
	return avail
}

// reservedOnThisMachine sums what daemons sharing this machine have promised.
//
// Asked directly rather than read from the health poller's cache. The poller
// runs every ten seconds or so, and a chunk that arrives inside that window is
// admitted against a figure from before the reservation it needs to see - the
// two daemons then admit in the same window and the machine goes over anyway.
// Measured with the cached figure: a forced spill still drove the host out of
// memory, though the kernel took a pipedpeer worker rather than the desktop.
// A daemon sharing this machine is reachable over the loopback, so asking is
// cheap; the answer is held briefly so a burst of admissions is one question.
func (s *Server) reservedOnThisMachine() int64 {
	me := heartbeat.Machine()
	if me == "" {
		// Cannot tell which machine this is, so cannot tell who shares it.
		// Claiming co-location on a guess would shrink every node's usable
		// memory for no reason.
		return 0
	}

	s.coResMu.Lock()
	if time.Since(s.coResAt) < coResidentTTL {
		total := s.coResBytes
		s.coResMu.Unlock()
		return total
	}
	s.coResMu.Unlock()

	s.peersMu.RLock()
	var roommates []PeerHealth
	for _, ph := range s.peerHealths {
		if ph.Status == "healthy" && heartbeat.SameMachine(ph.Machine, me) {
			roommates = append(roommates, *ph)
		}
	}
	s.peersMu.RUnlock()

	var total int64
	for _, ph := range roommates {
		if live, ok := peerReservedNow(ph); ok {
			total += live
			continue
		}
		// Unreachable right now: the last known figure is a better guess than
		// zero, which would read as "it has promised nothing".
		total += ph.ReservedMem
	}

	s.coResMu.Lock()
	s.coResBytes, s.coResAt = total, time.Now()
	s.coResMu.Unlock()
	return total
}

// coResidentTTL bounds how often a daemon asks its roommates what they have
// promised. Short enough that a reservation made moments ago is seen, long
// enough that a burst of admissions asks once.
const coResidentTTL = 300 * time.Millisecond

// peerReservedNow asks a co-located daemon what it currently has reserved.
func peerReservedNow(ph PeerHealth) (int64, bool) {
	req, err := http.NewRequest("GET",
		fmt.Sprintf("http://%s/v1/reserved", net.JoinHostPort(ph.Host, strconv.Itoa(ph.Port))), nil)
	if err != nil {
		return 0, false
	}
	if tok := authtoken.Current(); tok != "" {
		req.Header.Set("X-Pipedpeer-Token", tok)
	}
	// Loopback, and on the admission path: a daemon that has stopped
	// answering must not hold up work here.
	client := &http.Client{Timeout: 400 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	var h reservedResponse
	if json.NewDecoder(resp.Body).Decode(&h) != nil {
		return 0, false
	}
	return h.ReservedMem, true
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
	// No-op until a token is configured, so an existing cluster keeps working
	// until its operator opts in. Once one is set, every endpoint here is
	// behind it — including /health, which is how peers decide who exists.
	r.Use(exceptUIPage(authtoken.Middleware))

	r.Get("/ui", s.handleUI)
	r.Get("/v1/stats", s.handleStats)
	r.Get("/health", s.handleHealth)
	r.Post("/v1/accept", s.handleAccept)
	r.Post("/v1/commit", s.handleCommit)
	r.Post("/v1/renew", s.handleRenew)
	r.Post("/v1/complete", s.handleComplete)
	r.Post("/v1/jobs/upload", s.handleJobUpload)
	r.Get("/v1/jobs/{id}/exec", s.handleJobExec)
	r.Get("/v1/jobs/{id}/results", s.handleJobResults)
	r.Get("/v1/jobs/{id}/attach", s.handleJobAttach)
	r.Get("/v1/jobs/{id}/status", s.handleJobStatus)
	r.Get("/v1/roundrobin", s.handleRoundRobin)
	r.Get("/v1/jobs", s.handleJobs)
	r.Get("/v1/nodes", s.handleNodes)
	r.Get("/v1/peers/unreachable", s.handleUnreachable)
	r.Get("/v1/reserved", s.handleReserved)
	r.Get("/v1/store", s.handleStoreCheck)
	r.Post("/v1/store/missing", s.handleStoreMissing)
	r.Post("/v1/store/import", s.handleStoreImport)
	r.Post("/v1/pool/map", s.handlePoolMap)
	r.Get("/v1/pool/stats", s.handlePoolStats)
	r.Get("/v1/bench", s.handleBench)
	r.Post("/v1/ddp/sync", s.handleDDPSync)
	r.Get("/v1/ddp/sync", s.handleDDPSyncStatus)
	r.Post("/v1/ddp/down", s.handleDDPDown)
	r.Post("/v1/ddp/join", s.handleDDPJoin)
	r.Post("/v1/ddp/snapshot", s.handleDDPSnapshotPut)
	r.Get("/v1/ddp/snapshot", s.handleDDPSnapshotGet)
	r.Get("/v1/introducers", s.handleIntroducers)
	r.Get("/v1/addresses", s.handleAddresses)
	r.Get("/v1/history", s.handleHistory)

	s.router = r
}

// handleReserved answers what this daemon has promised, and nothing else.
//
// Separate from /health deliberately. A daemon computing what it can grant
// asks its roommates what they have promised; if that question were answered
// by /health, and /health reported grantable memory, two daemons on one
// machine would each be waiting on the other to answer the same question.
func (s *Server) handleReserved(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, reservedResponse{
		ReservedMem: s.ReservedMem(),
		Machine:     heartbeat.Machine(),
	})
}

type reservedResponse struct {
	ReservedMem int64  `json:"reserved_mem"`
	Machine     string `json:"machine"`
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
	load.QueueDepth = s.QueueDepth()
	load.RecentFailures = s.RecentFailures()
	// What a peer may actually be given, not what the machine happens to have
	// free. A container worker capped at 2 GiB advertising the host's 10.7 GiB
	// is inviting work it will be killed for accepting, and every scheduler
	// that reads this - ring placement, pool peer choice - believes it.
	grantable := s.grantableFrom(load, reserved)
	load.AvailableMemBytes = grantable
	return healthResponse{
		Status:        "ok",
		NodeID:        s.nodeID,
		ActiveJobs:    activeJobs,
		ActiveLeases:  s.ActiveLeases(),
		ReservedMem:   reserved,
		AvailableMem:  grantable,
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

// refreshTrustedKeys records which keys this cluster signs store paths with.
//
// Written after every poll rather than once at startup, because membership
// changes: a peer that joins must have its closures accepted, and one that
// leaves must stop being trusted. The file is rewritten whole, so leaving is
// as effective as joining.
//
// A key is only recorded if it proves it belongs to the node publishing it -
// the material has to hash to that node's fingerprint. Anything else is
// logged and dropped, because a key trusted under the wrong name would let
// its holder sign as somebody else.
func (s *Server) refreshTrustedKeys() {
	self, err := identity.Key()
	if err != nil {
		return
	}
	nodes, err := s.store.ListAll()
	if err != nil {
		return
	}

	offered := map[string]string{}
	for _, n := range nodes {
		caps := map[string]string{}
		if n.CapsJSON != "" {
			_ = json.Unmarshal([]byte(n.CapsJSON), &caps)
		}
		key := caps[heartbeat.NixKeyCapability]
		if key == "" {
			continue
		}
		// Keyed by the fingerprint the key claims, which Collect then checks
		// against the key material itself. The node's UUID is a different
		// namespace and cannot be compared with it.
		fp, ok := nixsign.FingerprintOf(key)
		if !ok {
			continue
		}
		offered[fp] = key
	}

	trusted, rejected := nixsign.Collect(self, offered)
	for _, r := range rejected {
		log.Warn().Str("peer", r).Msg("ignoring a signing key that does not belong to the node offering it")
	}
	if err := nixsign.WriteTrustedKeys(trusted); err != nil {
		log.Warn().Err(err).Msg("could not record the cluster's signing keys")
	}
	s.warnUntrustedKeys(trusted)
}

// Seams for tests: what system nix trusts, and the root-owned key helper.
var (
	systemUntrusted = nixsign.SystemUntrusted
	helperInstalled = nixtrust.Installed
	helperAdd       = nixtrust.Add
)

// trustRetry is how long a key system nix refused waits before the daemon
// tries it again, so a missing helper or a broken sudo rule costs one nix
// command and one warning per key per interval, not per poll.
const trustRetry = 15 * time.Minute

// trustSettled marks a key system nix trusts: never checked again.
var trustSettled = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// warnUntrustedKeys makes system nix trust the cluster keys it would refuse:
// a multi-user nix-daemon ignores the keys narpack.Import passes, so closures
// signed by a key fail here until nix.conf lists it. Where setup installed the
// root-owned key helper (ADR-0018) the daemon adds the key through it with
// `sudo -n`, no prompt; otherwise, or if that fails, it warns and names
// `pipedpeer trust`, and tries again after trustRetry. Only keys due a check
// are looked at, so a steady cluster runs no nix command per poll. Returns the
// keys it warned about.
func (s *Server) warnUntrustedKeys(keys []string) []string {
	s.trustMu.Lock()
	defer s.trustMu.Unlock()
	if s.trustNext == nil {
		s.trustNext = map[string]time.Time{}
	}
	now := time.Now()
	var due []string
	for _, k := range keys {
		if next, seen := s.trustNext[k]; !seen || !now.Before(next) {
			due = append(due, k)
		}
	}
	if len(due) == 0 {
		return nil
	}
	missing, _, err := systemUntrusted(due)
	if err != nil {
		log.Warn().Err(err).Int("keys", len(due)).
			Msg("could not check which cluster keys system nix trusts; peer closures may be refused here (fix: pipedpeer trust)")
		for _, k := range due {
			s.trustNext[k] = now.Add(trustRetry)
		}
		return nil
	}
	for _, k := range due {
		s.trustNext[k] = trustSettled
	}
	if len(missing) == 0 {
		return nil
	}
	if helperInstalled() {
		err := helperAdd(missing)
		if err == nil {
			log.Info().Strs("keys", missing).Msg("system nix now trusts these cluster keys (added through the pipedpeer key helper)")
			return nil
		}
		log.Warn().Err(err).Strs("keys", missing).Msg("the pipedpeer key helper could not add these cluster keys")
	}
	for _, k := range missing {
		s.trustNext[k] = now.Add(trustRetry)
		log.Warn().Str("key", k).
			Msg("system nix does not trust this cluster key, so closures it signed are refused here; run `pipedpeer trust` on this machine")
	}
	return missing
}

// --- Core lease logic ---

func (s *Server) processAccept(req acceptRequest) (acceptResponse, int) {
	if req.TargetID == "" {
		return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id is required"}, http.StatusBadRequest
	}
	if req.TargetID != s.nodeID {
		return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id does not match this node"}, http.StatusConflict
	}
	// A pool-only or introducer-only node takes no jobs (heartbeat.Role).
	if role := heartbeat.Role; role != "" {
		return acceptResponse{Accepted: false, NodeID: s.nodeID,
			Reason: "this machine takes no jobs (" + role + " node)"}, http.StatusServiceUnavailable
	}

	// Host metrics are gathered before the lock: they read the OS and GPU, not
	// the lease table, and must not be collected while admission is held.
	// Memory is read with zero reservations so the reserved total can be
	// recomputed from the lease table inside the critical section.
	hostLoad := heartbeat.CollectLoad(0, 0)
	// Read outside the lock for the same reason as hostLoad: one asks the
	// kernel about this daemon's cgroup, the other asks a roommate over the
	// loopback, and neither should happen while admission is held.
	roommates := s.reservedOnThisMachine()
	budget := cgroups.SelfBudget()
	devices := gpu.PerDevice()
	gpuPresent := gpu.Detect().Vendor != gpu.VendorNone

	if req.RequiredCores > 0 {
		if free := freeCoresFromLoad(hostLoad); free < float64(req.RequiredCores) {
			reason := fmt.Sprintf("insufficient cores: need %d, free %.1f", req.RequiredCores, free)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	// Admission and lease creation happen under one lock. Checking capacity and
	// then inserting in a separate critical section would let concurrent
	// submitters all observe the same free slot and blow past the cap.
	s.mu.Lock()

	// Concurrency cap. Counts every live lease, not just running ones: a
	// reserved-but-not-yet-committed lease is a task on its way here, and
	// counting only running leases would let an unbounded number of
	// submitters slip past the cap in the window before they commit.
	if max := s.MaxConcurrentJobs(); max > 0 {
		if active := len(s.leases); active >= max {
			s.mu.Unlock()
			reason := fmt.Sprintf("at concurrency limit: %d/%d tasks", active, max)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	// Resource admission check
	if req.RequiredMemBytes > 0 {
		var reserved int64
		for _, l := range s.leases {
			reserved += l.MemBytes
		}
		// The same figure AvailableForJob reports, computed here because the
		// reserved total has to come from the lease table inside this lock.
		//
		// This path had its own arithmetic and used neither bound - so a
		// daemon admitted leases past its own share of the machine and past
		// what a roommate had already promised, which is every gap the budget
		// work closed, still open on the path every job is actually admitted
		// through. A stacked-reservation test caught it by failing on the
		// machine where the budget binds and passing on the one where free
		// memory does.
		available := hostLoad.AvailableMemBytes - reserved - roommates
		if budget.Total > 0 {
			if r := budget.Remaining() - reserved; r < available {
				available = r
			}
		}
		if available < 0 {
			available = 0
		}
		if !resourceest.Admit(req.RequiredMemBytes, available, len(s.leases) == 0 && roommates == 0, "this node") {
			s.mu.Unlock()
			reason := fmt.Sprintf("insufficient memory: need %d bytes, available %d bytes",
				req.RequiredMemBytes, available)
			return acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: reason}, http.StatusServiceUnavailable
		}
	}

	// GPU admission: reserve a specific device so two concurrent jobs on this
	// node do not land on the same card.
	gpuIndex := -1
	if req.RequireGPU {
		gpuIndex = s.reserveGPULocked(devices, gpuPresent, req.RequiredGPUMem)
		if gpuIndex < 0 {
			s.mu.Unlock()
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

	s.leases[leaseID] = lease
	s.mu.Unlock()
	s.persist()
	log.Info().Str("lease", leaseID).Str("job", req.JobName).Str("submitter", req.SubmitterNode).
		Int64("mem", req.RequiredMemBytes).Msg("lease granted")

	return acceptResponse{
		Accepted:  true,
		NodeID:    s.nodeID,
		LeaseID:   leaseID,
		GPUIndex:  gpuIndex,
		ExpiresAt: lease.ExpiresAt.Format(time.RFC3339Nano),
	}, http.StatusOK
}

// freeCoresFromLoad derives free cores from an already-collected load sample,
// so admission can reuse one sample instead of re-reading the host.
func freeCoresFromLoad(load registry.LoadInfo) float64 {
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

// reserveGPULocked is reserveGPU's body with the caller holding s.mu, so the
// device choice and the lease that claims it can be made atomically. Device
// telemetry is passed in because reading it shells out to vendor tools.
func (s *Server) reserveGPULocked(devices []gpu.DeviceUsage, gpuPresent bool, minVRAM int64) int {
	if len(devices) == 0 {
		// A GPU with no per-device telemetry (AMD without a parseable
		// rocm-smi) can still run the job on device 0. But with no
		// VRAM numbers we cannot honour a minimum, so a request that specifies
		// one must be refused rather than accepted on a guess.
		if gpuPresent && minVRAM == 0 {
			return 0
		}
		return -1
	}

	reserved := make(map[int]bool, len(s.leases))
	for _, l := range s.leases {
		if l.GPUIndex >= 0 && (l.State == LeaseReserved || l.State == LeaseRunning) {
			reserved[l.GPUIndex] = true
		}
	}

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
		s.persist()
		log.Warn().Str("lease", req.LeaseID).Str("state", string(LeaseReserved)).Msg("lease expired")
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
			s.persist()
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
	s.persist()

	return commitResponse{Committed: true, NodeID: s.nodeID}, http.StatusOK
}

func (s *Server) processComplete(req completeRequest) {
	s.mu.Lock()
	leaseID := req.LeaseID
	_, found := s.leases[leaseID]
	if leaseID != "" {
		delete(s.leases, leaseID)
	}
	s.mu.Unlock()
	s.persist()
	if found {
		log.Info().Str("lease", leaseID).Str("status", req.Status).Msg("lease released")
	}
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

// --- Helpers ---

func generateLeaseID() string {
	return uuid.New().String()
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// diffNARFor builds an archive holding only the closure paths a peer is
// missing, and reports whether it is worth using. The caller keeps the whole
// archive when this declines, so every failure mode here is a slower
// transfer rather than a broken one.
func (s *Server) diffNARFor(ph *PeerHealth, storePath string) (string, bool) {
	// Register the closure in the local store first. It arrives as a cached
	// archive and is otherwise only imported at exec time, so until then this
	// node cannot answer questions about its own contents - nix says "path is
	// not valid" - and every transfer falls back to the whole archive. Doing
	// it here rather than before the peer loop keeps the "peer already has
	// it" check answering about the peer.
	_ = s.materializeClosure(storePath)

	// Each decline is logged: a transfer that quietly sends everything looks
	// exactly like one that sent only what was needed, and the whole point of
	// this path is the difference between those two.
	paths, err := closurePaths(storePath)
	if err != nil || len(paths) == 0 {
		log.Info().Err(err).Str("store", storePath).
			Msg("cannot list closure paths; sending the whole archive")
		return "", false
	}
	missing, ok := peerMissingPaths(ph.Host, ph.Port, paths)
	if !ok {
		log.Info().Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
			Msg("peer did not answer which paths it lacks; sending the whole archive")
		return "", false
	}
	if len(missing) == 0 {
		log.Info().Msg("peer already has every path; nothing to send")
		return "", false
	}
	if len(missing) == len(paths) {
		log.Info().Int("paths", len(paths)).
			Msg("peer shares no paths with this closure; a diff would save nothing")
		return "", false
	}
	dir, err := userdir.Scratch("nardiff-*")
	if err != nil {
		return "", false
	}
	out := filepath.Join(dir, "diff.nar")
	if err := exportPaths(missing, out); err != nil {
		os.RemoveAll(dir)
		return "", false
	}
	log.Info().Int("missing", len(missing)).Int("closure", len(paths)).
		Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
		Msg("sending only the store paths this peer lacks")
	return out, true
}
