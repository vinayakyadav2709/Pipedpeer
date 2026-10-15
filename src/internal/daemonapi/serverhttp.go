package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
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
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

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
	r.Post("/v1/env/foreign", s.handleForeignEnv)
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
