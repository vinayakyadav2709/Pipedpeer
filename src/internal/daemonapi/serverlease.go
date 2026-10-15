package daemonapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

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
