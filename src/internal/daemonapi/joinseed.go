package daemonapi

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// joinSeeder tracks closure seeding to nodes that join the cluster while a job
// is active, so pool spill can use them without waiting for a re-upload.
type joinSeeder struct {
	mu          sync.Mutex
	s           *Server
	attempted   map[string]map[string]bool // storePath -> peerKey -> attempted
	inFlight    map[string]bool            // storePath -> broadcast in flight
	broadcastFn func(storePath string) []string
}

func joinSeedPeerKey(ph PeerHealth) string {
	if ph.NodeID != "" {
		return ph.NodeID
	}
	return ph.addr()
}

func (s *Server) joinSeeder() *joinSeeder {
	s.joinSeedOnce.Do(func() {
		s.joinSeed = &joinSeeder{
			s:         s,
			attempted: make(map[string]map[string]bool),
			inFlight:  make(map[string]bool),
		}
	})
	return s.joinSeed
}

// registerJobPeers records the healthy peers at upload as already attempted
// for this store path, and marks a broadcast in flight until the returned
// cleanup function is called.
func (s *Server) registerJobPeers(storePath string) func() {
	if storePath == "" {
		return nil
	}
	js := s.joinSeeder()
	js.mu.Lock()
	defer js.mu.Unlock()

	if js.attempted[storePath] == nil {
		js.attempted[storePath] = make(map[string]bool)
	}
	peers, _ := s.healthyPeers()
	for _, ph := range peers {
		js.attempted[storePath][joinSeedPeerKey(ph)] = true
	}
	js.inFlight[storePath] = true

	return func() {
		js.mu.Lock()
		defer js.mu.Unlock()
		delete(js.inFlight, storePath)
	}
}

// isJobActive reports whether a job is active for closure seeding: status
// "importing" or "running", or "uploaded" and younger than 10 minutes.
// Jobs uploaded with skip_broadcast (DDP) are not active for closure seeding.
func isJobActive(j *JobRecord, now time.Time) bool {
	if j == nil || j.SkipBroadcast || j.StorePath == "" {
		return false
	}
	switch j.Status {
	case "importing", "running":
		return true
	case "uploaded":
		return now.Sub(j.CreatedAt) < 10*time.Minute
	default:
		return false
	}
}

// seedJoiningPeers seeds active job closures to healthy peers that joined since
// upload. Called at the end of each pollAllNodes cycle.
func (s *Server) seedJoiningPeers() {
	now := time.Now()
	activeStores := make(map[string]bool)

	s.jobsMu.Lock()
	for _, j := range s.jobs {
		if isJobActive(j, now) {
			activeStores[j.StorePath] = true
		}
	}
	s.jobsMu.Unlock()

	targets, _ := s.healthyPeers()
	currentHealthy := make(map[string]bool, len(targets))
	for _, ph := range targets {
		currentHealthy[joinSeedPeerKey(ph)] = true
	}

	js := s.joinSeeder()
	js.mu.Lock()

	// The set is deleted when no active job uses the store path.
	for sp := range js.attempted {
		if !activeStores[sp] {
			delete(js.attempted, sp)
		}
	}

	// A peer no longer healthy leaves the set.
	for _, peers := range js.attempted {
		for key := range peers {
			if !currentHealthy[key] {
				delete(peers, key)
			}
		}
	}

	var toBroadcast []string
	for sp := range activeStores {
		// At most one broadcast in flight per store path; peers seen while one is
		// in flight stay outside the set and are taken by the next poll.
		if js.inFlight[sp] {
			continue
		}

		if js.attempted[sp] == nil {
			js.attempted[sp] = make(map[string]bool)
		}

		var newPeers []PeerHealth
		for _, ph := range targets {
			if !js.attempted[sp][joinSeedPeerKey(ph)] {
				newPeers = append(newPeers, ph)
			}
		}

		if len(newPeers) == 0 {
			continue
		}

		// Each seed logs peer and store; a missing cached NAR logs a warning
		// and marks nothing.
		if s.narCache == nil {
			log.Warn().Str("store", sp).Msg("nar cache not configured; skipping join seed")
			continue
		}
		narPath, ok := s.narCache.narFileFor(sp)
		if !ok || narPath == "" {
			for _, ph := range newPeers {
				log.Warn().Str("peer", joinSeedPeerKey(ph)).Str("store", sp).
					Msg("cached NAR no longer available; skipping closure broadcast to joining peer")
			}
			continue
		}

		for _, ph := range newPeers {
			log.Info().Str("peer", joinSeedPeerKey(ph)).Str("store", sp).
				Msg("seeding closure to joining peer")
			js.attempted[sp][joinSeedPeerKey(ph)] = true
		}

		js.inFlight[sp] = true
		toBroadcast = append(toBroadcast, sp)
	}
	js.mu.Unlock()

	// Broadcasts run asynchronously and never block pollAllNodes.
	for _, sp := range toBroadcast {
		go func(storePath string) {
			defer func() {
				js.mu.Lock()
				delete(js.inFlight, storePath)
				js.mu.Unlock()
			}()
			fn := js.getBroadcastFn()
			fn(storePath)
		}(sp)
	}
}

func (js *joinSeeder) getBroadcastFn() func(string) []string {
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.broadcastFn != nil {
		return js.broadcastFn
	}
	return js.s.broadcastClosure
}

func (s *Server) setJoinSeedBroadcastFn(fn func(storePath string) []string) {
	js := s.joinSeeder()
	js.mu.Lock()
	defer js.mu.Unlock()
	js.broadcastFn = fn
}

func (s *Server) joinSeedAttempted(storePath, peerKey string) bool {
	js := s.joinSeeder()
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.attempted[storePath] == nil {
		return false
	}
	return js.attempted[storePath][peerKey]
}

func (s *Server) joinSeedInFlight(storePath string) bool {
	js := s.joinSeeder()
	js.mu.Lock()
	defer js.mu.Unlock()
	return js.inFlight[storePath]
}

func (s *Server) joinSeedHasAttemptedSet(storePath string) bool {
	js := s.joinSeeder()
	js.mu.Lock()
	defer js.mu.Unlock()
	_, exists := js.attempted[storePath]
	return exists
}
