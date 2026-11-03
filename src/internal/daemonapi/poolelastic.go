package daemonapi

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
)

// A map in progress uses the peers that hold its closure now. The shim asks
// for its pacing about once a second while a map runs (/v1/pool/stats?store=),
// and a peer becomes one of those peers two ways after the job started:
//   - it was still importing the closure the upload seeded (the first job
//     after a join: on a fresh lab every peer was, and the job ran wholly on
//     the submitter), or
//   - it joined the cluster mid-run, and holds nothing. Seeding happened only
//     at upload, so such a peer was filtered out of every chunk for good.
// seedLate covers the second and counts both as pending, so the shim keeps
// asking until they are ready rather than settling for local.

// heldTTL is how long a peer's "I can run this closure" is believed without
// asking again. A store path is immutable once valid, so a yes only goes
// stale if the peer's store is wiped; the shim's per-second pacing would
// otherwise cost one request per peer per second.
const heldTTL = 30 * time.Second

// lateSeedRetry is how long after a failed late seed the same peer and
// closure are left alone: a peer that refuses the closure must not be sent
// it again on every pacing request.
const lateSeedRetry = 5 * time.Minute

type heldCache struct {
	mu    sync.Mutex
	yes   map[string]time.Time // peer+store+need -> when it said yes
	tried map[string]time.Time // peer+store -> when a late seed of it failed
	gpu   map[string]bool      // store -> its closure needs a GPU
}

func heldKey(ph *PeerHealth, storePath string, need http.Header) string {
	return ph.addr() + " " + storePath + " " + fmt.Sprint(need)
}

// peerHolds is peerHasStore with its yes-answers cached for heldTTL.
func (s *Server) peerHolds(ph *PeerHealth, storePath string, need http.Header) bool {
	key := heldKey(ph, storePath, need)
	s.held.mu.Lock()
	at, ok := s.held.yes[key]
	s.held.mu.Unlock()
	if ok && time.Since(at) < heldTTL {
		return true
	}
	if !s.peerHasStore(ph, storePath, need) {
		return false
	}
	s.markHeld(ph, storePath, need)
	return true
}

func (s *Server) markHeld(ph *PeerHealth, storePath string, need http.Header) {
	s.held.mu.Lock()
	defer s.held.mu.Unlock()
	if s.held.yes == nil {
		s.held.yes = map[string]time.Time{}
	}
	s.held.yes[heldKey(ph, storePath, need)] = time.Now()
}

// closureIsGPU reports, once per store path, whether its closure is CUDA:
// broadcastClosure sends those only to NVIDIA peers, and so does seedLate.
func (s *Server) closureIsGPU(storePath string) bool {
	s.held.mu.Lock()
	gpu, ok := s.held.gpu[storePath]
	s.held.mu.Unlock()
	if ok {
		return gpu
	}
	paths, err := closurePaths(storePath)
	gpu = err == nil && closureNeedsGPU(paths)
	s.held.mu.Lock()
	if s.held.gpu == nil {
		s.held.gpu = map[string]bool{}
	}
	s.held.gpu[storePath] = gpu
	s.held.mu.Unlock()
	return gpu
}

// seedLate starts sending storePath to every healthy peer that could run it
// and lacks it, and returns how many peers are getting it now (importing it,
// or being sent it by this node). Peers of another system, pool-only phones
// and, for a CUDA closure, peers without an NVIDIA GPU are left to
// broadcastClosure's own paths (recipes, host python). Asked only for a
// closure a job on this node runs, so this node holds it.
func (s *Server) seedLate(storePath string) int {
	targets, _ := s.healthyPeers()
	gpu := false
	if len(targets) > 0 {
		gpu = s.closureIsGPU(storePath)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	pending := 0
	for i := range targets {
		ph := targets[i]
		if ph.Role == heartbeat.RolePoolOnly || (ph.Arch != "" && ph.Arch != identity.System()) ||
			(gpu && !isNVIDIA(ph)) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.lateSeedOne(ph, storePath) {
				mu.Lock()
				pending++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return pending
}

func isNVIDIA(ph PeerHealth) bool {
	keep, _ := gpuPeersOnly([]PeerHealth{ph})
	return len(keep) == 1
}

// lateSeedOne reports whether ph is getting storePath now, starting the send
// when nobody is.
func (s *Server) lateSeedOne(ph PeerHealth, storePath string) bool {
	if s.seedFor(ph, storePath) != nil {
		return true
	}
	s.held.mu.Lock()
	at, ok := s.held.yes[heldKey(&ph, storePath, nil)]
	s.held.mu.Unlock()
	if ok && time.Since(at) < heldTTL {
		return false
	}
	key := ph.addr() + " " + storePath
	s.held.mu.Lock()
	failed, tried := s.held.tried[key]
	s.held.mu.Unlock()
	if tried && time.Since(failed) < lateSeedRetry {
		return false // peerFn still finds it if it got the closure elsewhere
	}
	runnable, importing := s.peerStoreState(&ph, storePath, nil)
	switch {
	case runnable:
		s.markHeld(&ph, storePath, nil)
		return false
	case importing:
		return true
	}
	if s.serialImports.Load() && s.seedsInFlight() > 0 {
		return false // one import at a time on a shared-RAM rig; the next pacing asks again
	}
	sd, started := s.startSeed(ph, storePath)
	if !started {
		return true
	}
	log.Info().Str("peer", ph.addr()).Str("store", storePath).
		Msg("peer lacks the closure of a map in progress; sending it so the map can use it")
	go func() {
		start := time.Now()
		err := s.importStoreOnPeer(&ph, storePath, func() (string, bool) { return localArchive(s.narCache, storePath) })
		s.finishSeed(ph, storePath, sd, err)
		if err != nil {
			s.held.mu.Lock()
			if s.held.tried == nil {
				s.held.tried = map[string]time.Time{}
			}
			s.held.tried[key] = time.Now()
			s.held.mu.Unlock()
			log.Warn().Err(err).Str("peer", ph.addr()).Str("store", storePath).
				Msg("late closure send failed; this peer gets no work from the map")
			return
		}
		s.markHeld(&ph, storePath, nil)
		log.Info().Str("peer", ph.addr()).Str("store", storePath).
			Float64("secs", time.Since(start).Round(10*time.Millisecond).Seconds()).
			Msg("peer took the closure mid-run; it joins the map")
	}()
	return true
}

func (s *Server) seedsInFlight() int {
	s.seedsMu.Lock()
	defer s.seedsMu.Unlock()
	return len(s.seeds)
}

// substitutesItself reports whether a seed should let ph fetch the paths the
// binary cache serves instead of taking them from this node. Only for a peer
// reached through an internet forwarder (127.0.0.1:<port>): it has its own
// uplink, and this node's may be the slower one. A LAN peer takes every path
// from the sender. With the cache, each fresh lab PC downloaded the whole
// closure over the lab's one shared uplink while the sender sat idle on the
// same switch, and the sender waited substituteWait (30 s) before taking
// over; the first job after a join found every peer still importing after a
// minute (rehearsal-day E14).
func (ph *PeerHealth) substitutesItself() bool {
	return isLoopback(ph.Host)
}
