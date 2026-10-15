package daemonapi

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
)

// AddPeer registers the peer internet mode put in front of host:port (a local
// forwarder) as a verified internet path of whichever node answers there.
// A peer that does not answer yet is retried by every poll.
func (s *Server) AddPeer(host string, port int) error {
	if s.store == nil {
		return fmt.Errorf("node store unavailable")
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	since := s.pathGeneration()
	h, err := probeHealth(host, port)
	if err != nil {
		log.Warn().Err(err).Str("at", addr).Msg("internet peer not answering yet; retrying shortly, then every poll")
		s.pathMu.Lock()
		if s.removedAt[addr] > since {
			s.pathMu.Unlock()
			return nil // removed while we probed
		}
		s.pendingMu.Lock()
		if s.pending == nil {
			s.pending = map[string]struct{}{}
		}
		s.pending[addr] = struct{}{}
		s.pendingMu.Unlock()
		s.pathMu.Unlock()
		// A fresh link's first probe often meets a forwarder still settling
		// (EOF, no answer); waiting a whole poll for it cost 10 s per join.
		go s.retrySoon(addr, since)
		return nil
	}
	if _, err := s.adoptPath(h.NodeID, nodestore.PathInternet, host, port, since); err != nil {
		return err
	}
	// Poll now rather than at the next sweep: a peer not yet marked healthy is
	// invisible to closure broadcast and to spill.
	go s.pollAllNodes()
	return nil
}

// adoptPath records host:port as a verified path of nodeID, unless RemovePeer
// dropped that address after generation since.
func (s *Server) adoptPath(nodeID string, kind nodestore.PathKind, host string, port int, since uint64) (bool, error) {
	ok, err := s.verifyPath(nodeID, kind, host, port, since)
	if err != nil {
		return false, fmt.Errorf("verify path %s:%d for %s: %w", host, port, nodeID, err)
	}
	if !ok {
		log.Info().Str("peer", nodeID).Str("at", net.JoinHostPort(host, strconv.Itoa(port))).
			Msg("path removed while it was being checked; not re-adding it")
	}
	return ok, nil
}

func (s *Server) pathGeneration() uint64 {
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	return s.pathGen
}

// verifyPath is PathOK plus the pin, skipped (false) when the address was
// removed after generation since. Under pathMu, so a removal cannot land
// between the check and the write.
func (s *Server) verifyPath(nodeID string, kind nodestore.PathKind, host string, port int, since uint64) (bool, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	if s.removedAt[addr] > since {
		return false, nil
	}
	if err := s.store.PathOK(nodeID, kind, host, port); err != nil {
		return false, err
	}
	tlsid.Expect(addr, nodeID)
	return true, nil
}

// RemovePeer forgets the internet path at host:port, for when internet mode's
// forwarder goes away. Only that path: the node keeps any other.
func (s *Server) RemovePeer(host string, port int) error {
	if s.store == nil {
		return fmt.Errorf("node store unavailable")
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	s.pathGen++
	if s.removedAt == nil {
		s.removedAt = map[string]uint64{}
	}
	s.removedAt[addr] = s.pathGen
	s.pendingMu.Lock()
	delete(s.pending, addr)
	s.pendingMu.Unlock()

	nodes, err := s.store.ListAll()
	if err != nil {
		return err
	}
	for _, n := range nodes {
		paths, err := s.store.Paths(n.NodeID)
		if err != nil {
			return fmt.Errorf("paths of %s: %w", n.NodeID, err)
		}
		for _, p := range paths {
			if p.Host == host && p.Port == port {
				if err := s.store.DropPath(n.NodeID, host, port); err != nil {
					return err
				}
			}
		}
	}
	// The forwarder port goes back to the pool; the next peer behind it is
	// another node.
	tlsid.Forget(addr)
	return nil
}

// StartPeerPoller launches a background goroutine that discovers nodes on the
// LAN and polls every known node's paths.
func (s *Server) StartPeerPoller(interval time.Duration) {
	s.peersMu.Lock()
	s.peerHealths = make(map[string]*PeerHealth)
	s.stopPoller = make(chan struct{})
	s.peersMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.pollAllNodes()

		for {
			select {
			case <-ticker.C:
				s.pollAllNodes()
			case <-s.stopPoller:
				return
			}
		}
	}()
}

func (s *Server) StopPeerPoller() {
	s.peersMu.Lock()
	if s.stopPoller != nil {
		close(s.stopPoller)
	}
	s.peersMu.Unlock()
}

// SetSelfInfo tells the daemon how to describe itself in the node store, so
// this machine is a placement candidate for every peer that polls it and shows
// up in its own node list. host is the start-time guess; each poll publishes
// the address the kernel would route to the known peers instead.
func (s *Server) SetSelfInfo(host string, daemonPort int, caps map[string]string) {
	s.selfMu.Lock()
	s.selfIP = host
	s.selfPort = daemonPort
	s.selfCaps = caps
	s.selfMu.Unlock()
}

// pollAllNodes is one refresh cycle of cluster membership:
//
//	discover on the LAN → record self → check every peer's paths → gossip → prune.
//
// Everything the orchestrator later reads from /v1/nodes is written here.
func (s *Server) pollAllNodes() {
	if s.store == nil {
		return
	}

	// 1. LAN discovery adds candidates only. A sighting never replaces a
	//    working path and never refreshes a row (E4, E5e): the path checks
	//    below decide.
	s.hookMu.RLock()
	discover := s.discoverFn
	s.hookMu.RUnlock()
	if discover != nil {
		for _, d := range discover() {
			if d.NodeID == "" || d.NodeID == s.nodeID || d.Host == "" || d.DaemonPort == 0 {
				continue
			}
			if err := s.store.SeePath(d.NodeID, nodestore.PathLAN, d.Host, d.DaemonPort); err != nil {
				log.Warn().Err(err).Str("peer", d.NodeID).Str("host", d.Host).Msg("could not record discovered address")
			}
		}
	}

	// 2. Record self. Self is always a placement candidate, so it belongs in
	//    the same table as everyone else rather than being special-cased.
	s.upsertSelf()

	// 3. Internet forwarders whose peer had not answered yet.
	s.retryPending()

	// 4. Check every known peer concurrently.
	nodes, err := s.store.ListAll()
	if err != nil {
		log.Warn().Err(err).Msg("node store unreadable; skipping this poll")
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	finalHealths := make(map[string]*PeerHealth)

	for _, n := range nodes {
		if n.NodeID == s.nodeID {
			continue // self is refreshed directly, not over the network
		}
		wg.Add(1)
		go func(node nodestore.Node) {
			defer wg.Done()
			ph := s.pollOne(node)
			if ph == nil {
				return
			}
			mu.Lock()
			finalHealths[net.JoinHostPort(ph.Host, strconv.Itoa(ph.Port))] = ph
			mu.Unlock()
		}(n)
	}
	wg.Wait()

	s.peersMu.Lock()
	prev := s.peerHealths
	s.peerHealths = finalHealths
	s.peersMu.Unlock()
	hostnames := make(map[string]string, len(nodes))
	for _, n := range nodes {
		hostnames[n.NodeID] = n.Hostname
	}
	logPeerChanges(prev, finalHealths, hostnames)

	// 5. Learn moved peers' addresses from the healthy ones.
	s.maybePullGossip(finalHealths)

	// 6. Forget nodes and paths gone long enough (rules in nodestore).
	if err := s.store.PruneStale(); err != nil {
		log.Warn().Err(err).Msg("could not prune stale nodes")
	}

	// 7. Keep the list of signing keys this cluster uses current, so nix can
	//    require a signature and still accept peer closures.
	s.refreshTrustedKeys()
}

// retryPending polls the forwarders AddPeer could not identify yet.
func (s *Server) retryPending() {
	since := s.pathGeneration()
	s.pendingMu.Lock()
	addrs := make([]string, 0, len(s.pending))
	for a := range s.pending {
		addrs = append(addrs, a)
	}
	s.pendingMu.Unlock()
	for _, a := range addrs {
		s.tryPending(a, since)
	}
}

// retrySoonDelays are AddPeer's quick retries before the poll takes over.
var retrySoonDelays = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}

// retrySoon retries one pending forwarder after each of retrySoonDelays, and
// polls at once when it answers. It stops when the address is no longer
// pending (answered, or RemovePeer dropped it).
func (s *Server) retrySoon(addr string, since uint64) {
	for _, d := range retrySoonDelays {
		time.Sleep(d)
		s.pendingMu.Lock()
		_, still := s.pending[addr]
		s.pendingMu.Unlock()
		if !still {
			return
		}
		if s.tryPending(addr, since) {
			s.pollAllNodes()
			return
		}
	}
}

// tryPending probes one pending forwarder and, when it answers, records the
// path (subject to the removal check) and takes it off the pending list.
func (s *Server) tryPending(a string, since uint64) bool {
	host, portStr, err := net.SplitHostPort(a)
	if err != nil {
		return false // only ever written by JoinHostPort
	}
	port, _ := strconv.Atoi(portStr)
	h, err := probeHealth(host, port)
	if err != nil {
		return false // logged once by AddPeer; still pending
	}
	ok, err := s.adoptPath(h.NodeID, nodestore.PathInternet, host, port, since)
	if err != nil {
		log.Warn().Err(err).Str("at", a).Msg("internet peer answered but its path could not be stored")
		return false
	}
	s.pendingMu.Lock()
	delete(s.pending, a)
	s.pendingMu.Unlock()
	if ok {
		log.Info().Str("peer", h.NodeID).Str("at", a).Msg("internet peer answered")
	}
	return ok
}
