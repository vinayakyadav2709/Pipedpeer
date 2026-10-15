package daemonapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
)

// logPeerChanges writes "peer up" for a peer healthy now and not before, and
// "peer down" for one healthy before and not now (or gone from the table), so
// `pipedpeer events` can show joins and leaves on the discovery path too.
func logPeerChanges(prev, next map[string]*PeerHealth, hostnames map[string]string) {
	healthy := func(m map[string]*PeerHealth, at string) bool {
		ph := m[at]
		return ph != nil && ph.Status == "healthy"
	}
	emit := func(msg, at string, ph *PeerHealth) {
		log.Info().Str("peer", ph.NodeID).Str("host", hostnames[ph.NodeID]).
			Str("at", at).Str("source", ph.Source).Msg(msg)
	}
	for at, ph := range next {
		if ph != nil && healthy(next, at) && !healthy(prev, at) {
			emit("peer up", at, ph)
		}
	}
	for at, ph := range prev {
		if ph != nil && healthy(prev, at) && !healthy(next, at) {
			emit("peer down", at, ph)
		}
	}
}

// healthTimeout bounds one /health probe of one path.
const healthTimeout = 3 * time.Second

// probeHealth asks host:port who it is. An answer without a node id is not an
// answer.
func probeHealth(host string, port int) (healthResponse, error) {
	var h healthResponse
	client := &http.Client{Timeout: healthTimeout}
	resp, err := client.Get(fmt.Sprintf("http://%s/health", net.JoinHostPort(host, strconv.Itoa(port))))
	if err != nil {
		return h, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return h, fmt.Errorf("/health answered %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return h, fmt.Errorf("/health unreadable: %w", err)
	}
	if h.NodeID == "" {
		return h, fmt.Errorf("/health named no node")
	}
	return h, nil
}

// candidateOrder is the order the poller tries a node's paths: the active one
// first, then lan before internet before gossip.
func candidateOrder(paths []nodestore.Path) []nodestore.Path {
	rank := map[nodestore.PathKind]int{nodestore.PathLAN: 0, nodestore.PathInternet: 1, nodestore.PathGossip: 2}
	out := make([]nodestore.Path, 0, len(paths))
	var rest []nodestore.Path
	for _, p := range paths {
		if p.Kind == nodestore.PathSelf {
			continue
		}
		if p.Active && len(out) == 0 {
			out = append(out, p)
			continue
		}
		rest = append(rest, p)
	}
	sort.SliceStable(rest, func(i, j int) bool { return rank[rest[i].Kind] < rank[rest[j].Kind] })
	return append(out, rest...)
}

// pollOne checks one node: the active path first, then the other candidates
// until one answers as this node. A path that answers is verified (PathOK, and
// its certificate pinned to the node id); one that fails or answers as another
// node is marked so. While the active path is not a LAN one, LAN candidates are
// still tried so a verified LAN path can take over (nodestore decides).
func (s *Server) pollOne(node nodestore.Node) *PeerHealth {
	since := s.pathGeneration()
	paths, err := s.store.Paths(node.NodeID)
	if err != nil {
		log.Warn().Err(err).Str("peer", node.NodeID).Msg("could not read node paths")
		return nil
	}
	ph := &PeerHealth{Host: node.Host, Port: node.Port, NodeID: node.NodeID, Source: string(node.Source), Status: "unreachable"}
	var verified *healthResponse
	for _, p := range candidateOrder(paths) {
		if verified != nil && p.Kind != nodestore.PathLAN {
			break // only LAN candidates can still beat what answered
		}
		addr := net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
		h, err := probeHealth(p.Host, p.Port)
		if err != nil && strings.Contains(err.Error(), "expected node") {
			// The pinned key no longer answers here. Node id = key, so this is
			// another node (a reused address, a reinstall) or someone in the
			// middle; either way it is not this node's path. Forget the pin so
			// whoever lives here can be found as itself.
			s.dropPath(node.NodeID, p.Host, p.Port, "a different key answers at "+addr)
			continue
		}
		if err != nil {
			if ferr := s.store.PathFailed(node.NodeID, p.Host, p.Port, err.Error()); ferr != nil {
				log.Warn().Err(ferr).Str("peer", node.NodeID).Str("at", addr).Msg("could not record failed path")
			}
			continue
		}
		if h.NodeID != node.NodeID {
			// Someone else lives at this address now (a reused forwarder port,
			// a DHCP lease handed on).
			s.dropPath(node.NodeID, p.Host, p.Port, "node "+h.NodeID+" answers at "+addr)
			continue
		}
		ok, err := s.verifyPath(node.NodeID, p.Kind, p.Host, p.Port, since)
		if err != nil {
			log.Warn().Err(err).Str("peer", node.NodeID).Str("at", addr).Msg("could not record verified path")
			continue
		}
		if !ok {
			continue // RemovePeer dropped it while we probed
		}
		if verified == nil {
			hh := h
			verified = &hh
			ph.Host, ph.Port, ph.Source = p.Host, p.Port, string(p.Kind)
		}
	}

	if verified == nil {
		if err := s.store.MarkUnreachable(node.NodeID); err != nil {
			log.Warn().Err(err).Str("peer", node.NodeID).Msg("could not record unreachable peer")
		}
		return ph
	}
	h := *verified
	ph.Status = "healthy"
	ph.ActiveJobs = h.ActiveJobs
	ph.AvailableMem = h.AvailableMem
	ph.TotalCPUs = h.Load.TotalCPUs
	ph.ReservedMem = h.ReservedMem
	ph.Machine = h.Capabilities[heartbeat.MachineCapability]
	ph.ClosureFormats = h.Capabilities[heartbeat.ClosureFormatCapability]
	ph.GPU = h.Capabilities["gpu"]
	ph.Role = h.Capabilities[heartbeat.RoleCapability]
	ph.Arch = h.Capabilities["arch"]

	capsJSON, err := json.Marshal(h.Capabilities)
	if err != nil {
		log.Warn().Err(err).Str("peer", node.NodeID).Msg("could not encode peer capabilities")
	}
	loadJSON, err := json.Marshal(h.Load)
	if err != nil {
		log.Warn().Err(err).Str("peer", node.NodeID).Msg("could not encode peer load")
	}
	// Host/Port/Source are the store's (the active path); MarkHealthy never
	// moves them.
	if err := s.store.MarkHealthy(nodestore.Node{
		NodeID:       node.NodeID,
		Arch:         h.Capabilities["arch"],
		Hostname:     h.Capabilities["hostname"],
		ActiveJobs:   h.ActiveJobs,
		AvailableMem: h.AvailableMem,
		ReservedMem:  h.ReservedMem,
		TotalMem:     h.Load.TotalMemBytes,
		CPUPercent:   h.Load.CPUPercent,
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	}); err != nil {
		log.Warn().Err(err).Str("peer", node.NodeID).Msg("could not record healthy peer")
	}
	return ph
}

// dropPath lets go of a path that belongs to someone else now, and of the
// address's pin, so a different machine that takes the address is not refused.
func (s *Server) dropPath(nodeID, host string, port int, why string) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	log.Info().Str("peer", nodeID).Str("at", addr).Str("why", why).Msg("dropping a path that is not this node's any more")
	if err := s.store.DropPath(nodeID, host, port); err != nil {
		log.Warn().Err(err).Str("peer", nodeID).Str("at", addr).Msg("could not drop path")
	}
	tlsid.Forget(addr)
}

// upsertSelf writes this node's own live state into the store, at the address
// the kernel would use to reach the known peers (re-chosen every poll).
func (s *Server) upsertSelf() {
	s.selfMu.RLock()
	ip, port, caps, prevHost := s.selfIP, s.selfPort, s.selfCaps, s.selfHost
	s.selfMu.RUnlock()
	if port == 0 {
		return // SetSelfInfo not called (tests, or daemon not fully started)
	}

	reserved := s.ReservedMem()
	load := heartbeat.CollectLoad(s.ActiveJobs(), reserved)
	load.QueueDepth = s.QueueDepth()
	load.RecentFailures = s.RecentFailures()
	// The same grantable figure /health gives peers. The raw free memory is
	// the host's: a daemon in a 1.5 GiB container showed 9.8 GiB on its own
	// `nodes` row, and local placement read that row.
	load.AvailableMemBytes = s.grantableFrom(load, reserved)
	capsJSON, err := json.Marshal(caps)
	if err != nil {
		log.Warn().Err(err).Msg("could not encode own capabilities")
	}
	loadJSON, err := json.Marshal(load)
	if err != nil {
		log.Warn().Err(err).Msg("could not encode own load")
	}

	host := s.advertisedHost(ip)
	if prevHost != "" && prevHost != host {
		// PathOK below replaces the old self path.
		log.Info().Str("from", prevHost).Str("to", host).Msg("this machine's address changed")
	}
	if err := s.store.PathOK(s.nodeID, nodestore.PathSelf, host, port); err != nil {
		log.Warn().Err(err).Str("host", host).Msg("could not record own address")
	}
	s.selfMu.Lock()
	s.selfHost = host
	s.selfMu.Unlock()

	if err := s.store.MarkHealthy(nodestore.Node{
		NodeID:       s.nodeID,
		Arch:         caps["arch"],
		Hostname:     caps["hostname"],
		ActiveJobs:   s.ActiveJobs(),
		AvailableMem: load.AvailableMemBytes,
		ReservedMem:  s.ReservedMem(),
		TotalMem:     load.TotalMemBytes,
		CPUPercent:   load.CPUPercent,
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	}); err != nil {
		log.Warn().Err(err).Msg("could not record this node's own row")
	}
}

// advertisedHost is the address this node publishes to its peers.
//
// The interface-list guess is only right when a machine has one route to the
// world. A laptop holding a stale 192.168.x lease published that while its
// peers were reachable only over an overlay, and nothing complained: the node
// simply advertised an address no peer could dial.
//
// So ask the kernel which of our addresses it would use to reach the peers we
// can currently reach, and publish that.
func (s *Server) advertisedHost(fallback string) string {
	if s.store == nil {
		return fallback
	}
	nodes, err := s.store.ListAll()
	if err != nil {
		log.Warn().Err(err).Msg("node store unreadable; advertising the start-time address")
		return fallback
	}
	var peers []string
	for _, n := range nodes {
		if n.NodeID == s.nodeID || n.Host == "" || n.Port == 0 {
			continue
		}
		// Only peers we can currently reach get a vote: letting a machine
		// that is long gone decide how we describe ourselves is how the
		// stale answer survives.
		if n.State != "healthy" {
			continue
		}
		peers = append(peers, net.JoinHostPort(n.Host, strconv.Itoa(n.Port)))
	}
	return netaddr.Advertise(peers, fallback)
}

// rewriteHost swaps the host in a "user@host:port" endpoint, leaving the user
// and port alone.
func rewriteHost(endpoint, host string) string {
	if endpoint == "" || host == "" {
		return endpoint
	}
	user, rest := "", endpoint
	if i := strings.LastIndex(endpoint, "@"); i >= 0 {
		user, rest = endpoint[:i+1], endpoint[i+1:]
	}
	if _, port, err := net.SplitHostPort(rest); err == nil {
		return user + net.JoinHostPort(host, port)
	}
	return user + host
}
