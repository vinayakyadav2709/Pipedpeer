package daemonapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
)

// nodesState is the part of Server that keeps the node store fed: the peer
// poller, discovery, gossip, and how this node describes itself. Embedded in
// Server, so its fields read as s.<field>.
type nodesState struct {
	stopPoller chan struct{}

	// Discovery function for LAN scanning (set before StartPeerPoller)
	discoverFn func() []NodeDiscovered

	// How this node describes itself in the node store (set by SetSelfInfo).
	// selfHost is the address the last poll published (route-chosen), so a
	// change can drop the old self path.
	selfMu   sync.RWMutex
	selfIP   string
	selfPort int
	selfCaps map[string]string
	selfHost string

	// pending holds internet forwarders ("host:port") whose peer has not
	// answered /health yet, so its node id - and therefore its row - is not
	// known. Each poll retries them until they answer or RemovePeer drops them.
	pendingMu sync.Mutex
	pending   map[string]struct{}

	// pathGen counts RemovePeer calls; removedAt holds the generation each
	// address was removed at. A probe that started before a removal must not
	// re-add the path it dropped (PathOK recreates a missing path), so
	// verifying takes pathMu and checks removedAt against the generation the
	// probe started at. ponytail: entries are never deleted; bounded by the
	// forwarder ports ever used.
	pathMu    sync.Mutex
	pathGen   uint64
	removedAt map[string]uint64

	// introducerStatus and hinter come from internet mode (nil without it).
	hookMu           sync.RWMutex
	introducerStatus func() []IntroducerState
	hinter           func(node string, candidates []string)

	gossip gossipState
}

// NodeDiscovered is a node found by LAN discovery. Host is the packet's
// source address, never what the node advertised about itself (E4).
type NodeDiscovered struct {
	NodeID     string
	Host       string
	DaemonPort int
	Arch       string
	Hostname   string
}

// SetDiscoverFn installs the LAN discovery probe used by the peer poller.
// Guarded: AddPeer's immediate poll can already be running.
func (s *Server) SetDiscoverFn(fn func() []NodeDiscovered) {
	s.hookMu.Lock()
	s.discoverFn = fn
	s.hookMu.Unlock()
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.nodeList()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

// nodeList is the cluster as /v1/nodes serves it.
func (s *Server) nodeList() ([]nodeResp, error) {
	if s.store == nil {
		return []nodeResp{}, nil
	}
	dbNodes, err := s.store.ListAll()
	if err != nil {
		return nil, err
	}
	nodes := make([]nodeResp, 0, len(dbNodes))
	for _, n := range dbNodes {
		resp := toNodeResp(n)
		if resp.Reason == "" && resp.State == "unreachable" {
			resp.Reason = s.lastPathError(n.NodeID)
		}
		if resp.Reason != "" {
			resp.Capabilities[ReasonCapability] = resp.Reason
		}
		nodes = append(nodes, resp)
	}
	return nodes, nil
}

// lastPathError is the most useful failure the store holds for a node whose
// row carries no reason of its own: the best path's last error.
func (s *Server) lastPathError(nodeID string) string {
	paths, err := s.store.Paths(nodeID)
	if err != nil {
		log.Warn().Err(err).Str("peer", nodeID).Msg("could not read node paths for its reason")
		return ""
	}
	for _, p := range paths {
		if p.LastErr != "" {
			return p.LastErr
		}
	}
	if len(paths) == 0 {
		return "no address known"
	}
	return ""
}

// ReasonCapability carries why a peer cannot be reached. Kept in the
// capabilities map beside the top-level reason for consumers that decode rows
// into registry.NodeRecord.
const ReasonCapability = "unreachable_reason"

// PathCapability is how a peer is reached (lan, internet, self), mirrored
// from the top-level path for the same consumers.
const PathCapability = "path"

// withUnreachable used to merge internet mode's unreachable report into the
// rows. Rows carry their own state and reason now (B3: the report was keyed by
// fingerprint, rows by UUID, which made phantom rows), so it returns them as
// they are. ponytail: kept only so ui.go compiles; drop the call there.
func (s *Server) withUnreachable(nodes []nodeResp) []nodeResp { return nodes }

type unreachablePeer struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

// handleUnreachable serves GET /v1/peers/unreachable, sorted by peer: every
// row the poller found unreachable, with its reason.
func (s *Server) handleUnreachable(w http.ResponseWriter, r *http.Request) {
	out := []unreachablePeer{}
	nodes, err := s.nodeList()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for _, n := range nodes {
		if n.State == "unreachable" && n.Source != string(nodestore.PathSelf) {
			out = append(out, unreachablePeer{Peer: n.NodeID, Reason: n.Reason})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	writeJSON(w, http.StatusOK, out)
}

// pathName is the path a row is reached by as /v1/nodes shows it: lan,
// internet or self, "" with no active path. A verified gossip address is a
// direct TCP connection to the peer, the same thing as a LAN path.
func pathName(n nodestore.Node) string {
	if n.Host == "" {
		return ""
	}
	switch nodestore.PathKind(n.Source) {
	case nodestore.PathSelf:
		return "self"
	case nodestore.PathInternet:
		return "internet"
	}
	return "lan"
}

// nodeResp is a registry.NodeRecord as served by /v1/nodes, plus host, path
// and reason. The orchestrator decodes it straight back into
// registry.NodeRecord, so the shared JSON tags must match that type's.
type nodeResp struct {
	NodeID       string            `json:"node_id"`
	SSHEndpoint  string            `json:"ssh_endpoint"`
	DaemonPort   int               `json:"daemon_port"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
	State        string            `json:"state"`
	HealthScore  float64           `json:"health_score"`
	Source       string            `json:"source"`
	Host         string            `json:"host"`
	Path         string            `json:"path"`
	Reason       string            `json:"reason,omitempty"`
}

// staleAfter is how long a node may go unheard-from before /v1/nodes stops
// reporting it healthy. The poller runs every 10s, so this is four missed
// rounds: long enough to ride out a slow poll, far shorter than the prune
// that eventually deletes the row.
//
// This is the death filter for everything that reads the endpoint. Stored
// state is only ever as fresh as the last poll that wrote it, so a node whose
// daemon dies keeps its 'healthy' row until something rewrites it — and while
// the local daemon is down, nothing does. Callers filter on State == "healthy"
// (Coordinator.FindNode, ddpEligibleCount) and would otherwise offer work to a
// machine that has been gone for hours.
const staleAfter = 45 * time.Second

func toNodeResp(n nodestore.Node) nodeResp {
	// Prefer the full payload captured from the node's own /health; fall back
	// to the indexed columns for entries that have never been polled.
	caps := map[string]string{}
	if n.CapsJSON != "" {
		if err := json.Unmarshal([]byte(n.CapsJSON), &caps); err != nil {
			log.Warn().Err(err).Str("peer", n.NodeID).Msg("stored capabilities unreadable; showing indexed columns only")
			caps = map[string]string{}
		}
	}
	if caps["arch"] == "" && n.Arch != "" {
		caps["arch"] = n.Arch
	}
	if caps["hostname"] == "" && n.Hostname != "" {
		caps["hostname"] = n.Hostname
	}

	var load registry.LoadInfo
	if n.LoadJSON != "" {
		if err := json.Unmarshal([]byte(n.LoadJSON), &load); err != nil {
			log.Warn().Err(err).Str("peer", n.NodeID).Msg("stored load unreadable; showing indexed columns only")
		}
	}
	load.AvailableMemBytes = n.AvailableMem
	load.ActiveJobs = n.ActiveJobs
	load.ReservedMemBytes = n.ReservedMem
	if n.TotalMem > 0 {
		load.TotalMemBytes = n.TotalMem
	}
	if n.CPUPercent > 0 {
		load.CPUPercent = n.CPUPercent
	}

	state := n.State
	if state == "healthy" && time.Since(time.Unix(n.LastSeen, 0)) > staleAfter {
		state = "stale"
	}
	if state == "" {
		state = "unknown"
	}

	path := pathName(n)
	if path != "" {
		caps[PathCapability] = path
	}
	// Eight parsers read "root@<host>:22"; the decision is to keep it.
	ssh := ""
	if n.Host != "" {
		ssh = fmt.Sprintf("root@%s:22", n.Host)
	}

	return nodeResp{
		NodeID:       n.NodeID,
		SSHEndpoint:  ssh,
		DaemonPort:   n.Port,
		Capabilities: caps,
		Load:         load,
		State:        state,
		HealthScore:  n.HealthScore,
		Source:       string(n.Source),
		Host:         n.Host,
		Path:         path,
		Reason:       n.Reason,
	}
}

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
