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

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// nodesState is the part of Server that keeps the node store fed: the peer
// poller, discovery, and how this node describes itself. Embedded in Server,
// so its fields read as s.<field>.
type nodesState struct {
	// pathReporter says how each peer is reached, keyed by local address.
	// Set by internet mode; nil when nothing is joined over the internet.
	pathReporter func() map[string]string
	// unreachableReporter names the internet peers with no direct path, and
	// why (fingerprint → reason). Set by internet mode.
	unreachableReporter func() map[string]string

	stopPoller chan struct{}

	// Discovery function for mDNS scanning (set before StartPeerPoller)
	discoverFn func() []NodeDiscovered

	// How this node describes itself in the node store (set by SetSelfInfo)
	selfMu   sync.RWMutex
	selfSSH  string
	selfPort int
	selfCaps map[string]string
}

// NodeDiscovered represents a node found via mDNS or other discovery.
type NodeDiscovered struct {
	NodeID      string
	SSHEndpoint string
	DaemonPort  int
	Arch        string
	Hostname    string
}

// SetDiscoverFn installs the LAN discovery probe used by the peer poller.
// Must be called before StartPeerPoller.
func (s *Server) SetDiscoverFn(fn func() []NodeDiscovered) {
	s.discoverFn = fn
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

	// How each peer is reached, when anything knows. Carried in the
	// capabilities map rather than as a new field, so an older orchestrator
	// decoding this into a registry.NodeRecord keeps working and simply
	// ignores it.
	paths := s.reportedPaths()

	nodes := make([]nodeResp, 0, len(dbNodes))
	for _, n := range dbNodes {
		resp := toNodeResp(n)
		if p := pathFor(n, paths); p != "" {
			if resp.Capabilities == nil {
				resp.Capabilities = map[string]string{}
			}
			resp.Capabilities[PathCapability] = p
		}
		nodes = append(nodes, resp)
	}
	return nodes, nil
}

// ReasonCapability carries why a peer cannot be reached.
const ReasonCapability = "unreachable_reason"

// withUnreachable marks the rows the unreachable reporter names (State
// "unreachable", the reason under ReasonCapability) and appends a row for each
// such peer the store never held. Only /v1/stats uses it: `pipedpeer nodes`
// prints /v1/peers/unreachable itself.
func (s *Server) withUnreachable(nodes []nodeResp) []nodeResp {
	s.mu.Lock()
	f := s.unreachableReporter
	s.mu.Unlock()
	if f == nil {
		return nodes
	}
	marks := f()
	if len(marks) == 0 {
		return nodes
	}
	left := make(map[string]string, len(marks))
	for id, reason := range marks {
		left[id] = reason
	}
	for i := range nodes {
		reason, ok := left[nodes[i].NodeID]
		if !ok {
			continue
		}
		delete(left, nodes[i].NodeID)
		caps := make(map[string]string, len(nodes[i].Capabilities)+1)
		for k, v := range nodes[i].Capabilities {
			caps[k] = v
		}
		caps[ReasonCapability] = reason
		nodes[i].Capabilities = caps
		nodes[i].State = "unreachable"
	}
	ids := make([]string, 0, len(left))
	for id := range left {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		nodes = append(nodes, nodeResp{
			NodeID:       id,
			Capabilities: map[string]string{ReasonCapability: left[id]},
			State:        "unreachable",
			Source:       "internet",
		})
	}
	return nodes
}

// PathCapability is where the route to a peer travels.
const PathCapability = "path"

// SetPathReporter tells the daemon how peers are being reached.
//
// Set by internet mode, which is the only thing that knows: a peer there is
// behind a local forwarder and could be reached by a punched connection, a
// mapped port, or nothing at all. Without this a machine that has quietly
// stopped serving looks the same as one that was never there, which is the
// failure this exists to prevent.
func (s *Server) SetPathReporter(f func() map[string]string) {
	s.mu.Lock()
	s.pathReporter = f
	s.mu.Unlock()
}

func (s *Server) reportedPaths() map[string]string {
	s.mu.Lock()
	f := s.pathReporter
	s.mu.Unlock()
	if f == nil {
		return nil
	}
	return f()
}

// SetUnreachableReporter tells the daemon which internet peers it cannot
// reach and why. Such a peer has no forwarder, so it never shows up in
// /v1/nodes; without this it would vanish without a word.
func (s *Server) SetUnreachableReporter(f func() map[string]string) {
	s.mu.Lock()
	s.unreachableReporter = f
	s.mu.Unlock()
}

type unreachablePeer struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

// handleUnreachable serves GET /v1/peers/unreachable, sorted by peer. A
// separate route because /v1/nodes rows decode into registry.NodeRecord
// across many consumers.
func (s *Server) handleUnreachable(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	f := s.unreachableReporter
	s.mu.Unlock()
	out := []unreachablePeer{}
	if f != nil {
		for peer, reason := range f() {
			out = append(out, unreachablePeer{Peer: peer, Reason: reason})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	writeJSON(w, http.StatusOK, out)
}

// pathFor is how a node is reached, or "" when nothing can say.
//
// A peer behind a local forwarder is only knowable from the manager that made
// it. Anything else with a routable address is reached by talking straight to
// it, which is a direct path by construction - no relay is involved and never
// was - so that much can be stated without asking anyone.
func pathFor(n nodestore.Node, paths map[string]string) string {
	if n.Source == "self" {
		return "self"
	}
	addr := net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
	if p, ok := paths[addr]; ok && p != "" {
		return p
	}
	if ip := net.ParseIP(n.Host); ip != nil && ip.IsLoopback() {
		// A loopback address that no manager claims is a forwarder whose
		// link has gone, or a second daemon on this machine. Saying "lan"
		// here would be inventing a route.
		return ""
	}
	return "lan"
}

// nodeResp is a registry.NodeRecord as served by /v1/nodes. The orchestrator
// decodes it straight back into registry.NodeRecord, so the JSON tags must
// match that type's.
type nodeResp struct {
	NodeID       string            `json:"node_id"`
	SSHEndpoint  string            `json:"ssh_endpoint"`
	DaemonPort   int               `json:"daemon_port"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
	State        string            `json:"state"`
	HealthScore  float64           `json:"health_score"`
	Source       string            `json:"source"`
}

// staleAfter is how long a node may go unheard-from before /v1/nodes stops
// reporting it healthy. The poller runs every 10s, so this is four missed
// rounds: long enough to ride out a slow poll, far shorter than the
// staleNodeTTL that eventually deletes the row.
//
// This is the death filter for everything that reads the endpoint. Stored
// state is only ever as fresh as the last poll that wrote it, so a node whose
// daemon dies keeps its 'healthy' row until something rewrites it — and while
// the local daemon is down, nothing does. Callers filter on State == "healthy"
// (Coordinator.FindNode, ddpEligibleCount) and would otherwise offer work to a
// machine that has been gone for hours.
const staleAfter = 45 * time.Second

func toNodeResp(n nodestore.Node) nodeResp {
	ssh := n.SSHEndpoint
	if ssh == "" {
		ssh = fmt.Sprintf("root@%s:22", n.Host)
	}

	// Prefer the full payload captured from the node's own /health; fall back
	// to the indexed columns for entries that have never been polled.
	caps := map[string]string{}
	if n.CapsJSON != "" {
		_ = json.Unmarshal([]byte(n.CapsJSON), &caps)
	}
	if caps["arch"] == "" && n.Arch != "" {
		caps["arch"] = n.Arch
	}
	if caps["hostname"] == "" && n.Hostname != "" {
		caps["hostname"] = n.Hostname
	}

	var load registry.LoadInfo
	if n.LoadJSON != "" {
		_ = json.Unmarshal([]byte(n.LoadJSON), &load)
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

	return nodeResp{
		NodeID:       n.NodeID,
		SSHEndpoint:  ssh,
		DaemonPort:   n.Port,
		Capabilities: caps,
		Load:         load,
		State:        state,
		HealthScore:  n.HealthScore,
		Source:       n.Source,
	}
}

func (s *Server) handleNodesAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Host == "" || req.Port == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "host and port required"})
		return
	}
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
		return
	}
	if err := s.store.AddManual(req.Host, req.Port); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Poll it now rather than waiting for the next sweep. A peer that is
	// known but not yet marked healthy is invisible to closure broadcast and
	// to spill, so a job submitted in the seconds after `nodes add` runs
	// entirely local and nothing explains why.
	go s.pollAllNodes()
	writeJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

// AddPeer registers a peer at host:port and polls it immediately, the same way
// `pipedpeer nodes add` does. Exported so internet mode can register the local
// ports it puts in front of remote peers, without going back out through HTTP
// to reach its own daemon.
func (s *Server) AddPeer(host string, port int) error {
	if s.store == nil {
		return fmt.Errorf("node store unavailable")
	}
	if err := s.store.AddManual(host, port); err != nil {
		return err
	}
	go s.pollAllNodes()
	return nil
}

// RemovePeer forgets a peer, for when internet mode's local port goes away.
func (s *Server) RemovePeer(host string, port int) error {
	if s.store == nil {
		return fmt.Errorf("node store unavailable")
	}
	_, err := s.store.RemoveManualAt(host, port)
	return err
}

func (s *Server) handleNodesRemove(w http.ResponseWriter, r *http.Request) {
	host := chi.URLParam(r, "host")
	if host == "_all" {
		if s.store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
			return
		}
		if err := s.store.RemoveAll(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "all removed"})
		return
	}
	if host == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "host required"})
		return
	}
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store unavailable"})
		return
	}
	// The target may be a host (manual entries) or a node ID — full or the
	// short prefix `pipedpeer nodes` prints. Deleting nothing is an error:
	// a 200 no-op left ghost nodes that "removal" could never clear.
	n, err := s.store.RemoveManual(host)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if n == 0 {
		nodeID, err := s.store.ResolveNodeID(host)
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		if nodeID == "" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no node matches " + host})
			return
		}
		if nodeID == s.nodeID {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "refusing to remove this node's own entry"})
			return
		}
		if err := s.store.DeleteNode(nodeID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

// StartPeerPoller launches a background goroutine that discovers nodes via mDNS
// and polls health from both discovered and manually added nodes.
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

// extractHost pulls "host" from "user@host:port" or "host:port" or "host".
func extractHost(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx != -1 {
		s = s[idx+1:]
	}
	if idx := strings.LastIndex(s, ":"); idx != -1 {
		s = s[:idx]
	}
	return s
}

// SetSelfInfo tells the daemon how to describe itself in the node store, so
// this machine is a placement candidate for every peer that polls it and shows
// up in its own node list.
func (s *Server) SetSelfInfo(sshEndpoint string, daemonPort int, caps map[string]string) {
	s.selfMu.Lock()
	s.selfSSH = sshEndpoint
	s.selfPort = daemonPort
	s.selfCaps = caps
	s.selfMu.Unlock()
}

// pollAllNodes is one refresh cycle of cluster membership:
//
//	discover on the LAN → record self → health-check every peer → prune the dead.
//
// Everything the orchestrator later reads from /v1/nodes is written here.
func (s *Server) pollAllNodes() {
	if s.store == nil {
		return
	}

	// 1. mDNS discovery. New nodes are inserted with state "unknown" and get
	//    promoted to healthy by the health check below.
	if s.discoverFn != nil {
		for _, d := range s.discoverFn() {
			if d.NodeID == "" || d.NodeID == s.nodeID {
				continue
			}
			_ = s.store.UpsertNode(nodestore.Node{
				NodeID:      d.NodeID,
				Host:        extractHost(d.SSHEndpoint),
				Port:        d.DaemonPort,
				SSHEndpoint: d.SSHEndpoint,
				Arch:        d.Arch,
				Hostname:    d.Hostname,
				State:       "unknown",
				Source:      "discovery",
			})
		}
	}

	// 2. Record self. Self is always a placement candidate, so it belongs in
	//    the same table as everyone else rather than being special-cased.
	s.upsertSelf()

	// 3. Health-check every known peer concurrently.
	nodes, err := s.store.ListAll()
	if err != nil {
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
			mu.Lock()
			finalHealths[fmt.Sprintf("%s:%d", node.Host, node.Port)] = ph
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

	// 4. Forget auto-discovered nodes that have been gone a while. Manual
	//    entries survive — see nodestore.PruneStale.
	s.store.PruneStale(staleNodeTTL)

	// 5. Keep the list of signing keys this cluster uses current, so nix can
	//    require a signature and still accept peer closures.
	s.refreshTrustedKeys()
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

// staleNodeTTL is how long a discovered node may go unseen before it is
// dropped: long enough to ride out a daemon restart, short enough that a
// machine taken off the LAN stops being offered work.
const staleNodeTTL = 5 * time.Minute

func (s *Server) pollOne(node nodestore.Node) *PeerHealth {
	source := node.Source
	if source == "" {
		source = "discovery"
	}
	ph := &PeerHealth{Host: node.Host, Port: node.Port, NodeID: node.NodeID, Source: source}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s:%d/health", node.Host, node.Port))
	if err != nil {
		// A pin mismatch is not the same as a machine being down, and
		// reporting it as "unreachable" sends the operator to check cables
		// for a node that is answering. It means the peer's certificate
		// changed - a reinstall, or someone in the middle - and only they
		// can tell which.
		if strings.Contains(err.Error(), "certificate for") {
			log.Warn().Str("peer", fmt.Sprintf("%s:%d", node.Host, node.Port)).Err(err).
				Msg("peer certificate changed; run `pipedpeer auth forget <host:port>` if it was reinstalled")
		}
		ph.Status = "unreachable"
		s.store.MarkUnreachable(node.NodeID)
		return ph
	}
	defer resp.Body.Close()

	var h healthResponse
	if json.NewDecoder(resp.Body).Decode(&h) != nil || h.NodeID == "" {
		ph.Status = "unreachable"
		s.store.MarkUnreachable(node.NodeID)
		return ph
	}

	ph.NodeID = h.NodeID
	ph.Status = "healthy"
	ph.ActiveJobs = h.ActiveJobs
	ph.AvailableMem = h.AvailableMem
	ph.TotalCPUs = h.Load.TotalCPUs
	ph.ReservedMem = h.ReservedMem
	ph.Machine = h.Capabilities[heartbeat.MachineCapability]
	ph.ClosureFormats = h.Capabilities[heartbeat.ClosureFormatCapability]
	ph.GPU = h.Capabilities["gpu"]

	capsJSON, _ := json.Marshal(h.Capabilities)
	loadJSON, _ := json.Marshal(h.Load)

	updated := nodestore.Node{
		NodeID:       h.NodeID,
		Host:         node.Host,
		Port:         node.Port,
		SSHEndpoint:  node.SSHEndpoint,
		Arch:         h.Capabilities["arch"],
		Hostname:     h.Capabilities["hostname"],
		ActiveJobs:   h.ActiveJobs,
		AvailableMem: h.AvailableMem,
		ReservedMem:  h.ReservedMem,
		TotalMem:     h.Load.TotalMemBytes,
		CPUPercent:   h.Load.CPUPercent,
		Source:       source,
		IsManual:     node.IsManual,
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	}
	if err := s.store.MarkHealthy(updated); err != nil {
		return ph
	}

	// A manual entry added before the node was up carries a placeholder
	// "manual-host:port" ID. Now that the real UUID is known, drop the
	// placeholder row so the node is not counted twice.
	if node.NodeID != h.NodeID && strings.HasPrefix(node.NodeID, "manual-") {
		s.store.DeleteNode(node.NodeID)
	}
	return ph
}

// upsertSelf writes this node's own live state into the store.
func (s *Server) upsertSelf() {
	s.selfMu.RLock()
	ssh, port, caps := s.selfSSH, s.selfPort, s.selfCaps
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
	capsJSON, _ := json.Marshal(caps)
	loadJSON, _ := json.Marshal(load)

	// Both fields, not just Host: peers read the endpoint, and publishing a
	// routable Host beside an unroutable endpoint fixes nothing.
	host := s.advertisedHost(extractHost(ssh))
	ssh = rewriteHost(ssh, host)

	_ = s.store.MarkHealthy(nodestore.Node{
		NodeID:       s.nodeID,
		Host:         host,
		Port:         port,
		SSHEndpoint:  ssh,
		Arch:         caps["arch"],
		Hostname:     caps["hostname"],
		ActiveJobs:   s.ActiveJobs(),
		AvailableMem: load.AvailableMemBytes,
		ReservedMem:  s.ReservedMem(),
		TotalMem:     load.TotalMemBytes,
		CPUPercent:   load.CPUPercent,
		Source:       "self",
		CapsJSON:     string(capsJSON),
		LoadJSON:     string(loadJSON),
	})
}

// advertisedHost is the address this node publishes to its peers.
//
// The interface-list guess is only right when a machine has one route to the
// world. A laptop holding a stale 192.168.x lease published that while its
// peers were reachable only over an overlay, and nothing complained: the node
// simply advertised an address no peer could dial. DDP appeared to work
// because the reachable machine happened to draw rank 0; with the roles
// reversed the sync endpoint would have been unreachable for the other rank.
//
// So ask the kernel which of our addresses it would use to reach the peers we
// already know about, and publish that.
func (s *Server) advertisedHost(fallback string) string {
	if s.store == nil {
		return fallback
	}
	nodes, err := s.store.ListAll()
	if err != nil {
		return fallback
	}
	var peers []string
	for _, n := range nodes {
		if n.NodeID == s.nodeID || n.Host == "" || n.Port == 0 {
			continue
		}
		// Only peers we can currently reach get a vote. Unreachable rows
		// outlive whatever they described - manual entries never expire at
		// all - and letting a machine that is long gone decide how we
		// describe ourselves is how the stale answer survives.
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
