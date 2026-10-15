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

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
	"github.com/pipedpeer/pipedpeer/internal/registry"
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

	// strangers holds "nodeID|host:port" candidates where something answered
	// that could not prove it holds the cluster token while another path of
	// that node worked: on a network that reuses the node's subnet the
	// address is someone else's machine (E6). They are dropped, logged once
	// and not re-learnt from gossip until the node has no working path.
	// ponytail: in memory, so a restart probes each once more.
	strangerMu sync.Mutex
	strangers  map[string]bool
}

func strangerKey(nodeID, host string, port int) string {
	return nodeID + "|" + net.JoinHostPort(host, strconv.Itoa(port))
}

func (s *Server) isStranger(nodeID, host string, port int) bool {
	s.strangerMu.Lock()
	defer s.strangerMu.Unlock()
	return s.strangers[strangerKey(nodeID, host, port)]
}

// forgetStrangers lets a node's refused candidates be learnt again, for when
// none of its paths works any more.
func (s *Server) forgetStrangers(nodeID string) {
	s.strangerMu.Lock()
	defer s.strangerMu.Unlock()
	for k := range s.strangers {
		if strings.HasPrefix(k, nodeID+"|") {
			delete(s.strangers, k)
		}
	}
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
		if n.Source != string(nodestore.PathSelf) {
			resp.Paths = s.pathList(n.NodeID)
		}
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

// pathResp is one address of a node as /v1/nodes serves it, so a reader
// (doctor) can judge the node by the path that works rather than the first.
type pathResp struct {
	Kind   string `json:"kind"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Active bool   `json:"active,omitempty"`
	OK     bool   `json:"ok,omitempty"` // verified and still answering
	Err    string `json:"err,omitempty"`
}

func (s *Server) pathList(nodeID string) []pathResp {
	paths, err := s.store.Paths(nodeID)
	if err != nil {
		log.Warn().Err(err).Str("peer", nodeID).Msg("could not read node paths for /v1/nodes")
		return nil
	}
	out := make([]pathResp, 0, len(paths))
	for _, p := range paths {
		out = append(out, pathResp{Kind: string(p.Kind), Host: p.Host, Port: p.Port, Active: p.Active,
			OK: !p.LastOK.IsZero() && p.LastErr == "", Err: p.LastErr})
	}
	return out
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
// internet, gossip or self, "" with no active path. A gossip address is named
// for where it came from (another member's record), never "lan": nothing
// here saw it on this network (E6 showed a cut-off peer as "lan").
func pathName(n nodestore.Node) string {
	if n.Host == "" {
		return ""
	}
	switch nodestore.PathKind(n.Source) {
	case nodestore.PathSelf:
		return "self"
	case nodestore.PathInternet:
		return "internet"
	case nodestore.PathGossip:
		return "gossip"
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
	Paths        []pathResp        `json:"paths,omitempty"`
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
