package daemonapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// IntroducerState is how one introducer is doing, as internet mode reports it
// (same fields and JSON as internet.IntroducerState; daemonapi does not import
// internet).
type IntroducerState struct {
	Addr      string    `json:"addr"`
	LastReply time.Time `json:"last_reply"`
	LastErr   string    `json:"last_err,omitempty"`
	Members   int       `json:"members"`
}

// SetIntroducerStatus installs internet mode's per-introducer report.
func (s *Server) SetIntroducerStatus(f func() []IntroducerState) {
	s.hookMu.Lock()
	s.introducerStatus = f
	s.hookMu.Unlock()
}

// SetHinter installs internet mode's Hint: gossiped addresses of a node join
// the set it tries to punch.
func (s *Server) SetHinter(f func(node string, candidates []string)) {
	s.hookMu.Lock()
	s.hinter = f
	s.hookMu.Unlock()
}

// handleIntroducers serves GET /v1/introducers: [] without internet mode.
func (s *Server) handleIntroducers(w http.ResponseWriter, r *http.Request) {
	s.hookMu.RLock()
	f := s.introducerStatus
	s.hookMu.RUnlock()
	out := []IntroducerState{}
	if f != nil {
		if got := f(); got != nil {
			out = got
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// addrSigContext separates an address record signature from every other use
// of the node key.
const addrSigContext = "pipedpeer-addr/1"

// directPort is the port the direct QUIC transport listens on
// (internet.DefaultPort). A named copy: daemonapi does not import internet.
const directPort = 4243

// gossipEvery is the PEX limit: one pull from each healthy peer a minute.
const gossipEvery = time.Minute

// addrRecord is one member's signed statement of where it can be reached.
// Pub is the signer's public key; NodeID must be its fingerprint, so a record
// is checked without knowing the key beforehand and no member can forge
// another's.
type addrRecord struct {
	NodeID         string   `json:"node_id"`
	Seq            int64    `json:"seq"`
	Hosts          []string `json:"hosts"`
	DaemonPort     int      `json:"daemon_port"`
	DirectPort     int      `json:"direct_port"`
	IntroducerPort int      `json:"introducer_port"`
	Pub            string   `json:"pub"`
	Sig            []byte   `json:"sig,omitempty"`
}

// addressesResp is GET /v1/addresses.
type addressesResp struct {
	Self  *addrRecord  `json:"self"`
	Peers []addrRecord `json:"peers"`
}

// gossipState is the address records this daemon signed and learned.
type gossipState struct {
	mu       sync.Mutex
	self     *addrRecord           // cached until the host set changes
	records  map[string]addrRecord // node id → highest-seq valid record
	lastPull time.Time
}

// canonical is the record as signed: its JSON without the signature.
func (r addrRecord) canonical() []byte {
	r.Sig = nil
	b, err := json.Marshal(r)
	if err != nil {
		// A struct of strings and ints cannot fail to marshal.
		panic(fmt.Sprintf("address record: %v", err))
	}
	return b
}

// verify checks that the record is signed by the key it names and that the
// key is the node it claims to be.
func (r addrRecord) verify() error {
	pub, err := identity.DecodePublic(r.Pub)
	if err != nil {
		return fmt.Errorf("bad public key: %w", err)
	}
	if identity.Fingerprint(pub) != r.NodeID {
		return fmt.Errorf("key is node %s, record claims %s", identity.Fingerprint(pub), r.NodeID)
	}
	if !identity.Verify(pub, addrSigContext, r.canonical(), r.Sig) {
		return fmt.Errorf("signature does not verify")
	}
	return nil
}

// selfHosts is the routable addresses this node currently publishes:
// the address each poll chose and the start-time one. Loopback means nothing
// to another machine.
func (s *Server) selfHosts() ([]string, int) {
	s.selfMu.RLock()
	defer s.selfMu.RUnlock()
	var out []string
	for _, h := range []string{s.selfHost, s.selfIP} {
		if routable(h) && (len(out) == 0 || out[0] != h) {
			out = append(out, h)
		}
	}
	return out, s.selfPort
}

func routable(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && !ip.IsLoopback() && !ip.IsUnspecified()
}

// selfRecord signs this node's record, reusing the last one while the host
// set is unchanged. Seq is the signing time, so it rises across restarts.
func (s *Server) selfRecord() (*addrRecord, error) {
	hosts, port := s.selfHosts()
	s.gossip.mu.Lock()
	defer s.gossip.mu.Unlock()
	if c := s.gossip.self; c != nil && c.DaemonPort == port && equalStrings(c.Hosts, hosts) {
		return c, nil
	}
	key, err := identity.Key()
	if err != nil {
		return nil, fmt.Errorf("node key: %w", err)
	}
	seq := time.Now().UnixNano()
	if c := s.gossip.self; c != nil && seq <= c.Seq {
		seq = c.Seq + 1
	}
	rec := &addrRecord{
		NodeID:         key.Fingerprint(),
		Seq:            seq,
		Hosts:          append([]string{}, hosts...),
		DaemonPort:     port,
		DirectPort:     directPort,
		IntroducerPort: clustercfg.DefaultPort,
		Pub:            identity.EncodePublic(key.Public),
	}
	rec.Sig = key.Sign(addrSigContext, rec.canonical())
	s.gossip.self = rec
	return rec, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// handleAddresses serves GET /v1/addresses: this node's signed record and the
// records it learned, verbatim, so they travel on with their signatures.
func (s *Server) handleAddresses(w http.ResponseWriter, r *http.Request) {
	self, err := s.selfRecord()
	if err != nil {
		log.Warn().Err(err).Msg("cannot sign this node's address record")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.gossip.mu.Lock()
	peers := make([]addrRecord, 0, len(s.gossip.records))
	for _, rec := range s.gossip.records {
		peers = append(peers, rec)
	}
	s.gossip.mu.Unlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].NodeID < peers[j].NodeID })
	writeJSON(w, http.StatusOK, addressesResp{Self: self, Peers: peers})
}

// maybePullGossip asks each healthy peer for its address records, at most
// once a minute.
func (s *Server) maybePullGossip(healths map[string]*PeerHealth) {
	s.gossip.mu.Lock()
	if time.Since(s.gossip.lastPull) < gossipEvery {
		s.gossip.mu.Unlock()
		return
	}
	s.gossip.lastPull = time.Now()
	s.gossip.mu.Unlock()

	var wg sync.WaitGroup
	for _, ph := range healths {
		if ph == nil || ph.Status != "healthy" {
			continue
		}
		wg.Add(1)
		go func(ph *PeerHealth) {
			defer wg.Done()
			got, err := fetchAddresses(ph.Host, ph.Port)
			if err != nil {
				log.Warn().Err(err).Str("peer", ph.NodeID).Msg("address gossip pull failed")
				return
			}
			s.learnAddresses(ph.NodeID, got)
		}(ph)
	}
	wg.Wait()
}

func fetchAddresses(host string, port int) (addressesResp, error) {
	var out addressesResp
	client := &http.Client{Timeout: healthTimeout}
	resp, err := client.Get(fmt.Sprintf("http://%s/v1/addresses", net.JoinHostPort(host, strconv.Itoa(port))))
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("/v1/addresses answered %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("/v1/addresses unreadable: %w", err)
	}
	return out, nil
}

// learnAddresses takes the records one peer served. Each must verify and be
// newer than what is held; its addresses become candidates (a gossip path in
// the store, a hint to internet mode), never an active path.
func (s *Server) learnAddresses(from string, got addressesResp) {
	recs := got.Peers
	if got.Self != nil {
		recs = append([]addrRecord{*got.Self}, recs...)
	}
	for _, rec := range recs {
		if rec.NodeID == s.nodeID {
			continue
		}
		if err := rec.verify(); err != nil {
			log.Warn().Err(err).Str("from", from).Str("node", rec.NodeID).Msg("dropping address record")
			continue
		}
		s.gossip.mu.Lock()
		if s.gossip.records == nil {
			s.gossip.records = map[string]addrRecord{}
		}
		old, ok := s.gossip.records[rec.NodeID]
		fresh := !ok || rec.Seq > old.Seq
		if fresh {
			s.gossip.records[rec.NodeID] = rec
		}
		s.gossip.mu.Unlock()
		if !fresh {
			continue
		}
		s.applyRecord(rec)
	}
}

// applyRecord turns a fresh record into candidates.
func (s *Server) applyRecord(rec addrRecord) {
	var hints []string
	for _, h := range rec.Hosts {
		if !routable(h) {
			continue // another machine's loopback is ours
		}
		if s.store != nil && rec.DaemonPort > 0 && !s.isStranger(rec.NodeID, h, rec.DaemonPort) {
			if err := s.store.SeePath(rec.NodeID, nodestore.PathGossip, h, rec.DaemonPort); err != nil {
				log.Warn().Err(err).Str("node", rec.NodeID).Str("host", h).Msg("could not record gossiped address")
			}
		}
		if rec.DirectPort > 0 && netaddr.Probeable(h) {
			hints = append(hints, net.JoinHostPort(h, strconv.Itoa(rec.DirectPort)))
		}
	}
	s.hookMu.RLock()
	hint := s.hinter
	s.hookMu.RUnlock()
	if hint != nil && len(hints) > 0 {
		hint(rec.NodeID, hints)
	}
}
