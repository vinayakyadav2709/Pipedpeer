package daemonapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/peercache"
)

const peerHeaderWait = 10 * time.Second

// Seams for tests.
var (
	localMachine = heartbeat.Machine
	localSystem  = identity.System
	trustedKeys  = peercache.TrustedKeys
)

// cachePeers returns the peers worth asking for a store path: healthy, same
// nix system (store paths differ by system, and an old peer with no Arch has
// no /v1/store either), and not this machine. Role is not filtered:
// introducer-only and pool-only peers serve their store too.
func (s *Server) cachePeers() []PeerHealth {
	machine, system := localMachine(), localSystem()
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	var out []PeerHealth
	byNode := map[string]int{} // NodeID -> index in out
	for _, ph := range s.peerHealths {
		switch {
		case ph.Status != "healthy" || ph.Host == "" || ph.Port == 0:
		case ph.NodeID == s.nodeID:
		case machine != "" && ph.Machine == machine:
		case ph.Arch == "" || ph.Arch != system:
		default:
			if i, ok := byNode[ph.NodeID]; ok && ph.NodeID != "" {
				// One node under several addresses: keep the nearest.
				if addrRank(*ph) < addrRank(out[i]) || (addrRank(*ph) == addrRank(out[i]) && peerKey(*ph) < peerKey(out[i])) {
					out[i] = *ph
				}
				continue
			}
			byNode[ph.NodeID] = len(out)
			out = append(out, *ph)
		}
	}
	return out
}

// addrRank orders a node's addresses: LAN (private) first, other routable
// ones next, and loopback last (that is the internet forwarder, a QUIC path
// unfit for multi-GB NARs when a LAN address exists).
func addrRank(p PeerHealth) int {
	ip := net.ParseIP(p.Host)
	switch {
	case ip == nil:
		return 1
	case ip.IsLoopback():
		return 3
	case ip.IsPrivate() || ip.IsLinkLocalUnicast():
		return 0
	}
	return 2
}

// peerGet issues GET path against a peer's daemon. The response header must
// arrive within peerHeaderWait; the body is bounded only by ctx, because a
// NAR can be large.
var peerGet = func(ctx context.Context, p PeerHealth, path string) (*http.Response, error) {
	url := "http://" + net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) + path
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	timer := time.AfterFunc(peerHeaderWait, cancel)
	resp, err := http.DefaultClient.Do(req)
	timer.Stop()
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

type peerAnswer struct {
	peer    PeerHealth
	body    []byte // nil unless acceptable
	narHash string // the accepted narinfo's NarHash
	busy    bool   // the peer answered 503
	// clean is true when the peer answered definitively (404, or a narinfo we
	// rejected), false for a timeout or transport error.
	clean bool
}

// fanOutNarinfo asks every peer at once and returns the first acceptable
// answer. clean reports whether every peer that was heard from answered
// definitively, so a miss may be remembered. With no hit and a peer that
// answered 503, the hit is busy: not a miss. rest collects the slower peers
// that answer with the winner's NarHash (they can serve the NAR if the winner
// cannot) and calls add for each until the budget ends; the caller runs it
// after remembering the hit. Callers must run rest even for a miss.
func fanOutNarinfo(peers []PeerHealth, hash string) (hit cacheHit, clean bool, rest func(add func(PeerHealth))) {
	ctx, cancel := context.WithTimeout(context.Background(), narinfoBudget)
	trusted := trustedKeys()
	ch := make(chan peerAnswer, len(peers))
	for _, p := range peers {
		go func() { ch <- askPeer(ctx, p, hash, trusted) }()
	}
	clean = true
	busy := false
	for i := range peers {
		a := <-ch
		if a.body == nil {
			clean = clean && a.clean
			busy = busy || a.busy
			continue
		}
		hit = cacheHit{body: a.body, peers: []PeerHealth{a.peer}}
		left := len(peers) - i - 1
		return hit, true, func(add func(PeerHealth)) {
			defer cancel()
			for ; left > 0; left-- {
				select {
				case o := <-ch:
					// A different NarHash is a different build of the path;
					// nix would reject that NAR.
					if o.body != nil && o.narHash == a.narHash {
						add(o.peer)
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}
	cancel()
	if !clean {
		log.Warn().Str("hash", hash).Int("peers", len(peers)).Msg("nix cache: no peer answered in time")
	}
	return cacheHit{busy: busy}, clean && !busy, func(func(PeerHealth)) {}
}

// peerWarnEvery is how often one peer may be warned about.
const peerWarnEvery = 30 * time.Second

var (
	peerWarnMu   sync.Mutex
	peerWarnLast = map[string]time.Time{}
)

// warnPeer logs a warning about peer at most once per peerWarnEvery, so a
// dead peer does not warn on every lookup.
func warnPeer(p PeerHealth, err error, msg string, fields map[string]any) {
	peerWarnMu.Lock()
	now := cacheNow()
	// Per peer and message: a NAR failure must not hide a later "narinfo
	// rejected" from the same peer. A peer with no NodeID is told by address.
	key := p.NodeID
	if key == "" {
		key = net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
	}
	key += "|" + msg
	if last, ok := peerWarnLast[key]; ok && now.Sub(last) < peerWarnEvery {
		peerWarnMu.Unlock()
		return
	}
	peerWarnLast[key] = now
	peerWarnMu.Unlock()
	ev := log.Warn().Err(err).Str("peer", p.NodeID)
	for k, v := range fields {
		ev = ev.Interface(k, v)
	}
	ev.Msg(msg)
}

func peerKey(p PeerHealth) string {
	return net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) + "/" + p.NodeID
}

// askPeer fetches and checks one peer's narinfo.
func askPeer(ctx context.Context, p PeerHealth, hash string, trusted []signature.PublicKey) peerAnswer {
	a := peerAnswer{peer: p}
	resp, err := peerGet(ctx, p, "/v1/store/narinfo/"+hash)
	if err != nil {
		if ctx.Err() == nil {
			warnPeer(p, err, "nix cache: narinfo request failed", map[string]any{"hash": hash})
		}
		return a
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		a.clean = true
		return a
	}
	if resp.StatusCode != http.StatusOK {
		a.busy = resp.StatusCode == http.StatusServiceUnavailable
		warnPeer(p, nil, "nix cache: peer refused the narinfo", map[string]any{"hash": hash, "status": resp.StatusCode})
		return a
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		if ctx.Err() == nil {
			warnPeer(p, err, "nix cache: narinfo read failed", map[string]any{"hash": hash})
		}
		return a
	}
	a.clean = true
	ni, err := narinfo.Parse(bytes.NewReader(raw))
	if err == nil {
		err = peercache.Accept(ni, hash, trusted)
	}
	if err != nil {
		warnPeer(p, err, "nix cache: narinfo rejected", map[string]any{"hash": hash, "addr": fmt.Sprintf("%s:%d", p.Host, p.Port)})
		return a
	}
	a.body, a.narHash = raw, ni.NarHash.String()
	return a
}
