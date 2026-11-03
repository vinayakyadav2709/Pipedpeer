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
	seen := map[string]bool{}
	for _, ph := range s.peerHealths {
		switch {
		case ph.Status != "healthy" || ph.Host == "" || ph.Port == 0:
		case ph.NodeID == s.nodeID:
		case machine != "" && ph.Machine == machine:
		case ph.Arch == "" || ph.Arch != system:
		case ph.NodeID != "" && seen[ph.NodeID]: // one node under several addresses
		default:
			seen[ph.NodeID] = true
			out = append(out, *ph)
		}
	}
	return out
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
	// clean is true when the peer answered definitively (404, or a narinfo we
	// rejected), false for a timeout or transport error.
	clean bool
}

// fanOutNarinfo asks every peer at once and returns the first acceptable
// answer (the others are cancelled). clean reports whether every peer that
// was heard from answered definitively, so a miss may be remembered.
func fanOutNarinfo(peers []PeerHealth, hash string) (cacheHit, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), narinfoBudget)
	defer cancel()
	trusted := trustedKeys()
	ch := make(chan peerAnswer, len(peers))
	for _, p := range peers {
		go func() { ch <- askPeer(ctx, p, hash, trusted) }()
	}
	clean := true
	for i := range peers {
		a := <-ch
		if a.body == nil {
			clean = clean && a.clean
			continue
		}
		hit := cacheHit{body: a.body, peers: []PeerHealth{a.peer}}
		// Other peers that already answered with the same NarHash can serve
		// the NAR if the winner cannot. A different NarHash means a different
		// build of the path; nix would reject that NAR.
		grace := time.NewTimer(fallbackGrace)
		for left := len(peers) - i - 1; left > 0; left-- {
			select {
			case o := <-ch:
				if o.body != nil && o.narHash == a.narHash {
					hit.peers = append(hit.peers, o.peer)
				}
				continue
			case <-grace.C:
			}
			break
		}
		grace.Stop()
		return hit, true
	}
	if !clean {
		log.Warn().Str("hash", hash).Int("peers", len(peers)).Msg("nix cache: no peer answered in time")
	}
	return cacheHit{}, clean
}

// fallbackGrace is how long a hit waits for the slower peers' answers, to
// have more than one NAR source. A variable so tests can shorten it.
var fallbackGrace = 100 * time.Millisecond

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
	if last, ok := peerWarnLast[p.NodeID]; ok && now.Sub(last) < peerWarnEvery {
		peerWarnMu.Unlock()
		return
	}
	peerWarnLast[p.NodeID] = now
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
