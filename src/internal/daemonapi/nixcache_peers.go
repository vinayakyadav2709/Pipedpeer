package daemonapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
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
	for _, ph := range s.peerHealths {
		switch {
		case ph.Status != "healthy" || ph.Host == "" || ph.Port == 0:
		case ph.NodeID == s.nodeID:
		case machine != "" && ph.Machine == machine:
		case ph.Arch == "" || ph.Arch != system:
		default:
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
	peer PeerHealth
	body []byte // nil unless acceptable
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
	failed := map[string]bool{}
	for range peers {
		a := <-ch
		if a.body != nil {
			hit := cacheHit{body: a.body, peers: []PeerHealth{a.peer}}
			// Peers still pending may hold it too; the NAR fetch tries them
			// after the winner and treats a 404 as a miss.
			for _, p := range peers {
				if p.NodeID != a.peer.NodeID || p.Host != a.peer.Host || p.Port != a.peer.Port {
					if !failed[peerKey(p)] {
						hit.peers = append(hit.peers, p)
					}
				}
			}
			return hit, true
		}
		failed[peerKey(a.peer)] = true
		clean = clean && a.clean
	}
	if !clean {
		log.Warn().Str("hash", hash).Int("peers", len(peers)).Msg("nix cache: no peer answered in time")
	}
	return cacheHit{}, clean
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
			log.Warn().Err(err).Str("hash", hash).Str("peer", p.NodeID).Msg("nix cache: narinfo request failed")
		}
		return a
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		a.clean = true
		return a
	}
	if resp.StatusCode != http.StatusOK {
		log.Warn().Int("status", resp.StatusCode).Str("hash", hash).Str("peer", p.NodeID).Msg("nix cache: peer refused the narinfo")
		return a
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		if ctx.Err() == nil {
			log.Warn().Err(err).Str("hash", hash).Str("peer", p.NodeID).Msg("nix cache: narinfo read failed")
		}
		return a
	}
	a.clean = true
	ni, err := narinfo.Parse(bytes.NewReader(raw))
	if err == nil {
		err = peercache.Accept(ni, hash, trusted)
	}
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Str("peer", p.NodeID).
			Str("addr", fmt.Sprintf("%s:%d", p.Host, p.Port)).Msg("nix cache: narinfo rejected")
		return a
	}
	a.body = raw
	return a
}
