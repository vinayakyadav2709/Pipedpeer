// Package relay carries traffic between peers that cannot reach each other
// directly.
//
// Measurement decided both halves of this design. One of the two development
// machines is on a mobile carrier's NAT, which hands out a different external
// port per destination: the address a peer would be given was never valid for
// that peer, and no hole punch to it has ever succeeded. So a relay is not a
// fallback for unlucky networks, it is required for ordinary ones.
//
// And it speaks QUIC, because the public host's inbound TCP is restricted to
// ports that already belong to other services while its UDP is open. That is
// not a quirk of one machine - a relay has to work from wherever it is put -
// but it does mean the transport must be able to live entirely on UDP, and
// QUIC is that with reliable ordered streams rather than a hand-rolled
// retransmit loop nobody should be writing.
//
// The relay carries bytes and knows nothing about them. Peers address each
// other by node id and everything above that - HTTP, the job protocol, the
// gradient sync - travels unchanged.
package relay

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/quic-go/quic-go"
)

// ALPN identifies this protocol during the TLS handshake, so a relay and
// something else on the same port cannot be confused for one another.
const ALPN = "pipedpeer-relay/1"

// maxHeader bounds the JSON preamble on a stream, so a peer cannot make the
// relay buffer without limit before it has said anything useful.
const maxHeader = 4096

// hello is the first thing a client says on a fresh connection.
//
// The node is not named, it is proved. A relay introduces strangers, and a
// peer that simply asserts a name asserts whoever's name it likes - so the
// address a peer is registered under is the fingerprint of the key it signs
// with, and taking somebody else's requires their private half rather than
// their spelling.
type hello struct {
	Cluster string `json:"cluster"`
	// PubKey is base64 ed25519. The registered address is its fingerprint.
	PubKey string `json:"pubkey"`
	// Nonce is fresh per connection and signed, so a hello captured from the
	// wire cannot be replayed to claim the same identity later.
	Nonce string `json:"nonce"`
	Sig   string `json:"sig"`
}

// helloContext binds a hello's signature to this purpose, so it cannot be
// replayed as any other signed message.
const helloContext = "relay-hello"

// verify checks the signature and returns the address this peer is entitled
// to be registered under.
func (h hello) verify() (string, error) {
	if h.Cluster == "" || h.PubKey == "" || h.Nonce == "" || h.Sig == "" {
		return "", fmt.Errorf("hello must carry a cluster, a public key, a nonce and a signature")
	}
	pub, err := identity.DecodePublic(h.PubKey)
	if err != nil {
		return "", fmt.Errorf("public key: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(h.Sig)
	if err != nil {
		return "", fmt.Errorf("signature: %w", err)
	}
	if !identity.Verify(pub, helloContext, []byte(h.Cluster+"|"+h.Nonce), sig) {
		return "", fmt.Errorf("signature does not match the public key offered")
	}
	return identity.Fingerprint(pub), nil
}

// openReq is the first thing written on a stream that wants to reach a peer.
type openReq struct {
	To string `json:"to"`
}

// openResp says whether the far side was found.
type openResp struct {
	OK    bool   `json:"ok"`
	From  string `json:"from,omitempty"`
	Error string `json:"error,omitempty"`
}

// registry maps cluster and node to whatever represents a live connection.
// Generic only so it can be exercised without standing up QUIC, which is what
// the reconnect case needs - that bug is a few lines of bookkeeping and does
// not deserve a network to reproduce.
type registry[T comparable] struct {
	mu sync.RWMutex
	m  map[string]map[string]T
}

func newRegistry[T comparable]() *registry[T] {
	return &registry[T]{m: map[string]map[string]T{}}
}

func (r *registry[T]) put(cluster, node string, v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.m[cluster]
	if c == nil {
		c = map[string]T{}
		r.m[cluster] = c
	}
	c[node] = v
}

// drop removes an entry only if it is still the one given. A peer that
// reconnected has already replaced it, and removing the new one would leave
// it unreachable while it believes it is connected.
func (r *registry[T]) drop(cluster, node string, v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.m[cluster]
	if c == nil {
		return
	}
	if c[node] == v {
		delete(c, node)
	}
	if len(c) == 0 {
		delete(r.m, cluster)
	}
}

func (r *registry[T]) get(cluster, node string) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[cluster][node]
	return v, ok
}

func (r *registry[T]) names(cluster string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for node := range r.m[cluster] {
		out = append(out, node)
	}
	return out
}

// Server is the relay.
type Server struct {
	// Peers in different clusters cannot address each other; the cluster id
	// comes from the shared token, so crossing that boundary would hand one
	// user's machines to another.
	conns *registry[*quic.Conn]
}

func NewServer() *Server { return &Server{conns: newRegistry[*quic.Conn]()} }

// Serve accepts relayed connections until ctx ends.
func (s *Server) Serve(ctx context.Context, ln *quic.Listener) error {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn *quic.Conn) {
	// The first stream identifies the peer. A connection that does not say
	// who it is gets nothing: without a name it cannot be addressed, and it
	// would otherwise sit in the map as an anonymous hole.
	first, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.CloseWithError(1, "no hello")
		return
	}
	var h hello
	if _, err := readHeader(io.LimitReader(first, maxHeader), &h); err != nil {
		conn.CloseWithError(1, "bad hello")
		return
	}
	addr, err := h.verify()
	if err != nil {
		conn.CloseWithError(1, "unauthenticated: "+err.Error())
		return
	}
	_ = first.Close()

	s.conns.put(h.Cluster, addr, conn)
	defer s.conns.drop(h.Cluster, addr, conn)

	for {
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.handleStream(ctx, h.Cluster, addr, st)
	}
}

// handleStream splices one peer's stream to a stream on the other's
// connection, and copies until either end stops.
func (s *Server) handleStream(ctx context.Context, cluster, from string, st *quic.Stream) {
	defer st.Close()

	var req openReq
	rest, err := readHeader(io.LimitReader(st, maxHeader), &req)
	if err != nil || req.To == "" {
		writeJSON(st, openResp{Error: "expected an open request naming a peer"})
		return
	}

	target, ok := s.conns.get(cluster, req.To)
	if !ok {
		writeJSON(st, openResp{Error: fmt.Sprintf("peer %q is not connected to this relay", req.To)})
		return
	}

	out, err := target.OpenStreamSync(ctx)
	if err != nil {
		writeJSON(st, openResp{Error: "peer accepted no stream: " + err.Error()})
		return
	}
	defer out.Close()

	if err := writeJSON(out, openResp{OK: true, From: from}); err != nil {
		return
	}
	if err := writeJSON(st, openResp{OK: true, From: req.To}); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// rest already holds whatever arrived after the header, and reading
		// on through it continues into the stream.
		_, _ = io.Copy(out, rest)
		_, _ = io.Copy(out, st)
		_ = out.Close()
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(st, out)
	}()
	wg.Wait()
}

// Peers lists who is currently connected, for diagnostics.
func (s *Server) Peers(cluster string) []string { return s.conns.names(cluster) }

// readHeader reads one newline-terminated JSON header and hands back a reader
// positioned exactly at the payload.
//
// Not a json.Decoder, which was the first attempt and is subtly wrong here: it
// consumes the JSON value but leaves the terminating newline in its own
// buffer, so forwarding what it buffered injects a stray byte into the
// payload. Relaying an echo showed it at once - "\n\n\nthe quick brown fox" -
// and on HTTP it corrupts the request line instead, which surfaces as a
// malformed response and points nowhere near the cause.
func readHeader(r io.Reader, v any) (*bufio.Reader, error) {
	br := bufio.NewReader(r)
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(line, v); err != nil {
		return nil, err
	}
	return br, nil
}

func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// ServerTLS is the relay's TLS configuration.
func ServerTLS(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPN},
		MinVersion:   tls.VersionTLS13,
	}
}

// Listen opens a QUIC listener for the relay.
func Listen(addr string, cert tls.Certificate) (*quic.Listener, error) {
	return quic.ListenAddr(addr, ServerTLS(cert), quicConfig())
}

func quicConfig() *quic.Config {
	return &quic.Config{
		// Long enough that an idle cluster does not have to reconnect
		// constantly, short enough that a dead peer is noticed.
		MaxIdleTimeout:  90 * time.Second,
		KeepAlivePeriod: 20 * time.Second,
	}
}
