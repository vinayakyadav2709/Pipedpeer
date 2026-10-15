package direct

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/quic-go/quic-go"
)

// ALPN keeps direct connections from being confused with relayed ones. They
// speak the same handshake but arrive by different routes, and a peer that
// dials one expecting the other should be told so rather than half-work.
//
// /2: the hello is bound to the TLS session and proves the cluster token. A
// /1 peer cannot produce either, so it fails at the handshake with a version
// reason instead of an identity one.
//
// /3: the 4242 port block, one identity and signed address gossip ship
// together as one hard cut (ADR-0006); a /2 build is refused with the version
// reason rather than half-working against the new ports.
const ALPN = "pipedpeer-direct/3"

// ekmLabel names the TLS exporter the hello is bound to (RFC 9266 style).
// Both ends of one session derive the same bytes and no other session can, so
// a hello lifted from one connection is worthless on another.
const ekmLabel = "EXPORTER-pipedpeer-direct-hello"

// macContext separates the token proof from any other HMAC of the token.
const macContext = "pipedpeer-direct-mac|"

// sessionEKM is the exporter value of conn's TLS session.
func sessionEKM(conn *quic.Conn) ([]byte, error) {
	cs := conn.ConnectionState().TLS
	return cs.ExportKeyingMaterial(ekmLabel, nil, 32)
}

// helloContext binds a hello's signature to this purpose. Distinct from the
// relay's, so a hello signed for one cannot be replayed at the other.
const helloContext = "direct-hello"

// maxHeader bounds the preamble a peer can send before proving who it is.
const maxHeader = 4096

// hello proves identity as the relay's does, and more: the signature covers
// the TLS session's exporter, so it cannot be replayed on another connection,
// and Mac proves the sender holds the cluster token. The cluster ID alone is
// known to the introducer's operator; the token is not.
//
// The node is not named, it is proved: the address a peer is entitled to is
// the fingerprint of the key it signs with. That matters more here than on
// the relay. A race between candidate addresses means connecting to whoever
// answers first, and whoever answers first is not necessarily who was meant -
// a stale mapping, another machine that picked up the address by DHCP, or
// somebody who saw the introducer traffic.
type hello struct {
	Cluster string `json:"cluster"`
	PubKey  string `json:"pubkey"`
	Nonce   string `json:"nonce"`
	Sig     string `json:"sig"`
	Mac     string `json:"mac"`
}

func helloMAC(secret, cluster, pubkey string, ekm []byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(macContext + cluster + "|" + pubkey + "|" + hex.EncodeToString(ekm)))
	return m.Sum(nil)
}

// verify checks the hello against this session's exporter and this
// cluster's token, and returns the fingerprint the sender proved.
func (h hello) verify(ekm []byte, secret string) (string, error) {
	if h.Cluster == "" || h.PubKey == "" || h.Nonce == "" || h.Sig == "" || h.Mac == "" {
		return "", fmt.Errorf("hello must carry a cluster, a public key, a nonce, a signature and a mac")
	}
	pub, err := identity.DecodePublic(h.PubKey)
	if err != nil {
		return "", fmt.Errorf("public key: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(h.Sig)
	if err != nil {
		return "", fmt.Errorf("signature: %w", err)
	}
	if !identity.Verify(pub, helloContext, []byte(h.Cluster+"|"+h.Nonce+"|"+hex.EncodeToString(ekm)), sig) {
		return "", fmt.Errorf("signature does not match the public key offered on this session")
	}
	mac, err := base64.StdEncoding.DecodeString(h.Mac)
	if err != nil {
		return "", fmt.Errorf("mac: %w", err)
	}
	if !hmac.Equal(mac, helloMAC(secret, h.Cluster, h.PubKey, ekm)) {
		return "", fmt.Errorf("the peer could not prove it holds the cluster token")
	}
	return identity.Fingerprint(pub), nil
}

func signHello(cluster, secret string, key identity.KeyPair, ekm []byte) (hello, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return hello{}, err
	}
	nonce := base64.StdEncoding.EncodeToString(raw[:])
	pub := identity.EncodePublic(key.Public)
	return hello{
		Cluster: cluster,
		PubKey:  pub,
		Nonce:   nonce,
		Sig:     base64.StdEncoding.EncodeToString(key.Sign(helloContext, []byte(cluster+"|"+nonce+"|"+hex.EncodeToString(ekm)))),
		Mac:     base64.StdEncoding.EncodeToString(helloMAC(secret, cluster, pub, ekm)),
	}, nil
}

func quicConfig() *quic.Config {
	return &quic.Config{
		// A NAT forgets an idle mapping in about thirty seconds, so the
		// keepalive is what stops a working path quietly closing between
		// jobs - and reopening it would mean punching again.
		MaxIdleTimeout:  90 * time.Second,
		KeepAlivePeriod: 20 * time.Second,
	}
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

func readJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(io.LimitReader(r, maxHeader))
	return dec.Decode(v)
}

// Endpoint is one daemon's direct-connection machinery: the shared socket,
// the QUIC transport over it, and the prober that punches.
type Endpoint struct {
	conn      *net.UDPConn
	transport *quic.Transport
	listener  *quic.Listener
	prober    *Prober

	key       identity.KeyPair
	cluster   string
	secret    string
	onInbound func(string, *quic.Conn)
	// local dials the service a peer's stream should be spliced to, which
	// keeps everything above this ignorant of how the bytes arrived.
	local func(context.Context) (net.Conn, error)
	log   func(string, ...any)

	other   chan otherPacket
	tlsConf *tls.Config
	adopted []*quic.Transport

	mu    sync.Mutex
	peers map[string]*quic.Conn
}

// Config is what an Endpoint needs.
type Config struct {
	// Port is the shared UDP port. Fixed by default so a mapping and a
	// peer's cached address survive a restart.
	Port int
	// Key identifies this node; its fingerprint is the address peers use.
	Key identity.KeyPair
	// Cluster scopes everything, as it does on the relay.
	Cluster string
	// Secret is the cluster token; proves membership in the direct hello.
	// The cluster ID is not enough: the introducer's operator knows it.
	Secret string
	// Cert is this node's TLS certificate. Self-signed: identity is proved
	// by the signed hello, not by a certificate chain.
	Cert *tls.Certificate
	// Local dials the service inbound streams are spliced to.
	Local func(context.Context) (net.Conn, error)
	// OnInbound is called once a peer that dialled US has proved who it is.
	//
	// Needed because reachability is not symmetric. A machine whose router
	// allocates a fresh port per destination can dial out perfectly well and
	// cannot be dialled, so for that pair every connection is inbound at one
	// end - and without this the receiving side authenticates the peer,
	// files the connection away and never gives it a local port, while the
	// far side reconnects forever.
	OnInbound func(node string, conn *quic.Conn)
	// Log receives progress.
	Log func(string, ...any)
}

// Listen brings up the shared socket and starts accepting.
func Listen(cfg Config) (*Endpoint, error) {
	if cfg.Cluster == "" {
		return nil, fmt.Errorf("a direct endpoint needs a cluster")
	}
	if len(cfg.Key.Private) == 0 {
		return nil, fmt.Errorf("a direct endpoint needs a signing key: peers are " +
			"addressed by the fingerprint of the key they sign with")
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: cfg.Port})
	if err != nil {
		return nil, fmt.Errorf("binding the direct port %d: %w", cfg.Port, err)
	}

	e := &Endpoint{
		conn:      conn,
		key:       cfg.Key,
		cluster:   cfg.Cluster,
		secret:    cfg.Secret,
		local:     cfg.Local,
		onInbound: cfg.OnInbound,
		log:       cfg.Log,
		peers:     map[string]*quic.Conn{},
	}
	e.prober = NewProber(conn, cfg.Key.Fingerprint())

	// The transport gets the socket through a wrapper that takes the probes
	// out of the read stream first. One socket is not tidiness: a router's
	// mapping belongs to a socket, so the punch and the connection that uses
	// it have to come from the same one.
	e.other = make(chan otherPacket, 64)
	e.transport = &quic.Transport{
		Conn: &sharedConn{conn: conn, prober: e.prober, other: e.other},
	}

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{*cfg.Cert},
		NextProtos:   []string{ALPN},
		MinVersion:   tls.VersionTLS13,
		// A peer's certificate proves nothing here; the signed hello does.
		ClientAuth: tls.NoClientCert,
	}
	e.tlsConf = tlsConf
	ln, err := e.transport.Listen(tlsConf, quicConfig())
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("listening for direct connections: %w", err)
	}
	e.listener = ln
	return e, nil
}

// NextOther returns the next datagram that was neither QUIC nor a probe.
//
// The introducer's replies and its forwarded punch requests. They arrive on
// this socket because they have to: the registration that carries them is
// also what keeps this node's mapping alive on its own router, and sending it
// from anywhere else would refresh the wrong mapping.
func (e *Endpoint) NextOther(ctx context.Context) ([]byte, netip.AddrPort, error) {
	select {
	case p := <-e.other:
		return p.payload, p.from, nil
	case <-ctx.Done():
		return nil, netip.AddrPort{}, ctx.Err()
	}
}

// LocalDialer connects to a local service, for splicing a peer's streams to
// this daemon.
func LocalDialer(addr string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// adoptWindow is how long an adopted socket takes new connections. The peer
// dials the moment its spray hears back, so this is generous; a variable so a
// test can shorten it.
var adoptWindow = 30 * time.Second

// Adopt makes a socket found by collision carry connections too.
//
// The collision leaves the working mapping on one of the many sockets opened
// to create it, not on the shared one - that is the whole mechanism, since a
// single socket has a single mapping and one port in sixty thousand is not
// worth spraying at. The peer that found us will dial the address belonging
// to THAT socket, so unless something is listening there the collision
// achieves nothing.
//
// Only ctx's values are used, never its deadline. The caller is the
// collision, which returns as soon as this socket is adopted, while the peer's
// dial is still in flight; bound to its context, the listener closed
// mid-handshake and quic-go answered CONNECTION_REFUSED - every collision in
// the NAT lab (lab-nat.sh nat) failed that way. The socket takes new
// connections for adoptWindow, then stays open only while a connection it
// accepted lives, and is closed after the last one (or by Close).
//
// The prober is shared, so probes arriving on an adopted socket are answered
// exactly as on the main one.
func (e *Endpoint) Adopt(ctx context.Context, conn *net.UDPConn) error {
	tr := &quic.Transport{Conn: &sharedConn{conn: conn, prober: e.prober, other: e.other}}
	ln, err := tr.Listen(e.tlsConf, quicConfig())
	if err != nil {
		_ = tr.Close()
		return fmt.Errorf("listening on the socket the collision found: %w", err)
	}
	e.mu.Lock()
	e.adopted = append(e.adopted, tr)
	e.mu.Unlock()

	life := context.WithoutCancel(ctx)
	go func() {
		window, cancel := context.WithTimeout(life, adoptWindow)
		defer cancel()
		var live sync.WaitGroup
		for {
			c, err := ln.Accept(window)
			if err != nil {
				break
			}
			live.Add(1)
			go func() {
				defer live.Done()
				e.accept(life, c) // returns when the connection ends
			}()
		}
		_ = ln.Close()
		live.Wait()
		_ = tr.Close()
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			e.log("direct: closing the adopted socket %s: %v", conn.LocalAddr(), err)
		}
		e.mu.Lock()
		e.adopted = slices.DeleteFunc(e.adopted, func(t *quic.Transport) bool { return t == tr })
		e.mu.Unlock()
	}()
	return nil
}

// Prober exposes the punching machinery to the manager that drives it.
func (e *Endpoint) Prober() *Prober { return e.prober }

// Conn is the shared socket, for the introducer traffic that must come from
// the same mapping as everything else.
func (e *Endpoint) Conn() *net.UDPConn { return e.conn }

// Port is the shared port.
func (e *Endpoint) Port() uint16 {
	return uint16(e.conn.LocalAddr().(*net.UDPAddr).Port)
}

// Serve accepts direct connections until ctx ends.
func (e *Endpoint) Serve(ctx context.Context) error {
	for {
		conn, err := e.listener.Accept(ctx)
		if err != nil {
			return err
		}
		go e.accept(ctx, conn)
	}
}

// accept verifies who dialled before letting them do anything.
func (e *Endpoint) accept(ctx context.Context, conn *quic.Conn) {
	st, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.CloseWithError(1, "no hello")
		return
	}
	var h hello
	if err := readJSON(st, &h); err != nil {
		conn.CloseWithError(1, "bad hello")
		return
	}
	ekm, err := sessionEKM(conn)
	if err != nil {
		e.log("direct: no TLS exporter on the session from %s: %v", conn.RemoteAddr(), err)
		conn.CloseWithError(1, "")
		return
	}
	node, err := h.verify(ekm, e.secret)
	if err != nil {
		e.log("direct: refused a hello from %s: %v", conn.RemoteAddr(), err)
		conn.CloseWithError(1, "unauthenticated: "+err.Error())
		return
	}
	if h.Cluster != e.cluster {
		// Different cluster: not ours to talk to, and saying which cluster
		// this is would leak it.
		conn.CloseWithError(1, "unknown cluster")
		return
	}
	_ = st.Close()

	// Prove who we are in turn: the dialler raced several addresses and has
	// to know it reached the one it meant.
	mine, err := signHello(e.cluster, e.secret, e.key, ekm)
	if err != nil {
		conn.CloseWithError(1, "")
		return
	}
	ours, err := conn.OpenStreamSync(ctx)
	if err != nil {
		conn.CloseWithError(1, "")
		return
	}
	if err := writeJSON(ours, mine); err != nil {
		conn.CloseWithError(1, "")
		return
	}
	_ = ours.Close()

	e.mu.Lock()
	e.peers[node] = conn
	e.mu.Unlock()
	e.log("direct: %s connected from %s", short(node), conn.RemoteAddr())
	if e.onInbound != nil {
		e.onInbound(node, conn)
	}

	defer func() {
		e.mu.Lock()
		if e.peers[node] == conn {
			delete(e.peers, node)
		}
		e.mu.Unlock()
	}()

	e.serveStreams(ctx, conn)
}

// serveStreams splices everything a peer opens to the local service.
//
// Run for connections in BOTH directions, which is the point. A QUIC
// connection is symmetric once established - either end may open a stream -
// and only the accepting side ran this, so a peer we had dialled could never
// send us anything. Measured: the forwarder took the request, the stream went
// out, and nothing ever accepted it at the far end; curl sat until it timed
// out with zero bytes.
func (e *Endpoint) serveStreams(ctx context.Context, conn *quic.Conn) {
	for {
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go e.splice(ctx, st)
	}
}

// splice joins a peer's stream to the local service.
func (e *Endpoint) splice(ctx context.Context, st *quic.Stream) {
	defer st.Close()
	if e.local == nil {
		return
	}
	local, err := e.local(ctx)
	if err != nil {
		return
	}
	defer local.Close()

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(local, st)
		// Half-close, so the service sees the end of the request and answers
		// rather than waiting for a body that is not coming.
		if cw, ok := local.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(st, local)
		_ = st.Close()
		done <- struct{}{}
	}()
	// BOTH directions, not whichever finishes first. The request half
	// completes as soon as the caller half-closes, and returning then runs
	// the deferred Close on the stream - discarding the response the service
	// was about to write. Measured: an echo came back empty every time.
	<-done
	<-done
}

// Dial connects to a peer at an address the race has already proved
// reachable, and refuses the connection if the far side cannot prove it is
// that peer.
//
// A probe coming back from an address does not make it the peer's. It may be
// a stale mapping, a machine that took the address by DHCP, or somebody who
// watched the introducer traffic. So the hello is signed and the fingerprint
// it yields must be the node that was asked for - which is why an address
// race is safe to run at all.
func (e *Endpoint) Dial(ctx context.Context, node string, at netip.AddrPort) (*quic.Conn, error) {
	tlsConf := &tls.Config{
		// The certificate is self-signed and proves nothing; the hello does.
		// Checking a chain here would refuse every peer.
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}
	conn, err := e.transport.Dial(ctx, net.UDPAddrFromAddrPort(at), tlsConf, quicConfig())
	if err != nil {
		if isALPNMismatch(err) {
			return nil, &Unreachable{Peer: node, Reason: ReasonVersion, Tried: 1}
		}
		return nil, fmt.Errorf("dialling %s at %s: %w", short(node), at, err)
	}
	ekm, err := sessionEKM(conn)
	if err != nil {
		conn.CloseWithError(1, "")
		return nil, fmt.Errorf("no TLS exporter on the session to %s at %s: %w", short(node), at, err)
	}

	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	h, err := signHello(e.cluster, e.secret, e.key, ekm)
	if err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	if err := writeJSON(st, h); err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	_ = st.Close()

	// And the far side proves itself in turn. Without this the race would
	// hand the connection to whoever answered fastest.
	var theirs hello
	rst, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.CloseWithError(1, "")
		// A peer that closes with "already connected" is not failing to
		// identify itself - it has a working link to us and is declining a
		// second one, which happens whenever both ends race at once and both
		// win. Reporting that as an identity failure sends a reader looking
		// for an impostor.
		if isAlreadyConnected(err) {
			return nil, errAlreadyConnected
		}
		// The far side read our hello and refused it: no token, or a
		// replayed one. That is an identity failure, not a silent peer.
		if strings.Contains(err.Error(), "unauthenticated") {
			return nil, &Unreachable{Peer: node, Reason: ReasonIdentity, Tried: 1}
		}
		return nil, fmt.Errorf("%s at %s never identified itself: %w", short(node), at, err)
	}
	if err := readJSON(rst, &theirs); err != nil {
		conn.CloseWithError(1, "")
		return nil, fmt.Errorf("%s at %s sent no usable hello: %w", short(node), at, err)
	}
	_ = rst.Close()

	got, err := theirs.verify(ekm, e.secret)
	if err != nil {
		e.log("direct: %s at %s sent a hello that does not verify: %v", short(node), at, err)
		conn.CloseWithError(1, "")
		return nil, &Unreachable{Peer: node, Reason: ReasonIdentity, Tried: 1}
	}
	if got != node || theirs.Cluster != e.cluster {
		conn.CloseWithError(1, "wrong peer")
		return nil, &Unreachable{Peer: node, Reason: ReasonIdentity, Tried: 1}
	}

	e.mu.Lock()
	e.peers[node] = conn
	e.mu.Unlock()

	// A connection we dialled still has to serve what the peer opens on it.
	go func() {
		e.serveStreams(context.WithoutCancel(ctx), conn)
		e.mu.Lock()
		if e.peers[node] == conn {
			delete(e.peers, node)
		}
		e.mu.Unlock()
	}()
	return conn, nil
}

// Migrate moves a connection this end dialled onto the shared socket's current
// local address, after the machine's addresses changed.
//
// The socket is bound to every interface, so after a change the kernel
// already sends from the new address; what is missing is the peer agreeing to
// it. A probed path makes that explicit: the peer answers the challenge from
// the new address or it does not, and the caller learns which within the
// timeout instead of waiting out the idle timeout on a dead path. Only the
// dialling end can migrate (RFC 9000); the accepting end redials instead.
func (e *Endpoint) Migrate(ctx context.Context, conn *quic.Conn, timeout time.Duration) error {
	path, err := conn.AddPath(e.transport)
	if err != nil {
		return fmt.Errorf("adding a path: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := path.Probe(pctx); err != nil {
		if cerr := path.Close(); cerr != nil {
			e.log("direct: abandoning the unvalidated path to %s: %v", conn.RemoteAddr(), cerr)
		}
		return fmt.Errorf("probing the new path: %w", err)
	}
	if err := path.Switch(); err != nil {
		return fmt.Errorf("switching to the new path: %w", err)
	}
	return nil
}

// errAlreadyConnected says the peer already has a link to us and declined a
// second. Not a failure: both ends racing and both winning is the ordinary
// case, and one of the two connections has to lose.
var errAlreadyConnected = errors.New("the peer already has a link to us")

// IsAlreadyConnected reports whether a dial failed only because a link
// already exists.
func IsAlreadyConnected(err error) bool { return errors.Is(err, errAlreadyConnected) }

// noApplicationProtocol is TLS alert 120 as a QUIC crypto error (0x100 +
// alert, RFC 9001 section 4.8): the peer speaks no ALPN we offered.
const noApplicationProtocol = quic.TransportErrorCode(0x100 + 120)

// isALPNMismatch reports whether a dial failed because the peer runs another
// version of the direct protocol.
func isALPNMismatch(err error) bool {
	var te *quic.TransportError
	return errors.As(err, &te) && te.ErrorCode == noApplicationProtocol
}

func isAlreadyConnected(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already connected")
}

// Close shuts the endpoint down.
func (e *Endpoint) Close() error {
	if e.listener != nil {
		_ = e.listener.Close()
	}
	if e.transport != nil {
		_ = e.transport.Close()
	}
	e.mu.Lock()
	adopted := e.adopted
	e.adopted = nil
	e.mu.Unlock()
	for _, tr := range adopted {
		_ = tr.Close()
	}
	return e.conn.Close()
}
