package relay

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/quic-go/quic-go"
)

// Client is a peer's end of the relay: one connection, over which streams go
// both ways.
type Client struct {
	conn    *quic.Conn
	node    string
	cluster string

	// dial opens a connection to whatever local service an inbound stream is
	// destined for - the daemon's own port. A function rather than an address
	// so a test can point it anywhere, and so the relay stays ignorant of
	// what it is carrying.
	dial func(ctx context.Context) (net.Conn, error)
}

// Dial connects to a relay and announces who we are.
//
// verify is the certificate check. The relay is somebody else's machine, so
// this is not optional - but its certificate is self-signed, so a chain check
// could only ever fail. The caller supplies pinning: the same
// trust-on-first-use the daemons already use between themselves.
func Dial(ctx context.Context, addr, cluster string, key identity.KeyPair,
	verify func(rawCerts [][]byte, chains [][]*x509.Certificate) error,
	local func(context.Context) (net.Conn, error)) (*Client, error) {

	if cluster == "" {
		return nil, fmt.Errorf("a relay client needs a cluster")
	}
	if len(key.Private) == 0 {
		return nil, fmt.Errorf("a relay client needs a signing key: the relay " +
			"registers peers by the fingerprint of the key they sign with, not by a " +
			"name they choose")
	}
	tlsConf := &tls.Config{
		// The fingerprint answers the question that matters - is this the
		// same relay as last time - which a chain check on a self-signed
		// certificate cannot.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verify,
		NextProtos:            []string{ALPN},
		MinVersion:            tls.VersionTLS13,
	}
	conn, err := quic.DialAddr(ctx, addr, tlsConf, quicConfig())
	if err != nil {
		return nil, fmt.Errorf("dialling relay %s: %w", addr, err)
	}

	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	// A fresh nonce per connection: a hello captured from the wire must not
	// be replayable to claim this identity later.
	var nonceRaw [16]byte
	if _, err := rand.Read(nonceRaw[:]); err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	nonce := base64.StdEncoding.EncodeToString(nonceRaw[:])
	sig := key.Sign(helloContext, []byte(cluster+"|"+nonce))

	if err := writeJSON(st, hello{
		Cluster: cluster,
		PubKey:  identity.EncodePublic(key.Public),
		Nonce:   nonce,
		Sig:     base64.StdEncoding.EncodeToString(sig),
	}); err != nil {
		conn.CloseWithError(1, "")
		return nil, err
	}
	_ = st.Close()

	return &Client{conn: conn, node: key.Fingerprint(), cluster: cluster, dial: local}, nil
}

// Serve answers streams the relay forwards to us, splicing each to the local
// service.
func (c *Client) Serve(ctx context.Context) error {
	for {
		st, err := c.conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go c.handleInbound(ctx, st)
	}
}

func (c *Client) handleInbound(ctx context.Context, st *quic.Stream) {
	defer st.Close()

	var resp openResp
	rest, err := readHeader(io.LimitReader(st, maxHeader), &resp)
	if err != nil || !resp.OK {
		return
	}

	if c.dial == nil {
		return
	}
	local, err := c.dial(ctx)
	if err != nil {
		return
	}
	defer local.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(local, rest)
		_, _ = io.Copy(local, st)
		// Half-close so the local service sees the end of the request and
		// answers, rather than waiting for more that is never coming.
		if cw, ok := local.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(st, local)
	}()
	wg.Wait()
}

// Stream is a relayed byte stream. It exists because reading the relay's
// header with a JSON decoder inevitably buffers some of what follows, and
// those bytes are the caller's payload - for HTTP, the first line of the
// response. Handing back the raw stream loses them.
type Stream struct {
	*quic.Stream
	r io.Reader
}

func (s *Stream) Read(p []byte) (int, error) { return s.r.Read(p) }

// Open starts a stream to a named peer through the relay. The stream carries
// whatever the caller writes; the relay does not look inside it.
func (c *Client) Open(ctx context.Context, peer string) (*Stream, error) {
	st, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	if err := writeJSON(st, openReq{To: peer}); err != nil {
		st.Close()
		return nil, err
	}
	var resp openResp
	rest, err := readHeader(io.LimitReader(st, maxHeader), &resp)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("relay did not answer: %w", err)
	}
	if !resp.OK {
		st.Close()
		return nil, fmt.Errorf("relay refused: %s", resp.Error)
	}
	return &Stream{Stream: st, r: io.MultiReader(rest, st)}, nil
}

// Node is the address other peers reach this client at: the fingerprint of
// its key.
func (c *Client) Node() string { return c.node }

// Close ends the connection.
func (c *Client) Close() error { return c.conn.CloseWithError(0, "") }

// LocalDialer returns a dialer for a local TCP address, which is what a
// daemon's relay client points at.
func LocalDialer(addr string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		d := net.Dialer{Timeout: 5 * time.Second}
		return d.DialContext(ctx, "tcp", addr)
	}
}

// NetConn adapts a relayed stream to net.Conn, so an ordinary http.Client can
// be pointed through it and everything above the transport stays unchanged.
func NetConn(s *Stream) net.Conn { return &streamConn{s: s} }

type streamConn struct{ s *Stream }

func (c *streamConn) Read(b []byte) (int, error)  { return c.s.Read(b) }
func (c *streamConn) Write(b []byte) (int, error) { return c.s.Write(b) }
func (c *streamConn) Close() error                { return c.s.Close() }
func (c *streamConn) LocalAddr() net.Addr         { return relayAddr{} }
func (c *streamConn) RemoteAddr() net.Addr        { return relayAddr{} }

// Deadlines are the stream's own; QUIC already bounds an idle connection, and
// an http.Client sets these on every request.
func (c *streamConn) SetDeadline(t time.Time) error      { return c.s.SetDeadline(t) }
func (c *streamConn) SetReadDeadline(t time.Time) error  { return c.s.SetReadDeadline(t) }
func (c *streamConn) SetWriteDeadline(t time.Time) error { return c.s.SetWriteDeadline(t) }

type relayAddr struct{}

func (relayAddr) Network() string { return "quic" }
func (relayAddr) String() string  { return "relayed" }

// Closed reports whether the connection to the relay has gone.
//
// A dead connection takes its streams with it, so a caller that keeps using it
// opens streams into nothing and every peer behind the relay silently stops
// answering. Checking beats waiting for the failures.
func (c *Client) Closed() bool {
	select {
	case <-c.conn.Context().Done():
		return true
	default:
		return false
	}
}
