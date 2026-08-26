package tlsid

import (
	"bufio"
	"crypto/tls"
	"net"
	"time"
)

// Listener accepts TLS and plain HTTP on the same port.
//
// The alternative is a flag day: every daemon in a cluster switching scheme
// at once, with anything missed simply unable to talk. Peers upgrade
// independently instead, and a cluster ends up encrypted as its nodes are
// updated rather than in one co-ordinated jump.
//
// A TLS record begins 0x16 (handshake) and an HTTP request begins with a
// method name, so one buffered byte decides it — and the byte is handed back
// either way, so neither side sees a truncated stream.
//
// Classification happens before Accept returns, because net/http only
// populates Request.TLS when the connection it is given really is a
// *tls.Conn; a lazy wrapper that decides on first read leaves every request
// looking unencrypted. Each connection is classified in its own goroutine so
// one peer that connects and says nothing cannot stall the accept loop.
type Listener struct {
	inner  net.Listener
	cfg    *tls.Config
	conns  chan net.Conn
	errs   chan error
	closed chan struct{}
}

// NewListener wraps l so it serves both. A nil certificate leaves it plain.
func NewListener(l net.Listener, cert *tls.Certificate) net.Listener {
	if cert == nil {
		return l
	}
	ln := &Listener{
		inner:  l,
		cfg:    &tls.Config{Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12},
		conns:  make(chan net.Conn),
		errs:   make(chan error, 1),
		closed: make(chan struct{}),
	}
	go ln.accept()
	return ln
}

func (l *Listener) accept() {
	for {
		c, err := l.inner.Accept()
		if err != nil {
			select {
			case l.errs <- err:
			case <-l.closed:
			}
			return
		}
		go l.classify(c)
	}
}

func (l *Listener) classify(c net.Conn) {
	// A client that opens a connection and sends nothing must not hold a
	// slot open indefinitely.
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(c)
	first, err := br.Peek(1)
	_ = c.SetReadDeadline(time.Time{})
	if err != nil {
		_ = c.Close()
		return
	}
	wrapped := net.Conn(&bufferedConn{Conn: c, r: br})
	if first[0] == 0x16 {
		wrapped = tls.Server(wrapped, l.cfg)
	}
	select {
	case l.conns <- wrapped:
	case <-l.closed:
		_ = wrapped.Close()
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case err := <-l.errs:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return l.inner.Close()
}

func (l *Listener) Addr() net.Addr { return l.inner.Addr() }

// bufferedConn hands back the byte that decided the protocol.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }
