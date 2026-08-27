package direct

import (
	"net"
	"time"
)

// sharedConn lets the QUIC transport and the prober use one socket.
//
// They have to. A router's mapping belongs to a socket, so the punch that
// opens a path and the connection that uses it must come from the same one;
// two sockets means two mappings, and the punched one is not the one anything
// listens on.
//
// quic-go reads its PacketConn exclusively and has no hook for datagrams it
// does not recognise, so the split happens here: probes are taken out of the
// read stream and handed to the prober, everything else is passed up as
// though nothing had been filtered.
//
// The socket is a named field rather than an embedded one, and that is
// load-bearing. Embedding promotes *net.UDPConn's whole method set -
// SyscallConn, ReadMsgUDP and the rest - and quic-go looks for exactly those
// to enable its optimised read paths. It finds them, reads the socket
// directly, and ReadFrom below is never called: measured, every probe
// vanished into the QUIC stack and punching never answered. Hiding the
// concrete type costs the GSO and OOB fast paths and buys a punch that works.
type sharedConn struct {
	conn   *net.UDPConn
	prober *Prober
}

// ReadFrom returns the next datagram that is not one of ours.
func (s *sharedConn) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, addr, err := s.conn.ReadFromUDPAddrPort(b)
		if err != nil {
			return n, nil, err
		}
		// Consumed here and never passed on: QUIC would otherwise see a
		// stray datagram on a connection it has no record of.
		if s.prober != nil && s.prober.Deliver(b[:n], addr) {
			continue
		}
		return n, net.UDPAddrFromAddrPort(addr), nil
	}
}

func (s *sharedConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	return s.conn.WriteTo(b, addr)
}
func (s *sharedConn) Close() error                       { return s.conn.Close() }
func (s *sharedConn) LocalAddr() net.Addr                { return s.conn.LocalAddr() }
func (s *sharedConn) SetDeadline(t time.Time) error      { return s.conn.SetDeadline(t) }
func (s *sharedConn) SetReadDeadline(t time.Time) error  { return s.conn.SetReadDeadline(t) }
func (s *sharedConn) SetWriteDeadline(t time.Time) error { return s.conn.SetWriteDeadline(t) }
