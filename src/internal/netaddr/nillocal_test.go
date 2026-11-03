package netaddr

import (
	"net"
	"testing"
)

// nilLocalConn is a UDP conn whose LocalAddr is nil, as Go leaves it when
// getsockname fails (seen under proot on Android).
type nilLocalConn struct{ net.Conn }

func (nilLocalConn) LocalAddr() net.Addr { return nil }
func (nilLocalConn) Close() error        { return nil }

func TestSourceForNilLocalAddr(t *testing.T) {
	old := dialUDP
	t.Cleanup(func() { dialUDP = old })

	// Row 1: getsockname failed → "" and no panic; Advertise falls back.
	dialUDP = func(string, string) (net.Conn, error) { return nilLocalConn{}, nil }
	if got := SourceFor("192.168.0.107:4242"); got != "" {
		t.Fatalf("SourceFor with nil local addr = %q, want \"\"", got)
	}
	if got := Advertise([]string{"192.168.0.107:4242"}, "10.0.0.5"); got != "10.0.0.5" {
		t.Fatalf("Advertise with nil local addr = %q, want the fallback 10.0.0.5", got)
	}

	// Row 2 (opposite): a normal conn still yields its local address.
	dialUDP = old
	if got := SourceFor("127.0.0.1:9"); got != "" {
		t.Fatalf("loopback source = %q, want \"\" (loopback is never advertised)", got)
	}
}
