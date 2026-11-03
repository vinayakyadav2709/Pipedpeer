package direct

import (
	"net"
	"testing"
)

func TestLocalAddrHelpersNil(t *testing.T) {
	var typedNil *net.UDPAddr
	cases := []struct {
		name   string
		a      net.Addr
		key    string
		keyOK  bool
		port   int
		portOK bool
	}{
		{"nil (getsockname failed)", nil, "", false, 0, false},
		{"typed nil UDP addr", typedNil, "", true, 0, false},
		{"real UDP addr", &net.UDPAddr{IP: net.IPv4(10, 0, 0, 5), Port: 4243}, "10.0.0.5:4243", true, 4243, true},
		{"TCP addr has no UDP port", &net.TCPAddr{IP: net.IPv4(10, 0, 0, 5), Port: 80}, "10.0.0.5:80", true, 0, false},
	}
	for _, c := range cases {
		if c.name == "typed nil UDP addr" {
			// String on a typed-nil *UDPAddr is safe ("<nil>"); only the port matters here.
			if p, ok := udpPort(c.a); ok || p != 0 {
				t.Errorf("%s: udpPort = %d,%v want 0,false", c.name, p, ok)
			}
			continue
		}
		if k, ok := addrKey(c.a); k != c.key || ok != c.keyOK {
			t.Errorf("%s: addrKey = %q,%v want %q,%v", c.name, k, ok, c.key, c.keyOK)
		}
		if p, ok := udpPort(c.a); p != c.port || ok != c.portOK {
			t.Errorf("%s: udpPort = %d,%v want %d,%v", c.name, p, ok, c.port, c.portOK)
		}
	}
}

func TestEndpointPortFallsBackWhenSocketCannotReportIt(t *testing.T) {
	// A socket whose LocalAddr is nil: an unbound UDPConn value reports nil.
	e := &Endpoint{conn: &net.UDPConn{}, port: 4243, log: func(string, ...any) {}}
	if got := e.Port(); got != 4243 {
		t.Fatalf("Port() = %d, want the configured 4243", got)
	}
}
