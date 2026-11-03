package nattype

import (
	"net"
	"testing"
)

func TestAddrStringNil(t *testing.T) {
	if got := addrString(nil); got != "" {
		t.Errorf("addrString(nil) = %q, want \"\"", got)
	}
	if got := addrString(&net.UDPAddr{IP: net.IPv4(10, 0, 0, 5), Port: 4244}); got != "10.0.0.5:4244" {
		t.Errorf("addrString(real) = %q", got)
	}
}
