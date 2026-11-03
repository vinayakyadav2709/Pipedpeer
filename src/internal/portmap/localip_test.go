package portmap

import (
	"net"
	"testing"
)

func TestLocalIPNil(t *testing.T) {
	var typedNil *net.UDPAddr
	if _, err := localIP(nil); err == nil {
		t.Error("localIP(nil) gave no error; want one (getsockname failed)")
	}
	if _, err := localIP(typedNil); err == nil {
		t.Error("localIP(typed nil) gave no error")
	}
	ip, err := localIP(&net.UDPAddr{IP: net.IPv4(192, 168, 1, 7), Port: 5351})
	if err != nil || !ip.Equal(net.IPv4(192, 168, 1, 7)) {
		t.Errorf("localIP(real) = %v, %v; want 192.168.1.7", ip, err)
	}
}
