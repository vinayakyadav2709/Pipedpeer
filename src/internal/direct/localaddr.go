package direct

import "net"

// Under proot on Android getsockname can fail, and Go then leaves a socket's
// local address nil; calling a method on it, or asserting its type unchecked,
// crashed the daemon. These read it safely.

// addrKey is a.String(), false when a is nil.
func addrKey(a net.Addr) (string, bool) {
	if a == nil {
		return "", false
	}
	return a.String(), true
}

// udpPort is a's port, false when a is nil or not a UDP address.
func udpPort(a net.Addr) (int, bool) {
	ua, ok := a.(*net.UDPAddr)
	if !ok || ua == nil {
		return 0, false
	}
	return ua.Port, true
}
