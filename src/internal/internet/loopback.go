package internet

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net"
)

// listenTCP is net.Listen; a var so tests can stand in for a kernel whose
// getsockname fails.
var listenTCP = net.Listen

// loopbackTries bounds the explicit-port fallback in listenLoopback.
const loopbackTries = 20

// listenLoopback opens a TCP listener on 127.0.0.1 and returns it with its
// "host:port" address.
//
// Port zero lets the kernel pick, and Go learns the pick from getsockname.
// Under proot on Android that call can fail; Go then leaves the listener's
// address nil, and calling String on it crashed the phone's daemon in the
// middle of a peer connecting. So when the address is missing, this closes
// that listener and binds explicit ports from the ephemeral range instead,
// where the address is known without asking the kernel.
func listenLoopback() (net.Listener, string, error) {
	ln, err := listenTCP("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	if a := ln.Addr(); a != nil {
		return ln, a.String(), nil
	}
	_ = ln.Close()
	log.Printf("internet: listener on 127.0.0.1:0 has no local address (getsockname failed); binding an explicit port")
	var lastErr error
	for range loopbackTries {
		addr := fmt.Sprintf("127.0.0.1:%d", 32768+rand.IntN(60999-32768))
		ln, err := listenTCP("tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		return ln, addr, nil
	}
	return nil, "", fmt.Errorf("no free loopback port after %d tries: %w", loopbackTries, lastErr)
}
