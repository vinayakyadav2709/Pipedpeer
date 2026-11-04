package internet

import (
	"errors"
	"net"
	"strings"
	"testing"
)

// noAddrListener is a listener whose getsockname failed: Addr is nil.
type noAddrListener struct{ net.Listener }

func (noAddrListener) Addr() net.Addr { return nil }

func TestListenLoopback(t *testing.T) {
	orig := listenTCP
	t.Cleanup(func() { listenTCP = orig })

	t.Run("normal kernel: port zero, address from the listener", func(t *testing.T) {
		listenTCP = orig
		ln, addr, err := listenLoopback()
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		if addr != ln.Addr().String() || !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Fatalf("addr %q, listener %v", addr, ln.Addr())
		}
	})

	t.Run("nil address: explicit port, reachable", func(t *testing.T) {
		calls := 0
		listenTCP = func(network, address string) (net.Listener, error) {
			calls++
			ln, err := orig(network, address)
			if err != nil || !strings.HasSuffix(address, ":0") {
				return ln, err
			}
			return noAddrListener{ln}, nil
		}
		ln, addr, err := listenLoopback()
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		if calls < 2 || strings.HasSuffix(addr, ":0") {
			t.Fatalf("calls %d addr %q: want an explicit port", calls, addr)
		}
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		c.Close()
	})

	t.Run("listen fails: error, no panic", func(t *testing.T) {
		listenTCP = func(string, string) (net.Listener, error) { return nil, errors.New("no sockets") }
		if _, _, err := listenLoopback(); err == nil {
			t.Fatal("want error")
		}
	})

	t.Run("nil address and every explicit port taken: error, no panic", func(t *testing.T) {
		listenTCP = func(network, address string) (net.Listener, error) {
			if strings.HasSuffix(address, ":0") {
				ln, err := orig(network, address)
				if err != nil {
					return nil, err
				}
				return noAddrListener{ln}, nil
			}
			return nil, errors.New("address in use")
		}
		if _, _, err := listenLoopback(); err == nil {
			t.Fatal("want error")
		}
	})
}
