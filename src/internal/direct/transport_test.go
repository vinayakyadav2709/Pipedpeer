package direct

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
)

func testCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "pipedpeer-direct-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// testKey makes a throwaway node identity. Each call is a different peer, so
// tests get distinct addresses without touching the machine's real key.
func testKey(t *testing.T) identity.KeyPair {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return identity.KeyPair{Public: pub, Private: priv}
}

// echoService is what a peer's stream gets spliced to, standing in for the
// daemon's own HTTP listener.
func echoService(t *testing.T) func(context.Context) (net.Conn, error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				body, _ := io.ReadAll(c)
				// A real service takes a moment to answer. Echoing
				// instantly makes "did the splice wait for the response"
				// a race that usually passes by luck, which is not a test
				// of anything.
				time.Sleep(80 * time.Millisecond)
				_, _ = c.Write(body)
			}()
		}
	}()
	return func(ctx context.Context) (net.Conn, error) {
		return net.Dial("tcp", ln.Addr().String())
	}
}

func endpoint(t *testing.T, key identity.KeyPair, cluster string, local func(context.Context) (net.Conn, error)) *Endpoint {
	t.Helper()
	cert := testCert(t)
	e, err := Listen(Config{Port: 0, Key: key, Cluster: cluster, Cert: &cert, Local: local})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = e.Serve(ctx) }()
	return e
}

// TestOneSocketCarriesBothProbesAndQUIC is the property the whole design
// rests on. The punch and the connection have to come from the same socket,
// because a router's mapping belongs to a socket - and quic-go reads its
// PacketConn exclusively, so if the split were wrong the probes would vanish
// into the QUIC stack and punching would never answer.
func TestOneSocketCarriesBothProbesAndQUIC(t *testing.T) {
	server := endpoint(t, testKey(t), "c1", echoService(t))
	client := endpoint(t, testKey(t), "c1", nil)

	// First: a probe reaches the prober through the shared socket, which is
	// what proves the wrapper hands non-QUIC datagrams over rather than
	// letting quic-go swallow them.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), server.Port())
	got, err := client.Prober().Race(ctx, []Candidate{{Kind: KindLAN, Addr: at}}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("the probe never came back, so the shared socket is not shared: %v", err)
	}
	if got.Port() != server.Port() {
		t.Errorf("answered from %s, want the server's port %d", got, server.Port())
	}

	// Then: QUIC over that same socket, to that same address.
	conn, err := client.Dial(ctx, server.key.Fingerprint(), at)
	if err != nil {
		t.Fatalf("QUIC would not go over the socket the probe just used: %v", err)
	}
	defer conn.CloseWithError(0, "")

	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("hello over a punched path")); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	buf := make([]byte, 64)
	n, _ := io.ReadFull(st, buf[:25])
	if string(buf[:n]) != "hello over a punched path" {
		t.Errorf("echoed %q, want the message back", buf[:n])
	}
}

// TestTheWrongMachineCannotWinTheRace. Racing candidate addresses means
// connecting to whoever answers first, and whoever answers first is not
// necessarily who was meant: a stale mapping, a machine that took the address
// by DHCP, or somebody who watched the introducer traffic. The signed hello
// is what makes racing safe.
func TestTheWrongMachineCannotWinTheRace(t *testing.T) {
	impostor := endpoint(t, testKey(t), "c1", echoService(t))
	client := endpoint(t, testKey(t), "c1", nil)

	// The node we meant to reach, which is not the one at this address.
	intended := testKey(t).Fingerprint()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), impostor.Port())
	_, err := client.Dial(ctx, intended, at)
	if err == nil {
		t.Fatal("connected to a machine that is not the peer we asked for")
	}
	u, ok := IsUnreachable(err)
	if !ok || u.Reason != ReasonIdentity {
		t.Errorf("error is %v; it should say the far side could not prove itself", err)
	}
}

// TestAPeerFromAnotherClusterIsRefused. Clusters are the membership boundary,
// and a direct path must not become the way around it.
func TestAPeerFromAnotherClusterIsRefused(t *testing.T) {
	server := endpoint(t, testKey(t), "theirs", echoService(t))
	client := endpoint(t, testKey(t), "mine", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), server.Port())
	if _, err := client.Dial(ctx, server.key.Fingerprint(), at); err == nil {
		t.Error("a peer in another cluster accepted a direct connection")
	}
}

// TestAnEndpointNeedsAKeyToBeAddressable. Peers are addressed by the
// fingerprint of the key they sign with; without one there is nothing to be.
func TestAnEndpointNeedsAKeyToBeAddressable(t *testing.T) {
	cert := testCert(t)
	if _, err := Listen(Config{Cluster: "c1", Cert: &cert}); err == nil {
		t.Error("an endpoint with no signing key was allowed to listen")
	}
	if _, err := Listen(Config{Key: testKey(t), Cert: &cert}); err == nil {
		t.Error("an endpoint with no cluster was allowed to listen")
	}
}

// TestTheDirectALPNIsNotTheRelayALPN. They speak the same handshake by
// different routes; a peer dialling one and reaching the other should be
// told rather than half-work.
func TestTheDirectALPNIsNotTheRelayALPN(t *testing.T) {
	if ALPN == "pipedpeer-relay/1" {
		t.Error("direct and relay connections share an ALPN")
	}
	if !strings.Contains(ALPN, "direct") {
		t.Errorf("ALPN %q does not name what it is", ALPN)
	}
	// And the signature contexts differ, so a hello signed for one cannot be
	// replayed at the other.
	if helloContext == "relay-hello" {
		t.Error("a direct hello is signed with the relay's context and could be replayed there")
	}
}
