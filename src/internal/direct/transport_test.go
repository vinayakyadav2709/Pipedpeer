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
	"github.com/quic-go/quic-go"
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

// TestAnInboundConnectionIsHandedUp.
//
// Reachability is not symmetric. A machine whose router allocates a fresh
// external port per destination dials out perfectly well and cannot be
// dialled, so for that pair every connection is inbound at one end - and the
// receiving end has to make a usable link out of it.
//
// Seen against the two real machines before this existed: the peer dialled
// in, was authenticated, filed away and never given a local port, and it
// reconnected every few seconds forever while the daemon logged each arrival.
func TestAnInboundConnectionIsHandedUp(t *testing.T) {
	type inbound struct {
		node string
		conn *quic.Conn
	}
	got := make(chan inbound, 4)

	serverKey := testKey(t)
	cert := testCert(t)
	server, err := Listen(Config{
		Port: 0, Key: serverKey, Cluster: "c1", Cert: &cert,
		Local: echoService(t),
		OnInbound: func(node string, conn *quic.Conn) {
			got <- inbound{node, conn}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = server.Serve(ctx) }()

	clientKey := testKey(t)
	client := endpoint(t, clientKey, "c1", nil)

	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), server.Port())
	if _, err := client.Dial(ctx, serverKey.Fingerprint(), at); err != nil {
		t.Fatal(err)
	}

	select {
	case in := <-got:
		if in.node != clientKey.Fingerprint() {
			t.Errorf("handed up node %s, want the dialling peer %s",
				in.node[:8], clientKey.Fingerprint()[:8])
		}
		if in.conn == nil {
			t.Error("no connection handed up, so no local port can be made for it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an authenticated inbound connection was never handed up; the " +
			"peer would reconnect forever while this side ignored it")
	}
}

// And a connection that fails to prove itself must NOT be handed up: the
// callback is what turns a connection into a usable peer, so an unverified
// one reaching it would make the identity check pointless.
func TestAnUnverifiedInboundConnectionIsNotHandedUp(t *testing.T) {
	got := make(chan string, 4)

	serverKey := testKey(t)
	cert := testCert(t)
	server, err := Listen(Config{
		Port: 0, Key: serverKey, Cluster: "theirs", Cert: &cert,
		Local:     echoService(t),
		OnInbound: func(node string, _ *quic.Conn) { got <- node },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = server.Serve(ctx) }()

	// A peer from a different cluster.
	client := endpoint(t, testKey(t), "mine", nil)
	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), server.Port())
	_, _ = client.Dial(ctx, serverKey.Fingerprint(), at)

	select {
	case node := <-got:
		t.Errorf("a peer from another cluster was handed up as usable: %s", node[:8])
	case <-time.After(1500 * time.Millisecond):
	}
}

// TestBothEndsCanOpenStreams.
//
// A QUIC connection is symmetric once established: either end may open a
// stream. Only the accepting side served them, so a peer we had dialled could
// never send us anything - and for a pair where one side can only dial, that
// is every request in one direction.
//
// Measured before this was fixed: the forwarder took the request, the stream
// went out, and nothing accepted it at the far end. curl waited twenty
// seconds and got zero bytes.
func TestBothEndsCanOpenStreams(t *testing.T) {
	// Both ends run a service, so a stream opened either way has somewhere
	// to land.
	serverKey, clientKey := testKey(t), testKey(t)
	server := endpoint(t, serverKey, "c1", echoService(t))
	client := endpoint(t, clientKey, "c1", echoService(t))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	at := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), server.Port())
	outbound, err := client.Dial(ctx, serverKey.Fingerprint(), at)
	if err != nil {
		t.Fatal(err)
	}
	defer outbound.CloseWithError(0, "")

	// The dialler opening a stream: the ordinary direction, and the one that
	// already worked.
	if got := roundTrip(t, ctx, outbound, "from the dialler"); got != "from the dialler" {
		t.Errorf("dialler -> accepter carried %q", got)
	}

	// And the accepter opening one back over the same connection, which is
	// what a peer that can only be dialled has to be able to do.
	server.mu.Lock()
	back := server.peers[clientKey.Fingerprint()]
	server.mu.Unlock()
	if back == nil {
		t.Fatal("the accepting end kept no handle on the connection, so it can " +
			"never originate anything to that peer")
	}
	if got := roundTrip(t, ctx, back, "from the accepter"); got != "from the accepter" {
		t.Errorf("accepter -> dialler carried %q; a peer we dialled cannot be "+
			"sent anything, which is every request in one direction for a pair "+
			"where only one side can dial", got)
	}
}

func roundTrip(t *testing.T, ctx context.Context, conn *quic.Conn, msg string) string {
	t.Helper()
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("opening a stream: %v", err)
	}
	if _, err := st.Write([]byte(msg)); err != nil {
		t.Fatalf("writing: %v", err)
	}
	_ = st.Close()
	// A deadline, so a link that carries nothing FAILS rather than hangs.
	// Without it the mutation that stops the dialler serving streams makes
	// this block until the whole suite is killed, and a test that hangs
	// reports nothing at all.
	_ = st.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, len(msg))
	n, _ := io.ReadFull(st, buf)
	return string(buf[:n])
}
