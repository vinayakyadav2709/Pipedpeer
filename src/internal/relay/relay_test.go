package relay

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"time"
)

// testCert makes a throwaway self-signed certificate, which is what the relay
// presents in production too - its identity is pinned, not chained.
func testCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "pipedpeer-relay-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func acceptAny(rawCerts [][]byte, _ [][]*x509.Certificate) error { return nil }

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

// startRelay brings up a relay on a loopback port.
func startRelay(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := Listen("127.0.0.1:0", testCert(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv := NewServer()
	go func() { _ = srv.Serve(ctx, ln) }()
	return ln.Addr().String(), func() { cancel(); ln.Close() }
}

// TestHTTPSurvivesTheRelayUnchanged is the property that makes this worth
// building: everything above the transport - the job protocol, the gradient
// sync, results - is HTTP, and none of it should have to know it is being
// relayed.
func TestHTTPSurvivesTheRelayUnchanged(t *testing.T) {
	addr, stop := startRelay(t)
	defer stop()

	// The service the far peer is fronting.
	backend := http.NewServeMux()
	backend.HandleFunc("/v1/hello", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "method=%s path=%s body=%q", r.Method, r.URL.Path, body)
	})
	hs := &http.Server{Handler: backend}
	bl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer bl.Close()
	go hs.Serve(bl)

	workerKey, submitterKey := testKey(t), testKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The peer that is being reached: it answers relayed streams by dialling
	// its own service.
	server, err := Dial(ctx, addr, "c1", workerKey, acceptAny, LocalDialer(bl.Addr().String()))
	if err != nil {
		t.Fatalf("worker dial: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()

	// The peer doing the reaching.
	client, err := Dial(ctx, addr, "c1", submitterKey, acceptAny, nil)
	if err != nil {
		t.Fatalf("submitter dial: %v", err)
	}
	defer client.Close()

	// An ordinary HTTP client, over a stream the relay carries.
	httpc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				st, err := client.Open(ctx, workerKey.Fingerprint())
				if err != nil {
					return nil, err
				}
				return NetConn(st), nil
			},
			DisableKeepAlives: true,
		},
		Timeout: 15 * time.Second,
	}

	resp, err := httpc.Post("http://relayed/v1/hello", "text/plain", strings.NewReader("ping"))
	if err != nil {
		t.Fatalf("relayed request: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	want := `method=POST path=/v1/hello body="ping"`
	if string(got) != want {
		t.Errorf("relayed response = %q, want %q", got, want)
	}
}

// TestUnknownPeerIsRefusedNotHung. A submitter naming a peer that is not
// connected should be told so, not left waiting for a stream that will never
// carry anything.
func TestUnknownPeerIsRefusedNotHung(t *testing.T) {
	addr, stop := startRelay(t)
	defer stop()

	submitterKey := testKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Dial(ctx, addr, "c1", submitterKey, acceptAny, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, err := client.Open(ctx, "nobody")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("opening a stream to an unconnected peer succeeded")
		}
		if !strings.Contains(err.Error(), "not connected") {
			t.Errorf("error was %q; it should name the reason", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("opening a stream to an unconnected peer hung rather than failing")
	}
}

// TestClustersCannotReachEachOther. The cluster id comes from the shared
// token, so crossing it would let one user's submitter address another user's
// machines through a relay they happen to share.
func TestClustersCannotReachEachOther(t *testing.T) {
	addr, stop := startRelay(t)
	defer stop()

	workerKey, submitterKey := testKey(t), testKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	victim, err := Dial(ctx, addr, "theirs", workerKey, acceptAny, LocalDialer("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()
	go func() { _ = victim.Serve(ctx) }()

	attacker, err := Dial(ctx, addr, "mine", submitterKey, acceptAny, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer attacker.Close()

	if _, err := attacker.Open(ctx, workerKey.Fingerprint()); err == nil {
		t.Error("a peer in one cluster opened a stream to a peer in another")
	}
}

// TestReconnectDoesNotUnregisterTheLiveConnection. A peer whose connection
// drops and comes straight back replaces its own entry; the old connection's
// cleanup must not then remove the new one, or the peer becomes unreachable
// while believing it is connected.
func TestReconnectDoesNotUnregisterTheLiveConnection(t *testing.T) {
	type conn struct{ id int }
	r := newRegistry[*conn]()
	old, fresh := &conn{1}, &conn{2}

	r.put("c1", "n1", old)
	r.put("c1", "n1", fresh) // the peer reconnected
	r.drop("c1", "n1", old)  // the old connection's cleanup runs late

	got, ok := r.get("c1", "n1")
	if !ok {
		t.Fatal("the peer is gone: its stale connection unregistered the live one")
	}
	if got != fresh {
		t.Errorf("registry holds %v, want the reconnected one", got)
	}
}

// TestRawBytesSurviveTheRelay isolates the splice from anything HTTP, so a
// failure points at one or the other rather than at both.
//
// It is what found the real bug: the first version framed its headers with a
// json.Decoder, which consumes the JSON value but leaves the terminating
// newline in its own buffer. Forwarding what it buffered injected a stray byte
// into every payload - "\n\n\nthe quick brown fox". Over HTTP the same fault
// corrupts the request line and surfaces as "malformed HTTP response", which
// points nowhere near the cause.
func TestRawBytesSurviveTheRelay(t *testing.T) {
	addr, stop := startRelay(t)
	defer stop()

	bl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer bl.Close()
	go func() {
		for {
			c, err := bl.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()

	workerKey, submitterKey := testKey(t), testKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	worker, err := Dial(ctx, addr, "c1", workerKey, acceptAny, LocalDialer(bl.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	go func() { _ = worker.Serve(ctx) }()

	sub, err := Dial(ctx, addr, "c1", submitterKey, acceptAny, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	st, err := sub.Open(ctx, workerKey.Fingerprint())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	const msg = "the quick brown fox"
	if _, err := st.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(buf) != msg {
		t.Errorf("echoed %q, want %q — the payload was altered in transit", buf, msg)
	}
}

// TestIdentityCannotBeClaimedWithoutTheKey is the reason the hello is signed.
//
// A relay introduces strangers. A peer that merely asserts a name asserts
// whoever's name it likes, and the peer being impersonated has no way to
// notice - the submitter's job, its data and its results would go to the
// impostor. The address is therefore the fingerprint of the key the peer signs
// with, so taking it requires the private half.
func TestIdentityCannotBeClaimedWithoutTheKey(t *testing.T) {
	addr, stop := startRelay(t)
	defer stop()

	victimKey := testKey(t)
	impostorKey := testKey(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The impostor knows the victim's fingerprint - it is public - and dials
	// with its own key.
	impostor, err := Dial(ctx, addr, "c1", impostorKey, acceptAny, LocalDialer("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer impostor.Close()
	go func() { _ = impostor.Serve(ctx) }()

	if impostor.Node() == victimKey.Fingerprint() {
		t.Fatal("two distinct keys produced the same address")
	}

	// A submitter asking for the victim must not be given the impostor.
	sub, err := Dial(ctx, addr, "c1", testKey(t), acceptAny, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := sub.Open(ctx, victimKey.Fingerprint()); err == nil {
		t.Error("a stream to the victim's address was opened while only an impostor " +
			"was connected; the address was claimable without the key")
	}
}

// TestForgedHelloIsRefused. The signature has to be checked, not merely
// carried: a hello whose signature does not match the key it offers is exactly
// what an impersonation attempt looks like.
func TestForgedHelloIsRefused(t *testing.T) {
	victim := testKey(t)
	forger := testKey(t)

	// A hello offering the victim's public key, signed by somebody else.
	nonce := "abcd"
	sig := forger.Sign(helloContext, []byte("c1|"+nonce))
	h := hello{
		Cluster: "c1",
		PubKey:  identity.EncodePublic(victim.Public),
		Nonce:   nonce,
		Sig:     base64.StdEncoding.EncodeToString(sig),
	}
	if _, err := h.verify(); err == nil {
		t.Error("a hello signed by one key while offering another was accepted")
	}

	// And the honest case still works, or the check is simply broken.
	good := hello{
		Cluster: "c1",
		PubKey:  identity.EncodePublic(victim.Public),
		Nonce:   nonce,
		Sig:     base64.StdEncoding.EncodeToString(victim.Sign(helloContext, []byte("c1|"+nonce))),
	}
	got, err := good.verify()
	if err != nil {
		t.Fatalf("a correctly signed hello was refused: %v", err)
	}
	if got != victim.Fingerprint() {
		t.Errorf("registered address %q, want the key's fingerprint %q",
			got, victim.Fingerprint())
	}
}
