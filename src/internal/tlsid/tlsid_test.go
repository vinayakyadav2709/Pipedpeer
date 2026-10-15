package tlsid

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/pipedpeer/pipedpeer/internal/identity"
)

func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	pins.mu.Lock()
	pins.pins = map[string]pin{}
	pins.read = false
	pins.mu.Unlock()
}

// newNode makes a node key and its certificate, standing in for another
// machine.
func newNode(t *testing.T) (string, []byte) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	c, err := certFor(identity.KeyPair{Public: pub, Private: priv})
	if err != nil {
		t.Fatal(err)
	}
	return identity.Fingerprint(pub), c.Certificate[0]
}

func TestEnsureCertIsTheNodeKey(t *testing.T) {
	isolate(t)
	a, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	b, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	if Fingerprint(a.Certificate[0]) != Fingerprint(b.Certificate[0]) {
		t.Error("certificate changed between calls")
	}
	key, err := identity.Key()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(a.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if !key.Public.Equal(leaf.PublicKey) {
		t.Fatal("certificate key is not the node key")
	}
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature); err != nil {
		t.Fatalf("certificate does not verify against the node key: %v", err)
	}
	node, err := NodeOf(a.Certificate[0])
	if err != nil || node != key.Fingerprint() {
		t.Fatalf("NodeOf = %q, %v; want %q", node, err, key.Fingerprint())
	}
}

// TestNewMachineAtAnOldAddressIsAccepted: DHCP hands an old peer's IP to a
// different machine. Nothing expected that address to be anyone, so the new
// key is recorded, not refused.
func TestNewMachineAtAnOldAddressIsAccepted(t *testing.T) {
	isolate(t)
	nodeA, certA := newNode(t)
	nodeB, certB := newNode(t)
	const addr = "192.0.2.10:4242"
	if err := CheckOrPin(addr, certA); err != nil {
		t.Fatalf("first contact refused: %v", err)
	}
	if got := pins.pins[addr].Node; got != nodeA {
		t.Fatalf("recorded %q, want %q", got, nodeA)
	}
	if err := CheckOrPin(addr, certB); err != nil {
		t.Fatalf("new machine at an old address refused: %v", err)
	}
	if got := pins.pins[addr].Node; got != nodeB {
		t.Fatalf("recorded %q, want %q", got, nodeB)
	}
}

// TestExpectRefusesAChangedKey: where the address book says an address is a
// node, another key there is refused, and the expected key is accepted at
// any address it is expected at.
func TestExpectRefusesAChangedKey(t *testing.T) {
	isolate(t)
	nodeA, certA := newNode(t)
	_, certB := newNode(t)
	Expect("peer:4242", nodeA)
	err := CheckOrPin("peer:4242", certB)
	if err == nil {
		t.Fatal("a changed key for an expected node was accepted")
	}
	// dial.go refuses to fall back to plain HTTP on this prefix.
	if !strings.HasPrefix(err.Error(), "certificate for") {
		t.Fatalf("error %q lost the prefix the plain-HTTP fallback checks", err)
	}
	if err := CheckOrPin("peer:4242", certA); err != nil {
		t.Fatalf("expected node refused: %v", err)
	}
	Expect("10.24.7.5:4242", nodeA)
	if err := CheckOrPin("10.24.7.5:4242", certA); err != nil {
		t.Fatalf("expected node refused at its new address: %v", err)
	}
	Forget("peer:4242")
	if err := CheckOrPin("peer:4242", certB); err != nil {
		t.Fatalf("after forgetting, the new key should be accepted: %v", err)
	}
}

// TestOldBuildCertificateIsRefused: a P-256 certificate from an older build
// is not a node key (hard cut, ADR-0006).
func TestOldBuildCertificateIsRefused(t *testing.T) {
	isolate(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckOrPin("old:4242", der); err == nil {
		t.Fatal("an old-build P-256 certificate was accepted")
	}
}

// TestListenerServesBothProtocols is the property that makes upgrading a
// cluster possible at all: without it every daemon would have to switch
// scheme at the same moment, and anything missed would simply stop talking.
func TestListenerServesBothProtocols(t *testing.T) {
	isolate(t)
	cert, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := NewListener(raw, &cert)
	defer ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "tls=%v", r.TLS != nil)
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()

	addr := raw.Addr().String()

	get := func(client *http.Client, url string) (string, error) {
		resp, err := client.Get(url)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}

	plain, err := get(&http.Client{}, "http://"+addr+"/hello")
	if err != nil {
		t.Fatalf("plain HTTP failed on a TLS-capable listener: %v", err)
	}
	if plain != "tls=false" {
		t.Errorf("plain request reported %q", plain)
	}

	secure, err := get(&http.Client{Transport: &http.Transport{
		TLSClientConfig: ClientConfig(addr),
	}}, "https://"+addr+"/hello")
	if err != nil {
		t.Fatalf("TLS failed on the same listener: %v", err)
	}
	if secure != "tls=true" {
		t.Errorf("TLS request reported %q", secure)
	}
}

// TestClientConfigRejectsASwappedCertificate proves the pin is doing work
// rather than InsecureSkipVerify simply accepting anything.
func TestClientConfigRejectsASwappedCertificate(t *testing.T) {
	isolate(t)

	serve := func(cert tls.Certificate) (string, func()) {
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ln := tls.NewListener(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
		go srv.Serve(ln)
		return raw.Addr().String(), func() { srv.Close() }
	}

	cert, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	self, _ := identity.Key()
	Expect("pinned-peer", self.Fingerprint())
	addr, stop := serve(cert)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: ClientConfig("pinned-peer")}}
	if _, err := client.Get("https://" + addr); err != nil {
		t.Fatalf("expected node refused: %v", err)
	}
	stop()

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	other, err := certFor(identity.KeyPair{Public: pub, Private: priv})
	if err != nil {
		t.Fatal(err)
	}
	addr2, stop2 := serve(other)
	defer stop2()
	client2 := &http.Client{Transport: &http.Transport{TLSClientConfig: ClientConfig("pinned-peer")}}
	if _, err := client2.Get("https://" + addr2); err == nil {
		t.Fatal("a different key for an expected node was accepted")
	}
}

// TestQUICAcceptsTheNodeCertificate: the direct links (quic-go, TLS 1.3)
// serve the same ed25519 certificate.
func TestQUICAcceptsTheNodeCertificate(t *testing.T) {
	isolate(t)
	cert, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert}, NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		if c, err := ln.Accept(ctx); err == nil {
			<-c.Context().Done()
		}
	}()
	conn, err := quic.DialAddr(ctx, ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true, NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13,
	}, nil)
	if err != nil {
		t.Fatalf("quic handshake with the ed25519 certificate failed: %v", err)
	}
	defer conn.CloseWithError(0, "")
	got, err := NodeOf(conn.ConnectionState().TLS.PeerCertificates[0].Raw)
	key, _ := identity.Key()
	if err != nil || got != key.Fingerprint() {
		t.Fatalf("quic peer is %q (%v), want %q", got, err, key.Fingerprint())
	}
}

// dirForTest exposes the package's state directory to tests without widening
// the API.
func dirForTest() string { return dir() }

// TestForgetTakesEffectWithoutARestart covers advice that did not work.
//
// When a peer's certificate changes, the daemon logs "run `pipedpeer auth
// forget <peer>` if it was reinstalled". That command rewrites the pin file -
// but the daemon had loaded the file once and never looked again, so it went
// on refusing the peer from a copy in memory. The operator followed the
// instruction, nothing changed, and nothing said why.
func TestForgetTakesEffectWithoutARestart(t *testing.T) {
	isolate(t)

	const peer = "192.0.2.10:4242"
	nodeA, _ := newNode(t)
	_, certB := newNode(t)
	Expect(peer, nodeA)
	// A different key for an expected node is refused, as it should be.
	if err := CheckOrPin(peer, certB); err == nil {
		t.Fatal("a changed key was accepted")
	}

	// Another process - the CLI - drops the pin. Simulated by writing the
	// file the way `auth forget` does, then clearing this process's memory of
	// having read it is exactly what must NOT be needed.
	forgetFromAnotherProcess(t, peer)

	if err := CheckOrPin(peer, certB); err != nil {
		t.Errorf("still refusing the peer after the pin was dropped on disk: %v\n"+
			"the daemon is answering from a copy in memory, so the fix it "+
			"recommends does nothing until it is restarted", err)
	}
}

// forgetFromAnotherProcess rewrites the pin file without going through this
// process's in-memory store, which is what a separate `pipedpeer auth forget`
// invocation does.
func forgetFromAnotherProcess(t *testing.T, peer string) {
	t.Helper()
	path := filepath.Join(dirForTest(), "pins.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the pin file: %v", err)
	}
	var onDisk map[string]json.RawMessage
	if err := json.Unmarshal(b, &onDisk); err != nil {
		t.Fatalf("parsing the pin file: %v", err)
	}
	if _, ok := onDisk[peer]; !ok {
		t.Fatalf("%s was never written to the pin file; the test is not testing anything", peer)
	}
	delete(onDisk, peer)
	out, _ := json.Marshal(onDisk)
	// Written a moment later so the modification time genuinely differs even
	// on a filesystem with coarse timestamps.
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatalf("writing the pin file: %v", err)
	}
}
