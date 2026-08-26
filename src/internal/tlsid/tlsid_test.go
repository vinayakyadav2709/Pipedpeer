package tlsid

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
)

func isolate(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", dir)
	pins.mu.Lock()
	pins.pins = map[string]string{}
	pins.mu.Unlock()
}

func TestEnsureCertIsStableAcrossCalls(t *testing.T) {
	isolate(t)
	a, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	b, err := EnsureCert()
	if err != nil {
		t.Fatal(err)
	}
	// A daemon that minted a new identity on every start would break every
	// peer's pin, which is the same signal as an attacker.
	if Fingerprint(a.Certificate[0]) != Fingerprint(b.Certificate[0]) {
		t.Error("certificate changed between calls")
	}
	if info, err := os.Stat(dir() + "/daemon.key"); err == nil {
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("private key mode %o; it is the node's identity", perm)
		}
	}
}

func TestPinningAcceptsFirstContactAndRejectsChange(t *testing.T) {
	isolate(t)
	first := []byte("certificate-one")
	if err := CheckOrPin("peer:38080", first); err != nil {
		t.Fatalf("first contact rejected: %v", err)
	}
	if err := CheckOrPin("peer:38080", first); err != nil {
		t.Fatalf("same certificate rejected: %v", err)
	}
	// A changed certificate is either a reinstall or someone in the middle,
	// and only the operator can tell those apart — so it must not be
	// silently accepted.
	err := CheckOrPin("peer:38080", []byte("certificate-two"))
	if err == nil {
		t.Fatal("a changed certificate was accepted silently")
	}
	Forget("peer:38080")
	if err := CheckOrPin("peer:38080", []byte("certificate-two")); err != nil {
		t.Fatalf("after forgetting the pin, the new certificate should be accepted: %v", err)
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

	serve := func() (string, func()) {
		cert, err := EnsureCert()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ln := tls.NewListener(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
		go srv.Serve(ln)
		return raw.Addr().String(), func() { srv.Close() }
	}

	addr, stop := serve()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: ClientConfig("pinned-peer")}}
	if _, err := client.Get("https://" + addr); err != nil {
		t.Fatalf("first contact should pin, not fail: %v", err)
	}
	stop()

	// Same logical peer, different identity: a fresh cert in a fresh dir.
	isolateKeepPins(t)
	addr2, stop2 := serve()
	defer stop2()
	client2 := &http.Client{Transport: &http.Transport{TLSClientConfig: ClientConfig("pinned-peer")}}
	if _, err := client2.Get("https://" + addr2); err == nil {
		t.Fatal("a different certificate for a pinned peer was accepted")
	}
}

// isolateKeepPins swaps the cert directory but keeps the in-memory pins, so a
// test can present a genuinely different certificate for the same peer name.
func isolateKeepPins(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
}
