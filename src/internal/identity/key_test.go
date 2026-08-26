package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	keyOnce.Once = onceReset()
	keyOnce.kp, keyOnce.err = KeyPair{}, nil
	t.Cleanup(func() {
		keyOnce.Once = onceReset()
		keyOnce.kp, keyOnce.err = KeyPair{}, nil
	})
}

// TestKeyIsStableAcrossCalls. The fingerprint is the address peers reach this
// node at; a key that changed per process would make the node a stranger every
// time it restarted.
func TestKeyIsStableAcrossCalls(t *testing.T) {
	isolate(t)

	first, err := Key()
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	// Force a reload from disk, which is what a restart does.
	keyOnce.Once = onceReset()
	keyOnce.kp = KeyPair{}

	second, err := Key()
	if err != nil {
		t.Fatalf("key after reload: %v", err)
	}
	if first.Fingerprint() != second.Fingerprint() {
		t.Errorf("fingerprint changed across a reload: %s then %s",
			first.Fingerprint(), second.Fingerprint())
	}
}

// TestKeyIsWrittenPrivately. A signing key readable by every user on the
// machine proves nothing about which of them is speaking.
func TestKeyIsWrittenPrivately(t *testing.T) {
	isolate(t)
	if _, err := Key(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(keyPath())
	if err != nil {
		t.Fatalf("no key file: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("key file mode is %o; it must not be readable by other users", perm)
	}
}

// TestCorruptKeyIsNotSilentlyReplaced. Generating a fresh identity would make
// every peer that pinned this node see a stranger, with nothing said. Better
// to refuse and let the operator decide.
func TestCorruptKeyIsNotSilentlyReplaced(t *testing.T) {
	isolate(t)
	if err := os.MkdirAll(filepath.Dir(keyPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath(), []byte("not a key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Key(); err == nil {
		t.Error("a corrupt key file was replaced silently; every peer that had " +
			"pinned this node would see a stranger and nothing would say why")
	}
}

// TestSignatureVerifies is the basic contract.
func TestSignatureVerifies(t *testing.T) {
	isolate(t)
	k, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("cluster-abc|nonce-123")
	sig := k.Sign("relay-hello", msg)
	if !Verify(k.Public, "relay-hello", msg, sig) {
		t.Fatal("a signature this node made does not verify")
	}
	if Verify(k.Public, "relay-hello", []byte("cluster-abc|nonce-124"), sig) {
		t.Error("a signature verified against a different message")
	}
}

// TestContextStopsReplayAcrossPurposes. Without binding a signature to what it
// was for, a hello to a public relay would also be a valid authorisation to do
// something else entirely - and the relay is exactly where a stranger gets to
// choose the message.
func TestContextStopsReplayAcrossPurposes(t *testing.T) {
	isolate(t)
	k, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("the same bytes")
	sig := k.Sign("relay-hello", msg)
	if Verify(k.Public, "run-job", msg, sig) {
		t.Error("a signature made for one purpose verified for another")
	}
}

// TestForeignKeyDoesNotVerify. The whole point is that the fingerprint cannot
// be claimed by anyone without the private half.
func TestForeignKeyDoesNotVerify(t *testing.T) {
	isolate(t)
	k, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("hello")
	sig := k.Sign("relay-hello", msg)
	if Verify(other, "relay-hello", msg, sig) {
		t.Error("a signature verified against somebody else's public key")
	}
}

// TestFingerprintDependsOnlyOnTheKey. Peers derive the address independently;
// if it depended on anything local they would compute different names for the
// same node.
func TestFingerprintDependsOnlyOnTheKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := Fingerprint(pub)
	b := Fingerprint(pub)
	if a != b || a == "" {
		t.Errorf("fingerprint is not stable: %q then %q", a, b)
	}

	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if Fingerprint(other) == a {
		t.Error("two different keys share a fingerprint")
	}
}

// TestPublicKeyRoundTrips through the encoding used on the wire.
func TestPublicKeyRoundTrips(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePublic(EncodePublic(pub))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if Fingerprint(got) != Fingerprint(pub) {
		t.Error("a public key changed identity in transit")
	}
	if _, err := DecodePublic("short"); err == nil {
		t.Error("a truncated key was accepted")
	}
}
