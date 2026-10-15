package identity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNodeIDIsTheKeyFingerprint: one identity. The id peers know a node by
// is the fingerprint of the key that proves it, so the two can never name
// different machines.
func TestNodeIDIsTheKeyFingerprint(t *testing.T) {
	isolate(t)

	id, err := GetOrCreate()
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}
	key, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	if id.NodeID != key.Fingerprint() {
		t.Fatalf("node id %q, key fingerprint %q", id.NodeID, key.Fingerprint())
	}
	if id.Arch == "" {
		t.Fatal("expected non-empty arch")
	}
	path := filepath.Join(os.Getenv("XDG_DATA_HOME"), "pipedpeer", "node_identity.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("identity file not created: %v", err)
	}
	again, err := GetOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if again.NodeID != id.NodeID {
		t.Fatalf("id changed between calls: %s then %s", id.NodeID, again.NodeID)
	}
}

// TestOldUUIDIdentityIsMigrated: a file written by an older build holds a
// UUID; it is replaced by the key fingerprint, and the rest is kept.
func TestOldUUIDIdentityIsMigrated(t *testing.T) {
	isolate(t)
	path := filepath.Join(os.Getenv("XDG_DATA_HOME"), "pipedpeer", "node_identity.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	old := `{"node_id":"a1b2c3d4-5678-4abc-9def-0123456789ab","hostname":"old-host","arch":"x86_64-linux"}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := GetOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := Key()
	if id.NodeID != key.Fingerprint() || strings.Contains(id.NodeID, "-") {
		t.Fatalf("UUID not replaced: %q", id.NodeID)
	}
	if id.Hostname != "old-host" {
		t.Fatalf("hostname lost in migration: %q", id.Hostname)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), key.Fingerprint()) {
		t.Fatalf("migrated id not written back: %s", b)
	}
}

func TestShortIDAndMatchesID(t *testing.T) {
	id := NodeIdentity{NodeID: "a1b2c3d4-5678-4abc-9def-0123456789ab"}
	if id.ShortID() != "a1b2c3d4" {
		t.Fatalf("expected a1b2c3d4, got %s", id.ShortID())
	}
	if !id.MatchesID("a1b2c3d4") {
		t.Fatal("expected short prefix match")
	}
	if !id.MatchesID(id.NodeID) {
		t.Fatal("expected full ID match")
	}
	if id.MatchesID("xxxxxxxx") {
		t.Fatal("should not match wrong ID")
	}
}

func fakeHost(t *testing.T, host, model string) {
	oldH, oldP := hostname, getprop
	hostname = func() (string, error) { return host, nil }
	getprop = func(string) string { return model }
	t.Cleanup(func() { hostname, getprop = oldH, oldP })
}

// TestPhoneIsNamedByModel: Android says "localhost" for every phone, which is
// no name at all in `ran on` lines; the device model is used instead, also
// for an identity stored before this, and a real hostname always wins.
func TestPhoneIsNamedByModel(t *testing.T) {
	isolate(t)
	fakeHost(t, "localhost", "SM-G781B")
	path := filepath.Join(os.Getenv("XDG_DATA_HOME"), "pipedpeer", "node_identity.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"hostname":"localhost","arch":"aarch64-linux"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := GetOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if id.Hostname != "SM-G781B" {
		t.Fatalf("stored localhost not renamed: %q", id.Hostname)
	}

	for _, c := range []struct{ host, model, want string }{
		{"yeet", "SM-G781B", "yeet"},
		{"localhost", "Pixel 7", "Pixel-7"},
		{"localhost", "", "node-c8789d22"},
		{"", "", "node-c8789d22"},
	} {
		fakeHost(t, c.host, c.model)
		if got := hostName("c8789d22aa"); got != c.want {
			t.Errorf("hostName(host %q, model %q) = %q, want %q", c.host, c.model, got, c.want)
		}
	}
}
