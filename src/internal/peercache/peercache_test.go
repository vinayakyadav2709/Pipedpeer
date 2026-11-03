package peercache

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
)

const (
	bashHash  = "byi2zpy2bgcf3dr6y0l8m50rmjj8z7q1"
	nixosKey  = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
	patchPath = "/nix/store/001gp43bjqzx60cg345n2slzg7131za8-nix-nss-open-files.patch"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseNI(t *testing.T, text string) *narinfo.NarInfo {
	t.Helper()
	ni, err := narinfo.Parse(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return ni
}

func key(t *testing.T, s string) signature.PublicKey {
	t.Helper()
	k, err := signature.ParsePublicKey(s)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestValidHash(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{bashHash, true},
		{bashHash[:31], false},
		{bashHash + "a", false},
		{strings.Repeat("e", 32), false},
		{strings.Repeat("o", 32), false},
		{strings.Repeat("u", 32), false},
		{strings.Repeat("t", 32), false},
		{strings.ToUpper(bashHash), false},
		{"../" + bashHash[:29], false},
		{"", false},
	}
	for _, c := range cases {
		if got := ValidHash(c.in); got != c.want {
			t.Errorf("ValidHash(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestAccept(t *testing.T) {
	real21 := string(fixture(t, "narinfo-2.1.txt"))
	nixos := []signature.PublicKey{key(t, nixosKey)}
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed := parseNI(t, real21)
	sig, err := sk.Sign(rand.Reader, signed.Fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	signed.Signatures = []signature.Signature{sig}
	tampered := parseNI(t, strings.Replace(real21, "NarHash: sha256:0i3p", "NarHash: sha256:1i3p", 1))
	zeroSize := parseNI(t, strings.Replace(real21, "NarSize: 1856848", "NarSize: 0", 1))
	ca := parseNI(t, "StorePath: "+patchPath+"\nURL: nar/x.nar.zst\nCompression: zstd\nNarHash: sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2\nNarSize: 1656\nReferences: \nCA: fixed:r:sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2\n")
	otherHash := strings.Repeat("a", 32)
	// Same CA claim under a path it does not produce (a peer forging a swap).
	caForged := parseNI(t, strings.Replace(real21, "References:", "CA: fixed:r:sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2\nReferences:", 1))
	caForged.Signatures = nil
	if caForged.CA == "" {
		t.Fatal("test setup: CA not parsed")
	}
	caForgedSigned := parseNI(t, strings.Replace(real21, "References:", "CA: fixed:r:sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2\nReferences:", 1))
	caForgedSigned.Signatures = parseNI(t, real21).Signatures
	badDir := parseNI(t, strings.Replace(real21, "StorePath: /nix/store/", "StorePath: /evil/", 1))

	cases := []struct {
		name    string
		ni      *narinfo.NarInfo
		hash    string
		trusted []signature.PublicKey
		wantErr string // "" = nil
	}{
		{"1 real, trusted", parseNI(t, real21), bashHash, nixos, ""},
		{"2 real, no keys", parseNI(t, real21), bashHash, nil, "no trusted signature"},
		{"3 tampered NarHash", tampered, bashHash, nixos, "no trusted signature"},
		{"4 CA, no sig", ca, "001gp43bjqzx60cg345n2slzg7131za8", nil, ""},
		{"5 other hash", parseNI(t, real21), otherHash, nixos, "store path mismatch"},
		{"6 own key trusted", signed, bashHash, []signature.PublicKey{pk}, ""},
		{"7 own key untrusted", signed, bashHash, nixos, "test-1"},
		{"9 CA that does not match the path, no sig", caForged, bashHash, nixos, "no trusted signature"},
		{"10 CA mismatch but trusted sig", caForgedSigned, bashHash, nixos, ""},
		{"11 store path outside /nix/store", badDir, bashHash, nixos, "not a valid"},
		{"8 NarSize 0", zeroSize, bashHash, nixos, "NarSize"},
	}
	for _, c := range cases {
		err := Accept(c.ni, c.hash, c.trusted)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: error %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

func TestTrustedKeys(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keys")
	os.WriteFile(f, []byte(nixosKey+" garbage\n"), 0o600)
	oldK, oldC, oldN := keysFile, nixConfig, now
	defer func() { keysFile, nixConfig, now = oldK, oldC, oldN }()
	keysFile = func() string { return f }
	calls := 0
	nixConfig = func(string) (string, error) { calls++; return nixosKey, nil }
	clock := oldN()
	now = func() time.Time { return clock }
	keyCache.keys = nil

	if got := TrustedKeys(); len(got) != 1 || got[0].Name != "cache.nixos.org-1" {
		t.Fatalf("keys = %v, want the one valid key, deduplicated", got)
	}
	TrustedKeys()
	if calls != 1 {
		t.Errorf("nix asked %d times within a minute, want 1", calls)
	}
	clock = clock.Add(61 * time.Second)
	TrustedKeys()
	if calls != 2 {
		t.Errorf("nix asked %d times after a minute, want 2", calls)
	}
}
