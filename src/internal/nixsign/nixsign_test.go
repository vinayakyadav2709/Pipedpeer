package nixsign

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

func testKey(t *testing.T) identity.KeyPair {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return identity.KeyPair{Public: pub, Private: priv}
}

// TestTheSigningKeyIsTheNodeIdentity.
//
// There is no second key to generate, store, rotate or lose. A path signed by
// a node is signed by the same identity that authenticated the connection it
// arrived on, and the two cannot drift apart - which is the property that
// makes a published key checkable rather than taken on faith.
func TestTheSigningKeyIsTheNodeIdentity(t *testing.T) {
	k := testKey(t)

	name, b64, ok := strings.Cut(PublicKey(k), ":")
	if !ok {
		t.Fatalf("public key %q is not name:key", PublicKey(k))
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("key material does not decode: %v", err)
	}
	if string(raw) != string(k.Public) {
		t.Error("the published key is not this node's identity key")
	}
	// Nix splits name from key on a colon, so a name containing one would
	// make the key unparseable to the tool that has to read it.
	if strings.Contains(name, ":") {
		t.Errorf("key name %q contains a colon, which nix uses as the separator", name)
	}
	if !strings.HasPrefix(name, "pipedpeer-") {
		t.Errorf("key name %q does not say where it came from", name)
	}
}

// TestAKeyCannotBeOfferedForSomebodyElse.
//
// Keys travel with node records, and a record can claim anything. What makes
// distribution safe is that the key must hash to the fingerprint the
// connection was authenticated under: only the holder of the private half can
// produce a matching pair, so a peer can neither offer another node's key nor
// one it does not own.
func TestAKeyCannotBeOfferedForSomebodyElse(t *testing.T) {
	mine, theirs := testKey(t), testKey(t)

	if err := Verify(PublicKey(mine), mine.Fingerprint()); err != nil {
		t.Errorf("a node's own key was rejected: %v", err)
	}

	// Somebody else's key offered under my fingerprint.
	if err := Verify(PublicKey(theirs), mine.Fingerprint()); err == nil {
		t.Error("another node's key was accepted under this node's fingerprint")
	}

	// My key material relabelled with another node's name. The name is what
	// appears in signatures, so trusting this would trust their signatures.
	relabelled := KeyName(theirs) + ":" + base64.StdEncoding.EncodeToString(mine.Public)
	if err := Verify(relabelled, mine.Fingerprint()); err == nil {
		t.Error("a key whose label and bytes disagree was accepted")
	}

	for _, bad := range []string{
		"no-colon-here",
		"name:not-base64!!",
		"name:" + base64.StdEncoding.EncodeToString([]byte("too short")),
		"",
	} {
		if err := Verify(bad, mine.Fingerprint()); err == nil {
			t.Errorf("malformed key %q was accepted", bad)
		}
	}
}

// PublicKeyFor must agree with PublicKey, or a peer would derive a different
// key than the one the node publishes and trust nothing it signs.
func TestDerivedAndPublishedKeysAgree(t *testing.T) {
	k := testKey(t)
	if PublicKeyFor(k.Public) != PublicKey(k) {
		t.Errorf("derived %q but published %q", PublicKeyFor(k.Public), PublicKey(k))
	}
}

// TestTrustedKeysAreRewrittenNotAppended.
//
// A peer that leaves must stop being trusted. Appending would keep every key
// the machine had ever seen, so removing a compromised node from the cluster
// would not remove its ability to have its closures accepted.
func TestTrustedKeysAreRewrittenNotAppended(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())

	a, b, c := testKey(t), testKey(t), testKey(t)
	if err := WriteTrustedKeys([]string{PublicKey(a), PublicKey(b)}); err != nil {
		t.Fatal(err)
	}
	// b leaves the cluster, c joins.
	if err := WriteTrustedKeys([]string{PublicKey(a), PublicKey(c)}); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(TrustedKeysFile())
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	if strings.Contains(got, KeyName(b)) {
		t.Error("a peer that left is still trusted; the file was appended to, not rewritten")
	}
	for _, want := range []identity.KeyPair{a, c} {
		if !strings.Contains(got, KeyName(want)) {
			t.Errorf("current member %s is not trusted", KeyName(want))
		}
	}
	// Space separated on one line, which is the format nix reads.
	if strings.Count(strings.TrimSpace(got), "\n") != 0 {
		t.Errorf("trusted keys span several lines; nix expects one setting: %q", got)
	}
}

// Duplicates and blanks would make the file grow without bound as peers
// re-register.
func TestTrustedKeysAreDeduplicated(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	a := testKey(t)
	if err := WriteTrustedKeys([]string{PublicKey(a), PublicKey(a), "", "  "}); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(TrustedKeysFile())
	if n := strings.Count(string(body), KeyName(a)); n != 1 {
		t.Errorf("key appears %d times, want 1", n)
	}
}

// TestTheSecretKeyIsNotWorldReadable. It is on disk because `nix store sign`
// takes a file, and a key readable by every user on the machine is a key
// anybody can sign as this node with.
func TestTheSecretKeyIsNotWorldReadable(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	k := testKey(t)

	path, cleanup, err := keyFile(k)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("secret key is mode %o; anybody on this machine could sign as this node", mode)
	}
	body, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(body), KeyName(k)+":") {
		t.Errorf("key file is not in nix's name:key form: %q", string(body))
	}

	// And it does not outlive the signing run.
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("the secret key was left on disk after signing")
	}
}

// TestNixItselfAcceptsThisKey is the claim everything here rests on: that a
// key derived from the node identity is one nix will actually sign with.
//
// Everything above checks the encoding against what the documentation says.
// This checks it against the tool, because the whole design is "let nix
// decide" and a key nix rejects would leave the cluster unable to import
// anything at all - a strictly worse failure than the unsigned imports this
// replaces.
func TestNixItselfAcceptsThisKey(t *testing.T) {
	nixPath, err := nixstore.SystemNix()
	if err != nil {
		t.Skipf("no nix on this machine to ask: %v", err)
	}
	t.Setenv("XDG_DATA_HOME", t.TempDir())

	k := testKey(t)
	file, cleanup, err := keyFile(k)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Signing nothing is a no-op that still parses the key: nix reads and
	// validates --key-file before it looks at the paths, so a malformed key
	// is reported as such rather than as a missing path.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, nixPath,
		"--extra-experimental-features", "nix-command",
		"store", "sign", "--key-file", file, "--dry-run")
	out, _ := cmd.CombinedOutput()

	// A key nix cannot parse says so explicitly. Anything else - including
	// complaints about missing paths or dry-run - means the key was read.
	for _, bad := range []string{"corrupt secret key", "invalid base64", "key is not valid"} {
		if strings.Contains(strings.ToLower(string(out)), bad) {
			t.Fatalf("nix rejected a key derived from the node identity: %s", out)
		}
	}
}

// TestOnlyKeysThatProveThemselvesAreTrusted.
//
// Keys arrive in node records, and a record is whatever the peer put in it.
// What makes accepting them safe is that the material must hash to the
// fingerprint it is published under - which only the holder of the private
// half can arrange - so a peer can neither promote another node's key nor
// invent one.
func TestOnlyKeysThatProveThemselvesAreTrusted(t *testing.T) {
	self, honest, impostor := testKey(t), testKey(t), testKey(t)

	trusted, rejected := Collect(self, map[string]string{
		honest.Fingerprint(): PublicKey(honest),
		// An impostor publishing somebody else's key under its own name.
		impostor.Fingerprint(): PublicKey(honest),
		"deadbeefdeadbeef":     "garbage",
	})

	has := func(list []string, want string) bool {
		for _, k := range list {
			if k == want {
				return true
			}
		}
		return false
	}
	if !has(trusted, PublicKey(honest)) {
		t.Error("an honest peer's key was not trusted")
	}
	// Its own key, or a machine refuses closures it built and signed itself
	// after a store collection.
	if !has(trusted, PublicKey(self)) {
		t.Error("this node does not trust its own key; it would refuse its own work")
	}
	if len(trusted) != 2 {
		t.Errorf("trusted %d keys, want 2 (self and the honest peer): %v", len(trusted), trusted)
	}
	if len(rejected) != 2 {
		t.Errorf("rejected %d, want 2 (the impostor and the garbage): %v", len(rejected), rejected)
	}
	// The rejection has to name the peer, or an operator cannot tell which
	// machine is misconfigured or lying.
	joined := strings.Join(rejected, " ")
	if !strings.Contains(joined, impostor.Fingerprint()[:8]) {
		t.Errorf("rejections do not name the impostor: %v", rejected)
	}
}

// A key's fingerprint is a function of its material, so it can be recovered
// rather than looked up - and cannot disagree with what is stored elsewhere.
func TestAKeyCarriesItsOwnFingerprint(t *testing.T) {
	k := testKey(t)
	got, ok := FingerprintOf(PublicKey(k))
	if !ok || got != k.Fingerprint() {
		t.Errorf("FingerprintOf = %q/%v, want %q", got, ok, k.Fingerprint())
	}
	if _, ok := FingerprintOf("nonsense"); ok {
		t.Error("nonsense yielded a fingerprint")
	}
}
