package narpack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The round trip against a real nix, which is the only thing that can settle
// the question this package exists for.
//
// The previous attempt at signed closures was committed, and believed, on the
// strength of "the paths carry a signature" - which was true, and useless,
// because `nix-store --export` does not carry signatures with them. Nothing
// short of exporting, transferring and importing under a store that REQUIRES
// signatures can tell the two situations apart, and the negative case
// matters as much as the positive: a check that has only ever been seen to
// pass has not been seen to work.
//
// No machine configuration is touched. Both halves run against scratch stores
// with their rules given per invocation.

func nixOrSkip(t *testing.T) string {
	t.Helper()
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix is not installed")
	}
	return nix
}

// signingKey makes a throwaway nix key pair in nix's own name:base64 form.
func signingKey(t *testing.T, name string) (secretFile, public string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	secretFile = filepath.Join(t.TempDir(), "sign.key")
	body := name + ":" + base64.StdEncoding.EncodeToString(priv)
	if err := os.WriteFile(secretFile, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return secretFile, name + ":" + base64.StdEncoding.EncodeToString(pub)
}

// publishOne builds a trivial store path, signs it, and publishes it to a
// cache directory. Returns the store path and the cache directory.
func publishOne(t *testing.T, nix, secretFile string) (storePath, cache string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Built by a derivation, NOT added with `nix store add-path`.
	//
	// add-path produces a CONTENT-addressed path (ca = fixed:r:sha256:...),
	// and nix does not require a signature on one of those: the hash in the
	// name verifies the contents on its own, so a signature would add
	// nothing. A fixture built that way is accepted by a store that trusts
	// nobody, and the negative test below passes while proving nothing -
	// which is exactly what it did until this was found.
	//
	// A real pipedpeer closure is input-addressed, like this one.
	expr := `derivation {
  name = "narpack-fixture";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo pipedpeer-narpack > $out" ];
}`
	file := filepath.Join(t.TempDir(), "fixture.nix")
	if err := os.WriteFile(file, []byte(expr), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, nix, "--extra-experimental-features",
		"nix-command", "build", "--impure", "--file", file, "--no-link",
		"--print-out-paths", "--option", "sandbox", "false").Output()
	if err != nil {
		t.Skipf("cannot build a fixture derivation on this machine: %v", err)
	}
	storePath = strings.TrimSpace(string(out))
	if storePath == "" {
		t.Skip("the fixture build printed no store path")
	}

	// The fixture must be input-addressed, or none of this tests anything.
	info, err := exec.CommandContext(ctx, nix, "--extra-experimental-features",
		"nix-command", "path-info", "--json", storePath).Output()
	if err != nil {
		t.Fatalf("path-info on the fixture: %v", err)
	}
	if bytes.Contains(info, []byte(`"ca":"fixed:`)) || bytes.Contains(info, []byte(`"ca": "fixed:`)) {
		t.Fatalf("the fixture is content-addressed, so nix will never require "+
			"a signature for it and the negative test below cannot fail:\n%s", info)
	}

	if o, err := exec.CommandContext(ctx, nix, "--extra-experimental-features",
		"nix-command", "store", "sign", "--key-file", secretFile, storePath).CombinedOutput(); err != nil {
		t.Fatalf("signing %s: %v: %s", storePath, err, o)
	}

	cache = filepath.Join(t.TempDir(), "cache")
	if err := Publish(ctx, cache, []string{storePath}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	return storePath, cache
}

// TestASignedClosureSurvivesTheTransfer is the claim the last attempt got
// wrong: that a signature is still there on the far side.
func TestASignedClosureSurvivesTheTransfer(t *testing.T) {
	nix := nixOrSkip(t)
	secret, _ := signingKey(t, "narpack-test-1")
	storePath, cache := publishOne(t, nix, secret)

	index, err := Index(cache)
	if err != nil {
		t.Fatal(err)
	}
	ni, ok := index[storePath]
	if !ok {
		t.Fatalf("%s was published but has no narinfo", storePath)
	}
	if !ni.Signed {
		t.Fatalf("the published narinfo for %s carries no signature; this is "+
			"the exact failure nix-store --export had", storePath)
	}

	// And it is still there after packing and unpacking, which is what
	// actually crosses the wire.
	var buf bytes.Buffer
	if err := Pack(cache, []string{storePath}, []string{storePath}, nil, &buf); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := Unpack(bytes.NewReader(buf.Bytes()), dir); err != nil {
		t.Fatal(err)
	}
	far, err := Index(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !far[storePath].Signed {
		t.Fatalf("the signature did not survive the transfer")
	}
}

// scratchImport imports into a store of its own under the given rules, and
// reports what nix said.
func scratchImport(t *testing.T, cacheTar []byte, roots []string, trusted string) error {
	t.Helper()
	dir := t.TempDir()
	if _, err := Unpack(bytes.NewReader(cacheTar), dir); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	old := importSeam
	importSeam = []string{
		"--store", store,
		"--option", "require-sigs", "true",
		"--option", "trusted-public-keys", trusted,
		// Nothing may be fetched from anywhere else: the point is to test
		// what THIS archive carries, and a substituter would quietly supply
		// a path the archive failed to.
		"--option", "substituters", "",
	}
	defer func() { importSeam = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return Import(ctx, dir, roots)
}

// TestAClosureSignedByATrustedKeyIsAccepted, with require-sigs ON.
//
// This is what could not be done before: receive peer work on a machine that
// still checks signatures for everything else it installs.
func TestAClosureSignedByATrustedKeyIsAccepted(t *testing.T) {
	nix := nixOrSkip(t)
	secret, public := signingKey(t, "narpack-test-trusted")
	storePath, cache := publishOne(t, nix, secret)

	var buf bytes.Buffer
	if err := Pack(cache, []string{storePath}, []string{storePath}, nil, &buf); err != nil {
		t.Fatal(err)
	}
	if err := scratchImport(t, buf.Bytes(), []string{storePath}, public); err != nil {
		t.Fatalf("a closure signed by a trusted key was refused: %v", err)
	}
}

// TestAClosureFromAnUntrustedSenderIsRefused is the half that proves the
// other half means anything.
func TestAClosureFromAnUntrustedSenderIsRefused(t *testing.T) {
	nix := nixOrSkip(t)
	secret, _ := signingKey(t, "narpack-test-stranger")
	storePath, cache := publishOne(t, nix, secret)

	// A different key entirely: the sender is not in this machine's cluster.
	_, otherPublic := signingKey(t, "narpack-test-somebody-else")

	var buf bytes.Buffer
	if err := Pack(cache, []string{storePath}, []string{storePath}, nil, &buf); err != nil {
		t.Fatal(err)
	}
	err := scratchImport(t, buf.Bytes(), []string{storePath}, otherPublic)
	if err == nil {
		t.Fatal("a closure signed by nobody this store trusts was IMPORTED; " +
			"the signature check is not doing anything")
	}
	if !strings.Contains(err.Error(), "signature") {
		t.Fatalf("refused, but not over the signature: %v", err)
	}
}
