// Package nixsign lets nix decide for itself whether a peer's closure is
// trustworthy, instead of being told not to ask.
//
// A multi-user nix refuses to import a store path that no trusted key has
// signed - "lacks a signature by a trusted key" - and peer closures were
// unsigned, so a stock worker could not receive work at all: the upload
// succeeded and the import died. The workaround was `require-sigs = false`
// in the machine's nix.conf, which turns the check off for EVERYTHING on
// that machine, not only for pipedpeer. One project's convenience became a
// system-wide reduction in what the package manager will refuse.
//
// The intended answer was the one nix is built for: sign what we export and
// tell each machine which keys its cluster uses, so "accept anything
// unsigned" becomes "accept what a known cluster member signed".
//
// # Why that is not sufficient on its own, measured
//
// `nix-store --export` does not carry signatures. Signatures live in the
// store's own metadata, and the classic export format - which is what every
// transfer here uses - does not serialise them. So a path can be correctly
// signed on the sender, arrive at a receiver that trusts the signing key,
// and still be refused: verified on the two machines here, with all 26 paths
// in a closure signed by a trusted key and the import still failing with
// "lacks a signature by a trusted key".
//
// What this package therefore provides today is the key material and its
// distribution - each node's key, published, proved against the fingerprint
// the connection was authenticated under, and kept current as peers come and
// go. Those are the parts that are hard to get right and they are done. What
// remains is a transfer that preserves signatures: `nix copy` against a
// store URI rather than `nix-store --export`, which is a change to how
// closures move rather than to how they are signed.
//
// Until then a machine that requires signatures cannot import peer closures,
// and `require-sigs = false` remains the working configuration. Signing is
// still performed, so the moment the transfer carries signatures there is
// nothing else to switch on.
//
// # Where the key comes from
//
// Nix signing keys are ed25519, and so is the node identity key that peers
// already address each other by. So there is no second key to generate,
// store, rotate or lose: the signing key IS the node identity, in the
// encoding nix expects. A path signed by a node is therefore signed by the
// same identity that authenticated the connection it arrived on, and the two
// facts cannot drift apart.
package nixsign

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// KeyName is what a signature is labelled with in the store.
//
// The node's fingerprint, so a path's signature says which machine built it
// and that name matches the one used everywhere else. Nix requires the name
// to contain no colon, since colon separates name from key material.
func KeyName(k identity.KeyPair) string {
	return "pipedpeer-" + k.Fingerprint()[:16]
}

// PublicKey is this node's verifying key in nix's format: name:base64.
//
// Published to peers so they can add it to the keys they trust. It is derived
// from the identity key, so a peer that knows a node's fingerprint can check
// this is really that node's key rather than taking it on faith.
func PublicKey(k identity.KeyPair) string {
	return KeyName(k) + ":" + base64.StdEncoding.EncodeToString(k.Public)
}

// secretKey is the signing half, in the same format.
func secretKey(k identity.KeyPair) string {
	return KeyName(k) + ":" + base64.StdEncoding.EncodeToString(k.Private)
}

// PublicKeyFor derives the nix public key a node's fingerprint implies.
//
// The check that makes key distribution safe. A peer sends its public key
// with its node record, and a record can say anything; but the key must hash
// to the fingerprint the connection was authenticated under, and only the
// holder of the private half can produce a matching pair. So a peer cannot
// offer somebody else's key, nor a key it does not own.
func PublicKeyFor(pub ed25519.PublicKey) string {
	fp := identity.Fingerprint(pub)
	return "pipedpeer-" + fp[:16] + ":" + base64.StdEncoding.EncodeToString(pub)
}

// Verify reports whether an offered nix public key really belongs to the node
// with this fingerprint.
//
// Both halves are checked: that the key material hashes to the fingerprint,
// and that the label matches too - a key whose name says one node and whose
// bytes say another would be trusted under the wrong name, and the name is
// what appears in signatures.
func Verify(offered, fingerprint string) error {
	name, b64, ok := strings.Cut(offered, ":")
	if !ok {
		return fmt.Errorf("nix public key %q has no name:key form", offered)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return fmt.Errorf("nix public key %q: %w", name, err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return fmt.Errorf("nix public key %q is %d bytes, want %d", name, len(raw), ed25519.PublicKeySize)
	}
	got := identity.Fingerprint(ed25519.PublicKey(raw))
	if got != fingerprint {
		return fmt.Errorf("nix public key belongs to %s, not to %s", got[:8], fingerprint[:8])
	}
	if want := "pipedpeer-" + fingerprint[:16]; name != want {
		return fmt.Errorf("nix public key is labelled %q but its bytes belong to %q", name, want)
	}
	return nil
}

// keyFile writes the secret key where nix can read it, and returns a function
// that removes it.
//
// On disk because `nix store sign` takes a file rather than a value, and a
// secret on a command line is visible to every process on the machine. Mode
// 0600 in the user's own data directory, written fresh and removed after,
// so it exists for as long as one signing run takes.
func keyFile(k identity.KeyPair) (string, func(), error) {
	dir := userdir.Data()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, err
	}
	f, err := os.CreateTemp(dir, "nixsign-*.key")
	if err != nil {
		return "", nil, err
	}
	name := f.Name()
	cleanup := func() { _ = os.Remove(name) }
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		cleanup()
		return "", nil, err
	}
	if _, err := f.WriteString(secretKey(k)); err != nil {
		f.Close()
		cleanup()
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return name, cleanup, nil
}

// SignPaths signs store paths with this node's key.
//
// Signing is additive: a path may carry signatures from several keys, and
// signing one that is already signed adds ours rather than replacing
// anything. So this is safe to run over a closure that came from somewhere
// else, and safe to run twice.
func SignPaths(ctx context.Context, k identity.KeyPair, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	file, cleanup, err := keyFile(k)
	if err != nil {
		return fmt.Errorf("writing the signing key: %w", err)
	}
	defer cleanup()

	argv := append([]string{"nix", "store", "sign", "--key-file", file}, paths...)
	cmd, done, err := nixstore.Cmd("", argv...)
	if err != nil {
		return err
	}
	defer done()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nix store sign: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// TrustedKeysFile is where the cluster's public keys are kept for nix to read.
//
// In the user's own directory rather than /etc: a machine using a private
// store needs no root at all, and on a system store this file is what a
// one-time root action points nix at, rather than the alternative of
// switching signature checking off entirely.
func TrustedKeysFile() string {
	return filepath.Join(userdir.Data(), "trusted-public-keys")
}

// WriteTrustedKeys records the keys this node's cluster signs with.
//
// Rewritten whole each time rather than appended to, so a peer that leaves
// stops being trusted. Sorted and deduplicated so the file does not churn.
func WriteTrustedKeys(keys []string) error {
	seen := map[string]bool{}
	var out []string
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	sortStrings(out)

	dir := userdir.Data()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	body := strings.Join(out, " ") + "\n"
	// Written via a temporary file and renamed, so a reader never sees a
	// half-written list and conclude the cluster trusts nobody.
	tmp := TrustedKeysFile() + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, TrustedKeysFile())
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// EnsureSigned signs store paths with this node's key before they are sent.
//
// Called at every export site, and idempotent by nature: nix signatures are
// additive, so a path that already carries ours is unchanged and a path
// signed by whoever built it keeps that signature too. Cheap - nix already
// holds each path's hash, so this writes metadata rather than re-reading the
// store.
//
// A failure here is not fatal to the transfer. A peer whose nix predates
// `nix store sign`, or a store that will not accept a signature, should still
// be able to send work to a peer that does not require one; the receiving end
// decides what it will accept, and that is the correct place for the decision.
// The error is returned so callers can say so, not so they can abort.
func EnsureSigned(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	k, err := identity.Key()
	if err != nil {
		return fmt.Errorf("no node key to sign with: %w", err)
	}
	return SignPaths(ctx, k, paths)
}
