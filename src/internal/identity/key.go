package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// A node's signing key.
//
// The node id on its own is a claim, not a proof: anything that accepts one
// accepts whoever types it. That is tolerable between machines on a private
// network which have already agreed a shared token, and it stops being
// tolerable the moment a public relay is introducing strangers - a peer there
// is identified by a name it chose, and nothing stops it choosing somebody
// else's.
//
// A key makes the identity self-authenticating. The address a peer is reached
// at is the fingerprint of its public key, so impersonating it requires the
// private half rather than the spelling.

// KeyPair is a node's ed25519 identity.
type KeyPair struct {
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
}

// Fingerprint is the short, stable name derived from a public key. Peers
// address each other by this, so it has to be the same everywhere and depend
// on nothing but the key.
func Fingerprint(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:8])
}

// Fingerprint of this node's own key.
func (k KeyPair) Fingerprint() string { return Fingerprint(k.Public) }

var keyOnce struct {
	sync.Once
	kp  KeyPair
	err error
}

// Key returns this node's signing key, creating it on first use.
//
// Generated rather than derived from the node id: a key that can be
// reconstructed from a public identifier authenticates nothing.
func Key() (KeyPair, error) {
	keyOnce.Do(func() { keyOnce.kp, keyOnce.err = loadOrCreateKey() })
	return keyOnce.kp, keyOnce.err
}

func keyPath() string { return filepath.Join(userdir.Data(), "node.key") }

func loadOrCreateKey() (KeyPair, error) {
	path := keyPath()
	if b, err := os.ReadFile(path); err == nil {
		raw, derr := base64.StdEncoding.DecodeString(string(trimSpace(b)))
		if derr == nil && len(raw) == ed25519.PrivateKeySize {
			priv := ed25519.PrivateKey(raw)
			return KeyPair{Public: priv.Public().(ed25519.PublicKey), Private: priv}, nil
		}
		// A corrupt key is not something to paper over by generating a new
		// one: every peer that has pinned this node would silently stop
		// recognising it.
		return KeyPair{}, fmt.Errorf("%s is not a usable node key; move it aside to "+
			"generate a new identity, knowing peers will see this node as a stranger", path)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return KeyPair{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return KeyPair{}, err
	}
	// 0600: a signing key readable by every user on the machine proves
	// nothing about which of them is speaking.
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(priv)+"\n"), 0o600); err != nil {
		return KeyPair{}, err
	}
	return KeyPair{Public: pub, Private: priv}, nil
}

func trimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\n' || b[j-1] == '\r' || b[j-1] == '\t') {
		j--
	}
	return b[i:j]
}

// Sign signs a message with this node's key, under a context string.
//
// The context is mixed in so a signature produced for one purpose cannot be
// replayed as another: a hello to a relay must not also be a valid
// authorisation to run a job.
func (k KeyPair) Sign(context string, msg []byte) []byte {
	return ed25519.Sign(k.Private, domain(context, msg))
}

// Verify checks a signature made by Sign.
func Verify(pub ed25519.PublicKey, context string, msg, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(pub, domain(context, msg), sig)
}

// domain binds a message to its purpose, with a length prefix so that no two
// (context, message) pairs can produce the same bytes to sign.
func domain(context string, msg []byte) []byte {
	out := make([]byte, 0, len(context)+len(msg)+9)
	out = append(out, "pipedpeer\x00"...)
	out = append(out, context...)
	out = append(out, 0)
	return append(out, msg...)
}

// EncodePublic renders a public key for transport.
func EncodePublic(pub ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(pub)
}

// DecodePublic parses a public key from transport.
func DecodePublic(s string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key is %d bytes, want %d", len(raw), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(raw), nil
}

// onceReset exists for tests: Key memoises deliberately, and a test that
// exercises a fresh identity has to clear it.
func onceReset() sync.Once { return sync.Once{} }
