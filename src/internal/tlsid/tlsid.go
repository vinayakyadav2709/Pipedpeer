// Package tlsid gives each daemon a TLS identity and lets peers pin it.
//
// The token proves who may talk to a daemon; it does nothing about anyone
// listening. Over a trusted LAN that was a defensible gap, but the token
// itself travels in the request, so on any network the operator does not own
// — which is the whole point of the internet work — a passive observer
// collects it and becomes a member.
//
// There is no CA here and there should not be: a certificate authority is
// infrastructure to run, and the thing being authenticated is a machine the
// operator already identified by giving it the token.
//
// The certificate is made from the node key (identity.Key, ed25519), so the
// key a peer presents in the handshake is the node id it is known by:
// node id = identity.Fingerprint(cert public key). Pins are therefore by node
// id, not by certificate bytes: an address the address book expects to be a
// given node must present that node's key (Expect), and any other address is
// accepted and recorded by key, with the token proof (ADR-0007) still gating
// the cluster secret. A different machine on a reused IP is no longer an
// error; a different key where a known node was expected still is.
package tlsid

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

func dir() string { return filepath.Join(userdir.State(), "tls") }

// EnsureCert returns this daemon's certificate, made from the node key.
//
// Nothing is written: the certificate is a pure function of the key (fixed
// serial, validity and names, and ed25519 signatures are deterministic), so
// every call returns the same bytes. No addresses go in it: peers decide
// trust by the key, and an address list would go stale with every network
// change.
func EnsureCert() (tls.Certificate, error) {
	kp, err := identity.Key()
	if err != nil {
		return tls.Certificate{}, err
	}
	dropOldFiles.Do(removeOldFiles)
	return certFor(kp)
}

func certFor(kp identity.KeyPair) (tls.Certificate, error) {
	sum := sha256.Sum256(kp.Public)
	tmpl := x509.Certificate{
		SerialNumber: new(big.Int).SetBytes(sum[:16]),
		Subject:      pkix.Name{CommonName: "pipedpeer-node " + kp.Fingerprint()},
		NotBefore:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		// Far future: expiry protects nobody here, and a daemon that stops
		// answering because its certificate aged out is a failure with no
		// corresponding attack.
		NotAfter:              time.Date(2124, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"pipedpeer"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, kp.Public, kp.Private)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("make node certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: kp.Private, Leaf: leaf}, nil
}

var dropOldFiles sync.Once

// removeOldFiles deletes what older builds wrote: a P-256 certificate and
// key unrelated to the node key, and pins of certificate bytes that no longer
// mean anything.
func removeOldFiles() {
	for _, name := range []string{"daemon.crt", "daemon.key", "known_peers.json"} {
		p := filepath.Join(dir(), name)
		if err := os.Remove(p); err == nil {
			log.Info().Str("path", p).Msg("removed a file from the old TLS identity")
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Warn().Err(err).Str("path", p).Msg("could not remove a file from the old TLS identity")
		}
	}
}

// NodeOf returns the node id a certificate proves: the fingerprint of its
// ed25519 key. Certificates from older builds (P-256) are refused.
func NodeOf(der []byte) (string, error) {
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return "", err
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", fmt.Errorf("not a pipedpeer node key (%s); the peer runs an older pipedpeer build",
			cert.PublicKeyAlgorithm)
	}
	return identity.Fingerprint(pub), nil
}

// Fingerprint is the SHA-256 of a certificate's DER bytes, which is what
// peers pin.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

// --- pinning ---

// pin is what is known about one address.
type pin struct {
	Node string `json:"node"`
	// Expected: the address book said this address is Node, so another key
	// there is refused. Otherwise Node is just who answered last.
	Expected bool `json:"expected,omitempty"`
}

type pinStore struct {
	mu   sync.Mutex
	pins map[string]pin // "host:port" -> node
	// mod and size are the state of the file as last read, so a change made
	// by another process is noticed.
	mod  time.Time
	size int64
	read bool
}

var pins = &pinStore{pins: map[string]pin{}}

func pinPath() string { return filepath.Join(dir(), "pins.json") }

// load reads the pin file, and re-reads it whenever it has changed on disk,
// so `pipedpeer auth forget` in another process takes effect in a running
// daemon without a restart.
func (p *pinStore) load() {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, err := os.Stat(pinPath())
	if err != nil {
		// No file: nothing pinned yet, or the store was removed wholesale.
		if p.read && len(p.pins) > 0 {
			p.pins = map[string]pin{}
		}
		return
	}
	if p.read && st.ModTime().Equal(p.mod) && st.Size() == p.size {
		return
	}
	b, err := os.ReadFile(pinPath())
	if err != nil {
		log.Warn().Err(err).Str("path", pinPath()).Msg("could not read TLS pins; keeping the ones in memory")
		return
	}
	fresh := map[string]pin{}
	if err := json.Unmarshal(b, &fresh); err != nil {
		// Keep what we have rather than trusting a half-written file.
		log.Warn().Err(err).Str("path", pinPath()).Msg("TLS pin file unreadable; keeping the ones in memory")
		return
	}
	p.pins, p.mod, p.size, p.read = fresh, st.ModTime(), st.Size(), true
}

func (p *pinStore) save() {
	b, err := json.Marshal(p.pins)
	if err == nil {
		err = os.MkdirAll(dir(), 0o700)
	}
	if err == nil {
		err = os.WriteFile(pinPath(), b, 0o600)
	}
	if err != nil {
		log.Warn().Err(err).Str("path", pinPath()).Msg("could not save TLS pins")
		return
	}
	if st, err := os.Stat(pinPath()); err == nil {
		p.mod, p.size, p.read = st.ModTime(), st.Size(), true
	}
}

// Expect records that addr is nodeID: from now on a certificate at addr
// must carry that node's key. The address book calls it whenever it verifies
// a path. It replaces whatever was known about addr before.
func Expect(addr, nodeID string) {
	pins.load()
	pins.mu.Lock()
	defer pins.mu.Unlock()
	if pins.pins[addr] == (pin{Node: nodeID, Expected: true}) {
		return
	}
	pins.pins[addr] = pin{Node: nodeID, Expected: true}
	pins.save()
}

// CheckOrPin checks a peer's certificate at addr. The certificate must hold
// an ed25519 node key. Where addr is expected to be a node (Expect), the key
// must be that node's: a different one is a reinstalled machine or someone
// in the middle, and only the operator can tell those apart. Any other
// address is accepted and recorded by key.
func CheckOrPin(addr string, der []byte) error {
	node, err := NodeOf(der)
	if err != nil {
		return fmt.Errorf("certificate for %s refused: %w", addr, err)
	}
	pins.load()

	pins.mu.Lock()
	defer pins.mu.Unlock()
	known, ok := pins.pins[addr]
	if ok && known.Node == node {
		return nil
	}
	if ok && known.Expected {
		return fmt.Errorf("certificate for %s is node %s, expected node %s; "+
			"if that machine was reinstalled, run `pipedpeer auth forget %s`",
			addr, node, known.Node, addr)
	}
	if ok {
		log.Info().Str("addr", addr).Str("was", known.Node).Str("now", node).
			Msg("a different machine answers at this address")
	}
	pins.pins[addr] = pin{Node: node}
	pins.save()
	return nil
}

// Forget drops what is known about an address, for when a machine really was
// reinstalled or the address book let go of a path.
func Forget(addr string) {
	pins.load()
	pins.mu.Lock()
	defer pins.mu.Unlock()
	delete(pins.pins, addr)
	pins.save()
}

// ClientConfig verifies peers by node key rather than by CA chain.
//
// InsecureSkipVerify disables the chain check, not the verification: these
// certificates are self-signed, so a chain check can only ever fail, while
// the key answers the question that actually matters — which node is this.
func ClientConfig(addr string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("peer presented no certificate")
			}
			return CheckOrPin(addr, rawCerts[0])
		},
		MinVersion: tls.VersionTLS12,
	}
}

// ForgetAll drops every pin, and reports how many. Useful where daemons are
// genuinely disposable - a container lab recreates them on every run, so
// their keys change every time and the refusal is expected rather than
// suspicious.
func ForgetAll() int {
	pins.load()
	pins.mu.Lock()
	defer pins.mu.Unlock()
	n := len(pins.pins)
	pins.pins = map[string]pin{}
	pins.save()
	return n
}
