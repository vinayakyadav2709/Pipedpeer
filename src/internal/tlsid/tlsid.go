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
// operator already identified by giving it the token. So each daemon makes
// its own certificate, and a peer pins the fingerprint the first time it
// connects — trust on first use, with the token making that first contact
// meaningful rather than blind. A later swap of the certificate is then
// visible instead of silent.
//
// This is not the end state. Binding the certificate to a per-node keypair
// removes the first-contact window entirely (plan.md P3); the pinning
// machinery here is what that will slot into.
package tlsid

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

func dir() string { return filepath.Join(userdir.State(), "tls") }

// EnsureCert returns this daemon's certificate, generating one on first use.
//
// Ten years, because expiry here protects nobody: the certificate identifies
// a machine the operator vouched for with a token, and a daemon that stops
// answering because a self-signed certificate aged out is a failure with no
// corresponding attack.
func EnsureCert() (tls.Certificate, error) {
	certPath := filepath.Join(dir(), "daemon.crt")
	keyPath := filepath.Join(dir(), "daemon.key")

	if c, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		return c, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "pipedpeer-daemon"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		// Every address this machine has, so a peer dialling any of them
		// gets a certificate that at least matches what it typed. The
		// fingerprint is what actually decides trust.
		IPAddresses: localIPs(),
		DNSNames:    []string{"localhost", hostname()},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return tls.Certificate{}, err
	}
	// 0600: the private key is the identity.
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// Fingerprint is the SHA-256 of a certificate's DER bytes, which is what
// peers pin.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "pipedpeer"
	}
	return h
}

func localIPs() []net.IP {
	var out []net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return []net.IP{net.ParseIP("127.0.0.1")}
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP != nil {
			out = append(out, ipNet.IP)
		}
	}
	return out
}

// --- pinning ---

type pinStore struct {
	mu   sync.Mutex
	pins map[string]string // "host:port" -> fingerprint
	// mod and size are the state of the file as last read, so a change made
	// by another process is noticed.
	mod  time.Time
	size int64
	read bool
}

var pins = &pinStore{pins: map[string]string{}}

func pinPath() string { return filepath.Join(dir(), "known_peers.json") }

// load reads the pin file, and re-reads it whenever it has changed on disk.
//
// It used to load once and never again, which made the advice in its own
// error message wrong: a peer whose certificate changed tells the operator to
// run `pipedpeer auth forget <peer>`, that command rewrites this file, and the
// running daemon went on refusing the peer from a copy in memory until it was
// restarted. Nothing said so.
func (p *pinStore) load() {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, err := os.Stat(pinPath())
	if err != nil {
		// No file: nothing pinned yet, or the store was removed wholesale.
		if p.read && len(p.pins) > 0 {
			p.pins = map[string]string{}
		}
		return
	}
	if p.read && st.ModTime().Equal(p.mod) && st.Size() == p.size {
		return
	}
	b, err := os.ReadFile(pinPath())
	if err != nil {
		return
	}
	fresh := map[string]string{}
	if json.Unmarshal(b, &fresh) != nil {
		return // keep what we have rather than trusting a half-written file
	}
	p.pins, p.mod, p.size, p.read = fresh, st.ModTime(), st.Size(), true
}

func (p *pinStore) save() {
	b, err := json.Marshal(p.pins)
	if err != nil {
		return
	}
	_ = os.MkdirAll(dir(), 0o700)
	_ = os.WriteFile(pinPath(), b, 0o600)
	if st, err := os.Stat(pinPath()); err == nil {
		p.mod, p.size, p.read = st.ModTime(), st.Size(), true
	}
}

// CheckOrPin accepts a peer's certificate on first contact and requires the
// same one afterwards. Returns an error when a known peer's certificate has
// changed, which is either a reinstalled daemon or someone in the middle —
// and the operator is the only one who can tell those apart.
func CheckOrPin(addr string, der []byte) error {
	pins.load()
	fp := Fingerprint(der)

	pins.mu.Lock()
	defer pins.mu.Unlock()
	known, ok := pins.pins[addr]
	if !ok {
		pins.pins[addr] = fp
		pins.save()
		return nil
	}
	if known != fp {
		return fmt.Errorf("certificate for %s changed (pinned %s..., got %s...); "+
			"if this daemon was reinstalled, run `pipedpeer auth forget %s`",
			addr, known[:12], fp[:12], addr)
	}
	return nil
}

// Forget drops a pin, for when a daemon really was reinstalled.
func Forget(addr string) {
	pins.load()
	pins.mu.Lock()
	defer pins.mu.Unlock()
	delete(pins.pins, addr)
	pins.save()
}

// ClientConfig verifies peers by pinned fingerprint rather than by CA chain.
//
// InsecureSkipVerify disables the chain check, not the verification: these
// certificates are self-signed, so a chain check can only ever fail, while
// the fingerprint answers the question that actually matters — is this the
// same daemon I talked to before.
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
// their certificates change every time and the refusal is expected rather
// than suspicious.
func ForgetAll() int {
	pins.load()
	pins.mu.Lock()
	defer pins.mu.Unlock()
	n := len(pins.pins)
	pins.pins = map[string]string{}
	pins.save()
	return n
}
