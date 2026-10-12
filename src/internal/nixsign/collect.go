package nixsign

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/identity"
)

// Collect turns a cluster's published keys into the list this node should
// trust, discarding any that cannot prove they belong to the node offering
// them.
//
// Keys travel in node records and a record can claim anything, so each one is
// checked against the fingerprint it is published under: the material must
// hash to that fingerprint, which only its owner can arrange. A peer
// therefore cannot get another node's key trusted, nor one it does not hold
// the private half of.
//
// This node's own key is always included. A closure it built and signed is
// one it must be able to re-import - after a store garbage collection, for
// instance - and excluding it would make a machine refuse its own work.
func Collect(self identity.KeyPair, peers map[string]string) ([]string, []string) {
	trusted := []string{PublicKey(self)}
	var rejected []string

	for fingerprint, offered := range peers {
		offered = strings.TrimSpace(offered)
		if offered == "" {
			continue
		}
		if err := Verify(offered, fingerprint); err != nil {
			rejected = append(rejected, short(fingerprint)+": "+err.Error())
			continue
		}
		trusted = append(trusted, offered)
	}
	sortStrings(rejected)
	return trusted, rejected
}

// FingerprintOf reads the node fingerprint out of a published key.
//
// Used where a peer's key is known but its fingerprint is not to hand - the
// key carries the material, and the fingerprint is a function of it, so there
// is nothing to look up and nothing that can disagree.
func FingerprintOf(pubkey string) (string, bool) {
	_, b64, ok := strings.Cut(pubkey, ":")
	if !ok {
		return "", false
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return "", false
	}
	return identity.Fingerprint(ed25519.PublicKey(raw)), true
}

func short(fp string) string {
	if len(fp) > 8 {
		return fp[:8]
	}
	return fp
}
