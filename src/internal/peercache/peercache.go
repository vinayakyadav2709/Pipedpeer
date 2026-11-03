// Package peercache holds what the daemon's Nix binary cache and the nix
// build call sites share: store-path parsing, narinfo rendering and
// acceptance, and the per-command nix options that point nix at the local
// daemon's /nix-cache.
package peercache

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
)

// Route is where the daemon mounts the cache, below its API port.
const Route = "/nix-cache"

// CacheInfo is the nix-cache-info body. Priority 60 sits behind
// cache.nixos.org's 40, so peers are asked only for what it lacks.
const CacheInfo = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 60\n"

const nix32 = "0123456789abcdfghijklmnpqrsvwxyz"

// ValidHash reports whether h is a store-path hash: 32 nix base32 characters.
func ValidHash(h string) bool {
	if len(h) != 32 {
		return false
	}
	for i := 0; i < len(h); i++ {
		if strings.IndexByte(nix32, h[i]) < 0 {
			return false
		}
	}
	return true
}

// HashOf returns the hash part of a store path or basename ("" if none).
func HashOf(storePath string) string {
	b := path.Base(storePath)
	if len(b) < 32 {
		return ""
	}
	return b[:32]
}

// Accept reports why a peer's narinfo must not be passed on to nix; nil means
// acceptable. The hash must be the one asked for, and the path must be
// content-addressed or carry a signature that verifies against a trusted key.
// Nix checks everything again after the download.
func Accept(ni *narinfo.NarInfo, hash string, trusted []signature.PublicKey) error {
	if ni == nil {
		return fmt.Errorf("no narinfo")
	}
	if got := HashOf(ni.StorePath); got != hash {
		return fmt.Errorf("store path mismatch: asked %s, got %q", hash, ni.StorePath)
	}
	if err := ni.Check(); err != nil {
		return err
	}
	switch {
	case ni.NarSize == 0:
		return fmt.Errorf("NarSize is 0")
	case ni.NarHash == nil:
		return fmt.Errorf("no NarHash")
	case ni.URL == "":
		return fmt.Errorf("no URL")
	}
	if ni.CA != "" {
		return nil
	}
	if len(ni.Signatures) == 0 {
		return fmt.Errorf("no trusted signature: path is unsigned and not content-addressed")
	}
	if signature.VerifyFirst(ni.Fingerprint(), ni.Signatures, trusted) {
		return nil
	}
	names := make([]string, len(ni.Signatures))
	for i, s := range ni.Signatures {
		names[i] = s.Name
	}
	return fmt.Errorf("no trusted signature verifies (signed by %s)", strings.Join(names, ", "))
}

// Seams for tests.
var (
	keysFile  = nixsign.TrustedKeysFile
	nixConfig = nixsign.NixConfig
	now       = time.Now
)

var keyCache struct {
	mu   sync.Mutex
	at   time.Time
	keys []signature.PublicKey
}

const keyTTL = time.Minute

// TrustedKeys is the cluster's keys plus those of the machine's nix
// (cache.nixos.org's among them), re-read at most once a minute. Strings that
// are not keys are warned about and skipped.
func TrustedKeys() []signature.PublicKey {
	keyCache.mu.Lock()
	defer keyCache.mu.Unlock()
	if keyCache.keys != nil && now().Sub(keyCache.at) < keyTTL {
		return keyCache.keys
	}
	var raw []string
	file := keysFile()
	if body, err := os.ReadFile(file); err == nil {
		raw = append(raw, strings.Fields(string(body))...)
	} else if !os.IsNotExist(err) {
		log.Warn().Err(err).Str("file", file).Msg("peer cache: could not read the cluster's trusted keys")
	}
	if out, err := nixConfig("trusted-public-keys"); err == nil {
		raw = append(raw, strings.Fields(out)...)
	} else {
		log.Warn().Err(err).Msg("peer cache: could not read nix's trusted-public-keys")
	}
	keys := []signature.PublicKey{}
	seen := map[string]bool{}
	for _, k := range raw {
		if seen[k] {
			continue
		}
		seen[k] = true
		pk, err := signature.ParsePublicKey(k)
		if err != nil {
			log.Warn().Err(err).Str("key", k).Msg("peer cache: skipping a trusted key that does not parse")
			continue
		}
		keys = append(keys, pk)
	}
	keyCache.keys, keyCache.at = keys, now()
	return keys
}
