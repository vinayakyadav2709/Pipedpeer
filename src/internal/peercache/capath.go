package peercache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/nix-community/go-nix/pkg/nixbase32"
	"github.com/nix-community/go-nix/pkg/storepath"
)

// storeDir is the only store this cache serves (see CacheInfo).
const storeDir = "/nix/store"

// CAStorePath recomputes the store path a content-addressed narinfo must have
// (nix's makeFixedOutputPath) from its CA field, name and references. It
// supports fixed:[r:]<algo>:<hash> with sha1, sha256 and sha512; any other
// kind is an error, and the caller then falls back to requiring a signature.
// storePath is the narinfo's StorePath, used for the name and the self
// reference; refs are the narinfo's References (base names).
func CAStorePath(ca, storePath string, refs []string) (string, error) {
	sp, err := storepath.FromAbsolutePath(storePath)
	if err != nil || path.Dir(storePath) != storeDir {
		return "", fmt.Errorf("not a store path: %q", storePath)
	}
	parts := strings.Split(ca, ":")
	if len(parts) < 3 || parts[0] != "fixed" {
		return "", fmt.Errorf("unsupported CA %q", ca)
	}
	recursive := parts[1] == "r"
	if recursive {
		parts = append(parts[:1], parts[2:]...)
	}
	if len(parts) != 3 {
		return "", fmt.Errorf("unsupported CA %q", ca)
	}
	algo, enc := parts[1], parts[2]
	size := map[string]int{"sha1": 20, "sha256": 32, "sha512": 64}[algo]
	if size == 0 {
		return "", fmt.Errorf("unsupported CA hash %q", algo)
	}
	raw, err := nixbase32.DecodeString(enc)
	if err != nil || len(raw) != size {
		return "", fmt.Errorf("bad CA hash %q", enc)
	}
	hexHash := hex.EncodeToString(raw)

	var fp string
	if recursive && algo == "sha256" {
		typ := "source"
		var others []string
		self := false
		for _, r := range refs {
			if r == sp.String() {
				self = true
			} else {
				others = append(others, storeDir+"/"+r)
			}
		}
		sort.Strings(others)
		for _, r := range others {
			typ += ":" + r
		}
		if self {
			typ += ":self"
		}
		fp = typ + ":sha256:" + hexHash + ":" + storeDir + ":" + sp.Name
	} else {
		if len(refs) > 0 {
			return "", fmt.Errorf("CA %q path with references", ca)
		}
		method := ""
		if recursive {
			method = "r:"
		}
		inner := sha256.Sum256([]byte("fixed:out:" + method + algo + ":" + hexHash + ":"))
		fp = "output:out:sha256:" + hex.EncodeToString(inner[:]) + ":" + storeDir + ":" + sp.Name
	}
	sum := sha256.Sum256([]byte(fp))
	var comp [20]byte
	for i, b := range sum {
		comp[i%20] ^= b
	}
	return storeDir + "/" + nixbase32.EncodeToString(comp[:]) + "-" + sp.Name, nil
}
