package peercache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/nix-community/go-nix/pkg/nixhash"
	"github.com/rs/zerolog/log"
)

// PathInfo is one store path as `nix path-info --json --sigs` describes it.
type PathInfo struct {
	Path       string // full store path
	NarHash    string // "sha256:<nix32>"
	NarSize    uint64
	References []string // full store paths
	Deriver    string   // full path or ""
	Sigs       []string
	CA         string // "fixed:r:sha256:<nix32>" form or ""
}

type rawInfo struct {
	Path       string          `json:"path"` // nix <= 2.18 array form
	CA         json.RawMessage `json:"ca"`
	Deriver    *string         `json:"deriver"`
	NarHash    string          `json:"narHash"`
	NarSize    uint64          `json:"narSize"`
	References []string        `json:"references"`
	Signatures []string        `json:"signatures"`
}

// ParsePathInfo reads the stdout of `nix path-info --json --sigs` in any of
// the shapes nix has printed: an array (<= 2.18), an object keyed by full
// path, or format 2's {"info": {...}, "storeDir": ...}. An entry it cannot
// use is warned about and skipped, so that path is a miss rather than a
// failure.
func ParsePathInfo(stdout []byte) ([]PathInfo, error) {
	body := bytes.TrimSpace(stdout)
	if len(body) == 0 {
		return nil, fmt.Errorf("path-info: empty output")
	}
	type entry struct {
		key string
		raw rawInfo
	}
	var entries []entry
	storeDir := ""
	if body[0] == '[' {
		var arr []rawInfo
		if err := json.Unmarshal(body, &arr); err != nil {
			return nil, fmt.Errorf("path-info: %w", err)
		}
		for _, r := range arr {
			entries = append(entries, entry{r.Path, r})
		}
	} else {
		var top map[string]json.RawMessage
		if err := json.Unmarshal(body, &top); err != nil {
			return nil, fmt.Errorf("path-info: %w", err)
		}
		m := top
		if info, ok := top["info"]; ok {
			m = nil
			if err := json.Unmarshal(info, &m); err != nil {
				return nil, fmt.Errorf("path-info: info: %w", err)
			}
			storeDir = "/nix/store"
			if sd, ok := top["storeDir"]; ok {
				_ = json.Unmarshal(sd, &storeDir)
			}
		}
		for k, v := range m {
			var r rawInfo
			if err := json.Unmarshal(v, &r); err != nil {
				log.Warn().Err(err).Str("path", k).Msg("peer cache: skipping a path-info entry that does not parse")
				continue
			}
			entries = append(entries, entry{k, r})
		}
	}
	var out []PathInfo
	for _, e := range entries {
		pi, err := convert(e.key, storeDir, e.raw)
		if err != nil {
			log.Warn().Err(err).Str("path", e.key).Msg("peer cache: skipping a path that cannot be served")
			continue
		}
		out = append(out, pi)
	}
	return out, nil
}

func full(storeDir, p string) string {
	if storeDir == "" || strings.HasPrefix(p, "/") || p == "" {
		return p
	}
	return storeDir + "/" + p
}

func convert(key, storeDir string, r rawInfo) (PathInfo, error) {
	pi := PathInfo{Path: full(storeDir, key), NarSize: r.NarSize, Sigs: r.Signatures}
	if pi.Path == "" || HashOf(pi.Path) == "" {
		return pi, fmt.Errorf("no store path")
	}
	if r.NarSize == 0 {
		return pi, fmt.Errorf("narSize is 0 or missing")
	}
	h, err := nixhash.ParseAny(r.NarHash, nil)
	if err != nil {
		return pi, fmt.Errorf("narHash %q: %w", r.NarHash, err)
	}
	pi.NarHash = h.Format(nixhash.NixBase32, true)
	for _, ref := range r.References {
		pi.References = append(pi.References, full(storeDir, ref))
	}
	if r.Deriver != nil {
		pi.Deriver = full(storeDir, *r.Deriver)
	}
	if pi.CA, err = parseCA(r.CA); err != nil {
		return pi, err
	}
	return pi, nil
}

// parseCA turns format 1's string and format 2's {hash, method} object into
// the string narinfo uses.
func parseCA(raw json.RawMessage) (string, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return "", nil
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", err
		}
		return str, nil
	}
	var o struct {
		Hash   string `json:"hash"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return "", fmt.Errorf("ca: %w", err)
	}
	h, err := nixhash.ParseAny(o.Hash, nil)
	if err != nil {
		return "", fmt.Errorf("ca hash %q: %w", o.Hash, err)
	}
	hs := h.Format(nixhash.NixBase32, true) // "<algo>:<nix32>"
	switch o.Method {
	case "nar":
		return "fixed:r:" + hs, nil
	case "flat":
		return "fixed:" + hs, nil
	case "text":
		return "text:" + hs, nil
	}
	return "", fmt.Errorf("ca method %q is not served", o.Method)
}

// Narinfo renders pi the way the daemon serves it: the NAR is compressed
// while streaming, so URL is nar/<hash>.nar.zst and there is no FileHash.
func Narinfo(pi PathInfo) (*narinfo.NarInfo, error) {
	h, err := nixhash.ParseAny(pi.NarHash, nil)
	if err != nil {
		return nil, fmt.Errorf("narHash %q: %w", pi.NarHash, err)
	}
	hash := HashOf(pi.Path)
	if !ValidHash(hash) {
		return nil, fmt.Errorf("store path %q has no valid hash", pi.Path)
	}
	ni := &narinfo.NarInfo{
		StorePath:   pi.Path,
		URL:         "nar/" + hash + ".nar.zst",
		Compression: "zstd",
		NarHash:     h,
		NarSize:     pi.NarSize,
		CA:          pi.CA,
	}
	for _, r := range pi.References {
		ni.References = append(ni.References, path.Base(r))
	}
	if pi.Deriver != "" {
		ni.Deriver = path.Base(pi.Deriver)
	}
	for _, s := range pi.Sigs {
		sig, err := signature.ParseSignature(s)
		if err != nil {
			log.Warn().Err(err).Str("path", pi.Path).Msg("peer cache: dropping a signature that does not parse")
			continue
		}
		ni.Signatures = append(ni.Signatures, sig)
	}
	return ni, nil
}
