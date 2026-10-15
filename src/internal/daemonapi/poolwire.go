package daemonapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Frames wire format. A frames body is a small JSON header line followed by
// length-prefixed raw pickle frames:
//
//	{header json, "items_frames": N}\n[globals frame][item frame]...
//
// frame = [4-byte big-endian length][raw bytes]. The header carries only small
// metadata (function source, flags, counts); all bulk data travels as frames,
// so big items never go through base64 or a giant JSON parse. Both requests
// and responses use it; legacy base64-in-JSON bodies are still accepted and
// emitted when the submitter did not opt in (small payloads keep the old path).
func putFrame(buf []byte, payload []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	return append(buf, payload...)
}

// readFrame pulls one length-prefixed frame off src.
func readFrame(src []byte) ([]byte, []byte, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("truncated frame header")
	}
	n := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if len(src) < n {
		return nil, nil, fmt.Errorf("truncated frame payload (need %d, have %d)", n, len(src))
	}
	return src[:n], src[n:], nil
}

// parseFrames reads the globals frame (when present) and itemCount item frames.
func parseFrames(body []byte, hasGlobals bool, itemCount int) ([]byte, []json.RawMessage, error) {
	rest := body
	var globals []byte
	var err error
	if hasGlobals {
		if globals, rest, err = readFrame(rest); err != nil {
			return nil, nil, err
		}
	}
	items := make([]json.RawMessage, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		var f []byte
		if f, rest, err = readFrame(rest); err != nil {
			return nil, nil, err
		}
		items = append(items, f)
	}
	return globals, items, nil
}

// buildFrames assembles a frames body from a header line and optional globals.
func buildFrames(header []byte, globals []byte, items []json.RawMessage) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, header...)
	buf = append(buf, '\n')
	if globals != nil {
		buf = putFrame(buf, globals)
	}
	for _, it := range items {
		buf = putFrame(buf, it)
	}
	return buf
}

func chunkDirFor(storePath string) string {
	sum := sha256.Sum256([]byte(storePath))
	// Parsed chunks are worth keeping across a warm-worker respawn and can be
	// large; a tmpfs charges RAM for them and drops them on reboot.
	return filepath.Join(userdir.Cache(), "chunkcache", hex.EncodeToString(sum[:8]))
}

// poolFramesHeader is the first line of a frames reply.
func poolFramesHeader(n int, node string, parts []provPart) []byte {
	recJSON, _ := json.Marshal(parts) // []provPart always marshals
	return fmt.Appendf(nil, "{\"results_frames\": %d, \"receipt\": {\"node\": %q, \"parts\": %s}}\n",
		n, node, recJSON)
}

const framesContentType = "application/vnd.pipedpeer.frames"

// resultPickle extracts the raw pickle bytes a result carries. Results are
// standardised on raw pickle bytes internally; only the wire edges base64.
func resultPickle(r any) []byte {
	m, ok := r.(map[string]string)
	if !ok {
		return nil
	}
	return []byte(m["pickle"])
}

// --- The peer link (C21): U27 implements, U18 calls from pool.go.
//
// Three things happen to a frames body on its way to a peer, all here so the
// shim never pays for them on the loopback hop:
//   - a globals frame of 1 MiB or more crosses once per peer: later bodies
//     carry only its sha256 and the peer reads it from its own cache;
//   - the whole body becomes one zstd stream when a sample says it shrinks
//     and the link is slow enough for that to pay (Dask's sample test, R9 a1);
//   - the bytes/s every real transfer achieved feed a per-peer EWMA, which
//     replaces the shim's 64 MB probe (R9 a4).
// Everything is additive on the wire (ADR-0006): an older peer never sees a
// hash-only body or a zstd one unless it was sent by a build that knows it.

const (
	// globalsByHashMin is joblib's memmap threshold; below it the frame costs
	// less than the extra round trip a cache miss would.
	globalsByHashMin = 1 << 20
	// compressMinBody: smaller bodies are latency-bound; Dask skips < 10 kB.
	compressMinBody = 64 << 10
	trialSamples    = 5
	trialSampleSize = 16 << 10
	// globalsCacheMax bounds the peer-side globals cache on disk.
	globalsCacheMax = 2 << 30
	// heldPerPeer bounds how many hashes the origin remembers per peer.
	heldPerPeer = 64
	// linkFresh is how long a link sample stays believable: Wi-Fi moves.
	linkFresh = 15 * time.Minute
	linkEWMA  = 0.3
	// linkMinSample: smaller transfers measure latency, not bandwidth.
	linkMinSample = 256 << 10
)

type peerLink struct {
	bps  float64
	at   time.Time
	held map[string]time.Time // globals sha256 -> when sent
}

var links = struct {
	sync.Mutex
	m map[string]*peerLink
}{m: map[string]*peerLink{}}

func linkFor(peerKey string) *peerLink {
	l := links.m[peerKey]
	if l == nil {
		l = &peerLink{held: map[string]time.Time{}}
		links.m[peerKey] = l
	}
	return l
}

// encodePeerBody prepares a frames body for the link to one peer: it drops a
// globals frame the peer already holds (sending its globals_sha256 instead)
// and zstd-compresses the result when that pays on this link. Set
// Content-Encoding to the returned value when it is non-empty. Bodies that are
// not frames pass through untouched.
func encodePeerBody(peerKey string, body []byte) (out []byte, contentEncoding string) {
	body = globalsByHash(peerKey, body)
	bps, known := linkSpeed(peerKey)
	lvl, ok, _ := compressChoice(body, bps, known)
	if !ok {
		return body, ""
	}
	return encoderFor(lvl).EncodeAll(body, make([]byte, 0, len(body)/2)), "zstd"
}

// splitFramesHeader returns the parsed header line and the frames after it,
// or ok=false for a legacy (non-frames) body.
func splitFramesHeader(body []byte) (hdr map[string]json.RawMessage, rest []byte, ok bool) {
	nl := bytes.IndexByte(body, '\n')
	if nl <= 0 {
		return nil, nil, false
	}
	if err := json.Unmarshal(body[:nl], &hdr); err != nil {
		return nil, nil, false
	}
	var n int
	if json.Unmarshal(hdr["items_frames"], &n) != nil || n <= 0 {
		return nil, nil, false
	}
	return hdr, body[nl+1:], true
}

func hdrBool(hdr map[string]json.RawMessage, k string) bool {
	var b bool
	_ = json.Unmarshal(hdr[k], &b) // absent or not a bool reads as false
	return b
}

// joinFrames rebuilds a body from a header and the frames that follow it.
func joinFrames(hdr map[string]json.RawMessage, globals, rest []byte) []byte {
	h, _ := json.Marshal(hdr) // map of RawMessage that came from json always marshals
	n := len(h) + 1 + len(rest)
	if globals != nil {
		n += 4 + len(globals)
	}
	buf := make([]byte, 0, n)
	buf = append(buf, h...)
	buf = append(buf, '\n')
	if globals != nil {
		buf = putFrame(buf, globals)
	}
	return append(buf, rest...)
}

func globalsByHash(peerKey string, body []byte) []byte {
	hdr, rest, ok := splitFramesHeader(body)
	if !ok || !hdrBool(hdr, "globals") {
		return body
	}
	g, after, err := readFrame(rest)
	if err != nil || len(g) < globalsByHashMin {
		return body
	}
	sum := sha256.Sum256(g)
	sha := hex.EncodeToString(sum[:])
	hdr["globals_sha256"], _ = json.Marshal(sha)
	links.Lock()
	l := linkFor(peerKey)
	_, held := l.held[sha]
	if !held {
		if len(l.held) >= heldPerPeer {
			evictOldest(l.held)
		}
	}
	// Marked before the send: a send that fails leaves the peer without it,
	// and that peer's 409 brings it back through forgetPeerGlobals.
	l.held[sha] = time.Now()
	links.Unlock()
	if held {
		hdr["globals"] = json.RawMessage("false")
		return joinFrames(hdr, nil, after)
	}
	return joinFrames(hdr, g, after)
}

func evictOldest(m map[string]time.Time) {
	var oldK string
	var oldT time.Time
	for k, t := range m {
		if oldK == "" || t.Before(oldT) {
			oldK, oldT = k, t
		}
	}
	delete(m, oldK)
}

// forgetPeerGlobals is called when a peer answers that it lacks a globals
// blob it was assumed to hold, before the one resend.
func forgetPeerGlobals(peerKey, sha string) {
	links.Lock()
	defer links.Unlock()
	if l := links.m[peerKey]; l != nil {
		delete(l.held, sha)
	}
	log.Printf("[pool] peer %s lacked globals %.12s; resending them once", peerKey, sha)
}

// missingGlobalsError is decodePoolBody's answer to a hash-only body whose
// globals this node does not hold. The handler replies 409
// {"missing_globals": sha} (poolDecodeStatus) and the origin resends.
type missingGlobalsError struct{ Sha string }

func (e *missingGlobalsError) Error() string {
	return "globals " + e.Sha + " not in this node's cache"
}

// poolDecodeStatus maps a decodePoolBody error to the reply handlePoolMap
// writes: 409 {"missing_globals": sha} on a cache miss, else 400.
func poolDecodeStatus(err error) (int, map[string]string) {
	var m *missingGlobalsError
	if errors.As(err, &m) {
		return http.StatusConflict, map[string]string{"missing_globals": m.Sha}
	}
	return http.StatusBadRequest, map[string]string{"error": "bad pool body: " + err.Error()}
}

// missingGlobalsIn reads a peer's reply: ok when it is the 409 asking for a
// globals blob, so the origin calls forgetPeerGlobals and resends once.
func missingGlobalsIn(status int, body []byte) (sha string, ok bool) {
	if status != http.StatusConflict {
		return "", false
	}
	var r struct {
		Sha string `json:"missing_globals"`
	}
	if json.Unmarshal(body, &r) != nil || r.Sha == "" {
		return "", false
	}
	return r.Sha, true
}

// decodePoolBody undoes encodePeerBody on the receiving daemon, right after
// handlePoolMap reads the body: it unpacks zstd and puts back a globals frame
// sent by hash (or stores one sent in full, for the next chunk). A hash it
// cannot resolve is a *missingGlobalsError.
func decodePoolBody(contentEncoding string, raw []byte) ([]byte, error) {
	body := raw
	switch contentEncoding {
	case "", "identity":
	case "zstd":
		var err error
		if body, err = zstdDecoder().DecodeAll(raw, nil); err != nil {
			return nil, fmt.Errorf("zstd body: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", contentEncoding)
	}
	hdr, rest, ok := splitFramesHeader(body)
	if !ok {
		return body, nil
	}
	var sha string
	if json.Unmarshal(hdr["globals_sha256"], &sha) != nil || sha == "" {
		return body, nil
	}
	if !validSha256(sha) {
		return nil, fmt.Errorf("bad globals_sha256 %q", sha)
	}
	path := filepath.Join(globalsCacheDir(), sha)
	if hdrBool(hdr, "globals") {
		g, _, err := readFrame(rest)
		if err != nil {
			return nil, err
		}
		storeGlobals(path, sha, g)
		return body, nil
	}
	g, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("[pool] reading cached globals %.12s: %v; asking the origin to resend", sha, err)
		}
		return nil, &missingGlobalsError{Sha: sha}
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil { // LRU order for pruneGlobals
		log.Printf("[pool] touching cached globals %.12s: %v", sha, err)
	}
	hdr["globals"] = json.RawMessage("true")
	return joinFrames(hdr, g, rest), nil
}

func validSha256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// globalsCacheDir sits beside the per-store chunk caches: globals are
// content-addressed, so one directory serves every store.
func globalsCacheDir() string {
	return filepath.Join(userdir.Cache(), "chunkcache", "globals")
}

// storeGlobals keeps a globals blob for later hash-only bodies. Failing to
// store costs one resend later, so it logs and carries on.
func storeGlobals(path, sha string, g []byte) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	if sum := sha256.Sum256(g); hex.EncodeToString(sum[:]) != sha {
		log.Printf("[pool] globals frame does not match its globals_sha256 %.12s; not caching it", sha)
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Printf("[pool] globals cache %s: %v", dir, err)
		return
	}
	f, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		log.Printf("[pool] globals cache %s: %v", dir, err)
		return
	}
	_, werr := f.Write(g)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		log.Printf("[pool] writing globals %.12s: %v %v", sha, werr, cerr)
		_ = os.Remove(f.Name())
		return
	}
	if err := os.Rename(f.Name(), path); err != nil {
		log.Printf("[pool] storing globals %.12s: %v", sha, err)
		_ = os.Remove(f.Name())
		return
	}
	pruneGlobals(dir, globalsCacheMax)
}

// pruneGlobals drops the least recently used blobs above max bytes, in the
// style of narCache.prune. A wrong eviction costs one resend.
func pruneGlobals(dir string, max int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[pool] pruning globals cache %s: %v", dir, err)
		return
	}
	type item struct {
		path string
		size int64
		used time.Time
	}
	var items []item
	var total int64
	for _, e := range entries {
		if !validSha256(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // removed under us
		}
		items = append(items, item{filepath.Join(dir, e.Name()), info.Size(), info.ModTime()})
		total += info.Size()
	}
	sort.Slice(items, func(i, j int) bool { return items[i].used.Before(items[j].used) })
	for _, it := range items {
		if total <= max {
			return
		}
		if err := os.Remove(it.path); err != nil {
			log.Printf("[pool] pruning globals %s: %v", it.path, err)
			continue
		}
		total -= it.size
	}
}
