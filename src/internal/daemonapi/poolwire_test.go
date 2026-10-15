package daemonapi

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func resetLinks(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	links.Lock()
	links.m = map[string]*peerLink{}
	links.Unlock()
}

func framesBody(t *testing.T, globals []byte, items ...[]byte) []byte {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"func_src": "def run(x):\n    return x\n", "items_frames": len(items), "globals": globals != nil,
	})
	raw := make([]json.RawMessage, len(items))
	for i, it := range items {
		raw[i] = it
	}
	return buildFrames(hdr, globals, raw)
}

// decodedGlobals runs a body through the peer side and returns the globals
// frame the worker would see.
func decodedGlobals(t *testing.T, enc []byte, ce string) []byte {
	t.Helper()
	body, err := decodePoolBody(ce, enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	hdr, rest, ok := splitFramesHeader(body)
	if !ok || !hdrBool(hdr, "globals") {
		t.Fatalf("decoded body has no globals frame")
	}
	g, _, err := readFrame(rest)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// A 100 MB global crosses the link once per peer: the second chunk to the
// same peer carries only its hash, a new peer gets it in full, and the peer
// rebuilds the identical frame from its cache.
func TestGlobalsCrossOncePerPeer(t *testing.T) {
	resetLinks(t)
	g := bytes.Repeat([]byte("0123456789abcdef"), 100<<20/16)
	body := framesBody(t, g, []byte("item"))

	first, ce1 := encodePeerBody("peerA", body)
	if !bytes.Equal(decodedGlobals(t, first, ce1), g) {
		t.Fatal("first chunk: globals changed on the way")
	}
	second, ce2 := encodePeerBody("peerA", body)
	plain, err := decodePoolBodyRaw(ce2, second)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) > 1<<20 {
		t.Fatalf("second chunk to the same peer still carries %d bytes", len(plain))
	}
	if !bytes.Equal(decodedGlobals(t, second, ce2), g) {
		t.Fatal("second chunk: peer did not rebuild the globals from its cache")
	}
	other, ce3 := encodePeerBody("peerB", body)
	if p, _ := decodePoolBodyRaw(ce3, other); len(p) < 100<<20 {
		t.Fatalf("a new peer got %d bytes, not the full globals", len(p))
	}
}

// decodePoolBodyRaw undoes only the Content-Encoding, to see what crossed.
func decodePoolBodyRaw(ce string, b []byte) ([]byte, error) {
	if ce == "zstd" {
		return zstdDecoder().DecodeAll(b, nil)
	}
	return b, nil
}

// A peer that lost its copy answers 409 missing_globals; the origin forgets
// and the one resend carries the frame again.
func TestGlobalsMissResendsOnce(t *testing.T) {
	resetLinks(t)
	g := make([]byte, 2<<20)
	if _, err := rand.Read(g); err != nil {
		t.Fatal(err)
	}
	body := framesBody(t, g, []byte("item"))
	links.Lock()
	sum := sha256Hex(g)
	linkFor("peerA").held[sum] = time.Now() // origin wrongly believes the peer has it
	links.Unlock()

	enc, ce := encodePeerBody("peerA", body)
	_, err := decodePoolBody(ce, enc)
	var miss *missingGlobalsError
	if !errors.As(err, &miss) || miss.Sha != sum {
		t.Fatalf("want a missing-globals error for %s, got %v", sum, err)
	}
	code, reply := poolDecodeStatus(err)
	replyJSON, _ := json.Marshal(reply)
	sha, ok := missingGlobalsIn(code, replyJSON)
	if code != http.StatusConflict || !ok || sha != sum {
		t.Fatalf("reply %d %s does not name the missing globals", code, replyJSON)
	}
	forgetPeerGlobals("peerA", sha)
	enc, ce = encodePeerBody("peerA", body)
	if !bytes.Equal(decodedGlobals(t, enc, ce), g) {
		t.Fatal("resend did not carry the globals")
	}
}

func TestGlobalsShaIsValidated(t *testing.T) {
	resetLinks(t)
	hdr := []byte(`{"items_frames":1,"globals":false,"globals_sha256":"../../etc/passwd"}`)
	body := buildFrames(hdr, nil, []json.RawMessage{[]byte("x")})
	if _, err := decodePoolBody("", body); err == nil {
		t.Fatal("a path in globals_sha256 was accepted")
	}
}

// Incompressible input costs under 100 KB of trial compression and is sent
// as is; compressible input on a slow link is compressed at a higher level.
func TestCompressChoice(t *testing.T) {
	noise := make([]byte, 10<<20)
	if _, err := rand.Read(noise); err != nil {
		t.Fatal(err)
	}
	if _, ok, trialed := compressChoice(noise, 0, false); ok || trialed >= 100<<10 {
		t.Fatalf("noise: compress=%v after %d trial bytes", ok, trialed)
	}
	text := bytes.Repeat([]byte("pipedpeer pool chunk, text-heavy pickle "), 1<<16)
	if lvl, ok, _ := compressChoice(text, 2e6, true); !ok || lvl == 0 {
		t.Fatalf("text on a 2 MB/s link: compress=%v level=%v", ok, lvl)
	}
	if _, ok, _ := compressChoice(text, 1e15, true); ok {
		t.Fatal("compressed for a link faster than any compressor")
	}
	if _, ok, _ := compressChoice([]byte("small"), 1, true); ok {
		t.Fatal("compressed a tiny body")
	}
	enc, ce := encodePeerBody("x", framesBody(t, nil, text))
	if ce != "zstd" || len(enc) >= len(text)/2 {
		t.Fatalf("text body: encoding %q, %d bytes", ce, len(enc))
	}
}

func TestLinkSpeedEWMA(t *testing.T) {
	resetLinks(t)
	if _, ok := linkSpeed("p"); ok {
		t.Fatal("speed known before any transfer")
	}
	observeLink("p", 100, time.Millisecond) // latency-bound, ignored
	if _, ok := clusterLinkSpeed(); ok {
		t.Fatal("a tiny transfer counted as a link sample")
	}
	observeLink("p", 10<<20, time.Second)
	observeLink("p", 20<<20, time.Second)
	b, ok := linkSpeed("p")
	want := 0.3*float64(20<<20) + 0.7*float64(10<<20)
	if !ok || b < want*0.99 || b > want*1.01 {
		t.Fatalf("ewma %v, want %v", b, want)
	}
	observeLink("q", 1<<20, time.Second)
	if sum, _ := clusterLinkSpeed(); sum < b+float64(1<<20)-1 {
		t.Fatalf("cluster speed %v is not the sum of peers", sum)
	}
}

func TestDecodeRejectsUnknownEncoding(t *testing.T) {
	if _, err := decodePoolBody("br", []byte("x")); err == nil {
		t.Fatal("unknown Content-Encoding accepted")
	}
}

// workerDriver builds a frames payload with one out-of-band item, one plain
// item and an out-of-band globals frame, runs a worker script on it, and
// checks the results (plain and out-of-band), their writability and steps.
const workerDriver = `
import json, os, pickle, struct, subprocess, sys
MAGIC = b"\x00PP5"
def dump(obj):
    bufs = []
    p = pickle.dumps(obj, protocol=5, buffer_callback=bufs.append)
    if not bufs:
        return p
    raws = [b.raw() for b in bufs]
    return b"".join([MAGIC + struct.pack(">II%dI" % len(raws), len(raws), len(p), *[r.nbytes for r in raws]), p] + raws)
def load(b):
    mv = memoryview(b)
    if bytes(mv[:4]) != MAGIC:
        return pickle.loads(mv)
    nbuf, plen = struct.unpack_from(">II", mv, 4)
    lens = struct.unpack_from(">%dI" % nbuf, mv, 12)
    off = 12 + 4 * nbuf
    p = mv[off:off + plen]; off += plen
    bufs = []
    for n in lens:
        bufs.append(mv[off:off + n]); off += n
    return pickle.loads(p, buffers=bufs)
script, mode, d = sys.argv[1], sys.argv[2], sys.argv[3]
items = [dump(pickle.PickleBuffer(bytearray(b"ab" * 5000))), dump(7)]
g = dump({"K": pickle.PickleBuffer(bytearray(b"k" * 3000))})
hdr = {"func_src": "def run(x):\n    return bytearray(x) if not isinstance(x, int) else x + len(K)\n",
       "items_frames": 2, "globals": True, "oob": True}
body = json.dumps(hdr).encode() + b"\n" + b"".join(struct.pack(">I", len(f)) + f for f in [g] + items)
inp, out = os.path.join(d, "in.bin"), os.path.join(d, "out.bin")
open(inp, "wb").write(body)
if mode == "cold":
    subprocess.run([sys.executable, script, inp, out], check=True)
else:
    r = subprocess.run([sys.executable, script], input=json.dumps({"id": 1, "in_file": inp, "out_file": out}) + "\n",
                       capture_output=True, text=True, check=True)
    ack = json.loads(r.stdout.strip().splitlines()[-1])
    assert ack.get("done"), ack
data = bytearray(open(out, "rb").read())
nl = data.find(b"\n")
head = json.loads(data[:nl])
mv = memoryview(data); off = nl + 1; res = []
for _ in range(head["results_frames"]):
    n = struct.unpack_from(">I", mv, off)[0]; res.append(load(mv[off + 4:off + 4 + n])); off += 4 + n
assert bytes(res[0]) == b"ab" * 5000, res[0][:10]
assert res[1] == 7 + 3000, res[1]
assert {s["step"] for s in head["steps"]} >= {"unpickle", "compute", "pickle"}, head["steps"]
print("WORKER-OK")
`

func TestWorkerScriptsReadAndWriteOutOfBand(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	for mode, script := range map[string]string{"cold": poolRunner, "warm": warmWorkerScript} {
		t.Run(mode, func(t *testing.T) {
			d := t.TempDir()
			sp, dp := filepath.Join(d, "worker.py"), filepath.Join(d, "driver.py")
			if err := os.WriteFile(sp, []byte(script), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dp, []byte(workerDriver), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command(py, dp, sp, mode, d).CombinedOutput()
			if err != nil || !strings.Contains(string(out), "WORKER-OK") {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}
