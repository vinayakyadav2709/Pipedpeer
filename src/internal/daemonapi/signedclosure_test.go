package daemonapi

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
)

// Two closure transfer formats have to coexist, because two machines are
// never upgraded in the same instant. Getting the choice wrong in either
// direction is silent until a job fails on the far side: an upgraded peer
// sent the old format keeps needing require-sigs = false, and an old peer
// sent the new one fails somewhere inside nix with an error about the
// archive rather than about the version.

// TestOnlyAPeerThatSaysSoIsSentTheSignedFormat.
func TestOnlyAPeerThatSaysSoIsSentTheSignedFormat(t *testing.T) {
	tests := []struct {
		name    string
		caps    string
		wantNew bool
	}{
		{"a peer that has never heard of the question", "", false},
		{"a peer that lists it", narpack.FormatName, true},
		{"a peer that lists it among others", "somethingelse," + narpack.FormatName, true},
		{"a peer that lists it with spaces", " " + narpack.FormatName + " ", true},
		{"a peer listing only something else", "somethingelse", false},
		{"a peer whose format merely contains the name", narpack.FormatName + "x", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ph := &PeerHealth{ClosureFormats: tt.caps}
			if got := ph.TakesSignedClosures(); got != tt.wantNew {
				t.Errorf("TakesSignedClosures() = %v for %q, want %v", got, tt.caps, tt.wantNew)
			}
		})
	}
}

// signedArchive builds a well-formed narpack archive with no real store
// content, which is enough to test the routing decision.
func signedArchive(t *testing.T, root string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body, err := json.Marshal(narpack.Manifest{Format: narpack.Format, Roots: []string{root}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: narpack.ManifestName, Mode: 0o644, Size: int64(len(body)),
	}); err != nil {
		t.Fatal(err)
	}
	tw.Write(body)
	tw.Close()
	return buf.Bytes()
}

// TestTheFormatIsDecidedByTheBytesNotTheClaim.
//
// The sender does send a "format" field, but the receiver must not depend on
// it: a peer one version behind sends the old format without the field, and a
// peer one version ahead may send a field this node has never seen. Reading
// the archive itself is what stays correct across both.
func TestTheFormatIsDecidedByTheBytesNotTheClaim(t *testing.T) {
	signed := signedArchive(t, "/nix/store/aaaa-root")
	legacy := append([]byte{0x1f, 0x8b}, bytes.Repeat([]byte{0}, 600)...)

	for name, tc := range map[string]struct {
		body []byte
		want bool
	}{
		"a signed archive":                     {signed, true},
		"a gzipped export stream":              {legacy, false},
		"something far too short to be either": {[]byte("hi"), false},
		"a tar that is not one of ours":        {foreignTar(t), false},
	} {
		t.Run(name, func(t *testing.T) {
			br := bufio.NewReaderSize(bytes.NewReader(tc.body), 1024)
			if got := narpack.IsArchive(br); got != tc.want {
				t.Errorf("IsArchive = %v, want %v", got, tc.want)
			}
			// Whatever the answer, sniffing must not consume anything: the
			// bytes it looked at are the first bytes the importer needs.
			rest, _ := io.ReadAll(br)
			if !bytes.Equal(rest, tc.body) {
				t.Errorf("sniffing consumed %d of %d bytes", len(tc.body)-len(rest), len(tc.body))
			}
		})
	}
}

// foreignTar is a valid tar that is not a closure archive - a workspace that
// reached the wrong endpoint. Unpacking one into a store cache would be a
// stranger's files written wherever their names said.
func foreignTar(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "some/workspace/file.py", Mode: 0o644, Size: 3})
	tw.Write([]byte("abc"))
	tw.Close()
	return buf.Bytes()
}

// TestASignedClosureIsNeverCachedAsTheClosure.
//
// The archive holds only the paths one peer was missing, so it is not the
// closure. Cached under the closure's key it would later be forwarded to a
// third node as if it were complete - the same hazard the partial NAR path
// has, arriving by a different door.
func TestASignedClosureIsNeverCachedAsTheClosure(t *testing.T) {
	dir := t.TempDir()
	s := New("test-node")
	s.narCache = &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}
	storePath := "/nix/store/aaaa-root"

	var buf bytes.Buffer
	mp := multipart.NewWriter(&buf)
	_ = mp.WriteField("store_path", storePath)
	_ = mp.WriteField("format", narpack.FormatName)
	fw, _ := mp.CreateFormFile("nar", "closure.tar")
	fw.Write(signedArchive(t, storePath))
	mp.Close()

	// The import is stubbed to succeed, because everything worth asserting
	// happens after it: with a real store the call would fail here and the
	// test would pass without ever reaching the code it is named for.
	var gotRoots []string
	old := signedImport
	signedImport = func(_ context.Context, _ string, roots []string) error {
		gotRoots = roots
		return nil
	}
	defer func() { signedImport = old }()

	req := httptest.NewRequest(http.MethodPost, "/v1/store/import", &buf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()
	s.handleStoreImport(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("import returned %d: %s", rec.Code, rec.Body.String())
	}
	if path, ok := s.narCache.narFileFor(storePath); ok {
		t.Errorf("a signed archive was cached as the closure at %q", path)
	}
	// And what was imported is what this node asked for, not whatever the
	// sender's manifest happened to name.
	if len(gotRoots) != 1 || gotRoots[0] != storePath {
		t.Errorf("imported %v, want just %q", gotRoots, storePath)
	}
}

// TestASenderCannotMaterialiseSomethingElse.
//
// The archive carries its own list of roots, and it came from another
// machine. Importing what it names rather than what this node asked for would
// let a peer answering a request for one closure put a different one into
// this store.
func TestASenderCannotMaterialiseSomethingElse(t *testing.T) {
	dir := t.TempDir()
	s := New("test-node")
	s.narCache = &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}
	asked := "/nix/store/aaaa-what-was-asked-for"

	var buf bytes.Buffer
	mp := multipart.NewWriter(&buf)
	_ = mp.WriteField("store_path", asked)
	fw, _ := mp.CreateFormFile("nar", "closure.tar")
	fw.Write(signedArchive(t, "/nix/store/zzzz-something-else"))
	mp.Close()

	var gotRoots []string
	old := signedImport
	signedImport = func(_ context.Context, _ string, roots []string) error {
		gotRoots = roots
		return nil
	}
	defer func() { signedImport = old }()

	req := httptest.NewRequest(http.MethodPost, "/v1/store/import", &buf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	s.handleStoreImport(httptest.NewRecorder(), req)

	for _, r := range gotRoots {
		if r == "/nix/store/zzzz-something-else" {
			t.Fatalf("the sender's own root was imported: %v", gotRoots)
		}
	}
	if len(gotRoots) != 1 || gotRoots[0] != asked {
		t.Errorf("imported %v, want just %q", gotRoots, asked)
	}
}

// TestTheSendPathHonoursWhatThePeerSaidItTakes.
//
// TakesSignedClosures being right is not the same as importStoreOnPeer
// consulting it: a mutation that sent the signed format to EVERY peer left
// the predicate's own test green, because that test never reached the code
// that calls it. This drives the real send path against a peer that records
// what arrived.
func TestTheSendPathHonoursWhatThePeerSaidItTakes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		formats string
		// wantSignedField is whether the upload should declare the signed
		// format. An old peer must never be sent it.
		wantSignedField bool
	}{
		{"a peer that has not been upgraded", "", false},
		{"a peer that takes signed closures", narpack.FormatName, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			gotFormat := ""
			seen := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/store/missing" {
					writeJSON(w, http.StatusOK, map[string]any{"missing": []string{}})
					return
				}
				if err := r.ParseMultipartForm(32 << 20); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				gotFormat, seen = r.FormValue("format"), true
				mu.Unlock()
				writeJSON(w, http.StatusOK, map[string]any{"cached": true})
			}))
			defer srv.Close()

			host, port := splitHostPort(t, srv.URL)
			ph := &PeerHealth{Host: host, Port: port, ClosureFormats: tc.formats}

			dir := t.TempDir()
			s := New("test-node")
			s.narCache = &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}

			// A stand-in for the closure archive. Building a real one needs
			// nix; what is under test is which format the sender chooses.
			nar := filepath.Join(dir, "closure.nar")
			if err := os.WriteFile(nar, []byte("legacy-export-stream"), 0o644); err != nil {
				t.Fatal(err)
			}

			// Stubbed so that "the peer was not offered it" and "it could not
			// be built" stay distinguishable: without this, a sender that
			// ignored the peer's answer entirely would still fall back here
			// and the test would pass.
			asked := false
			oldBuild := buildSignedClosure
			buildSignedClosure = func(_ *Server, _ *PeerHealth, _ string) (string, func(), bool) {
				asked = true
				pack := filepath.Join(dir, "signed.tar")
				if err := os.WriteFile(pack, signedArchive(t, "/nix/store/aaaa-root"), 0o644); err != nil {
					t.Fatal(err)
				}
				return pack, func() {}, true
			}
			defer func() { buildSignedClosure = oldBuild }()

			_ = s.importStoreOnPeer(ph, "/nix/store/aaaa-root", nar)

			if asked != tc.wantSignedField {
				t.Errorf("the signed transfer was %sbuilt for a peer advertising %q",
					map[bool]string{true: "", false: "not "}[asked], tc.formats)
			}

			mu.Lock()
			defer mu.Unlock()
			if !seen {
				t.Fatal("the peer never received an upload")
			}
			if got := gotFormat == narpack.FormatName; got != tc.wantSignedField {
				t.Errorf("upload declared format %q (signed=%v), want signed=%v",
					gotFormat, got, tc.wantSignedField)
			}
		})
	}
}

func splitHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return u.Hostname(), p
}
