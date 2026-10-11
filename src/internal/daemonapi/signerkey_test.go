package daemonapi

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
)

func newKeyPair(t *testing.T) identity.KeyPair {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return identity.KeyPair{Public: pub, Private: priv}
}

// manifestArchive is a signed archive holding only a manifest with m's
// signer and the root /nix/store/aaaa-root.
func manifestArchive(t *testing.T, m narpack.Manifest) []byte {
	t.Helper()
	m.Format, m.Roots = narpack.Format, []string{"/nix/store/aaaa-root"}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	body, _ := json.Marshal(m)
	if err := tw.WriteHeader(&tar.Header{Name: narpack.ManifestName, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	tw.Write(body)
	tw.Close()
	return archive.Bytes()
}

// importWithManifest posts a signed archive carrying m and reports the keys
// the import was told to trust, whether it ran, and the response.
func importWithManifest(t *testing.T, m narpack.Manifest) ([]string, bool, *httptest.ResponseRecorder) {
	t.Helper()
	fakeSystemTrust(t, false, nil) // never ask this machine's nix or sudo
	s := New("test-node")
	s.narCache = &narCache{dir: filepath.Join(t.TempDir(), "nar-cache"), byID: map[string]string{}}
	root := "/nix/store/aaaa-root"
	archive := bytes.NewBuffer(manifestArchive(t, m))

	var buf bytes.Buffer
	mp := multipart.NewWriter(&buf)
	_ = mp.WriteField("store_path", root)
	fw, _ := mp.CreateFormFile("nar", "closure.tar")
	fw.Write(archive.Bytes())
	mp.Close()

	var gotKeys []string
	ran := false
	old := signedImport
	signedImport = func(_ context.Context, _ string, _ []string, keys ...string) error {
		ran, gotKeys = true, keys
		return nil
	}
	defer func() { signedImport = old }()

	req := httptest.NewRequest(http.MethodPost, "/v1/store/import", &buf)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()
	s.handleStoreImport(rec, req)
	return gotKeys, ran, rec
}

// TestAFreshNodeTrustsTheSenderItHasNotPolled: a worker that has not polled
// the submitter yet refused its first closure ("lacks a signature by a
// trusted key"), so the first run after joining stayed local. The archive
// now names its signer, and a key that proves it belongs to that signer is
// trusted for the import.
func TestAFreshNodeTrustsTheSenderItHasNotPolled(t *testing.T) {
	sender := newKeyPair(t)
	key := nixsign.PublicKey(sender)
	keys, ran, rec := importWithManifest(t, narpack.Manifest{Signer: sender.Fingerprint(), SignerKey: key})
	if rec.Code != http.StatusOK || !ran {
		t.Fatalf("import returned %d: %s", rec.Code, rec.Body.String())
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("the sender's key was not passed to the import: %q", keys)
	}
}

// TestASignerKeyUnderAnotherNodesNameIsRefused: a key that does not belong to
// the node it is offered for must not be trusted, or a sender could sign as
// somebody else.
func TestASignerKeyUnderAnotherNodesNameIsRefused(t *testing.T) {
	sender, other := newKeyPair(t), newKeyPair(t)
	_, ran, rec := importWithManifest(t, narpack.Manifest{Signer: other.Fingerprint(), SignerKey: nixsign.PublicKey(sender)})
	if ran {
		t.Fatal("imported a closure whose signer key belongs to a different node")
	}
	if rec.Code == http.StatusOK || !strings.Contains(rec.Body.String(), "does not belong") {
		t.Fatalf("want a refusal naming the mismatch, got %d: %s", rec.Code, rec.Body.String())
	}
}

// An older sender names no signer; its receiver relies on polling, as before.
func TestAnArchiveWithoutASignerAddsNoKey(t *testing.T) {
	keys, ran, rec := importWithManifest(t, narpack.Manifest{})
	if rec.Code != http.StatusOK || !ran || len(keys) != 0 {
		t.Fatalf("got %d ran=%v keys=%q", rec.Code, ran, keys)
	}
}
