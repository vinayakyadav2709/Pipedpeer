package narpack

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A narinfo's URL names the archive file, and the suffix depends on how the
// cache was compressed. Deriving it instead of reading it produces an archive
// whose narinfos point at files it does not contain - and the failure lands
// on the receiver, mid-import, as a missing substituter entry.
func TestNarinfoIsReadNotGuessed(t *testing.T) {
	body := `StorePath: /nix/store/aaaa-thing
URL: nar/deadbeef.nar.zst
Compression: zstd
FileHash: sha256:deadbeef
FileSize: 277
NarHash: sha256:cafebabe
NarSize: 640
References: bbbb-dep
Sig: pipedpeer-15cb1ed8e5150c1a:AAAA==
`
	ni, err := ParseNarinfo("aaaa.narinfo", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if ni.StorePath != "/nix/store/aaaa-thing" {
		t.Errorf("StorePath = %q", ni.StorePath)
	}
	if ni.URL != "nar/deadbeef.nar.zst" {
		t.Errorf("URL = %q", ni.URL)
	}
	if !ni.Signed {
		t.Errorf("a narinfo with a Sig line was read as unsigned")
	}
}

// References on a big closure is one enormous line. A scanner left at its
// default buffer stops there, which is BEFORE the URL field - so the parse
// would fail on exactly the large closures this transfer exists for, and
// succeed on every small fixture written by hand.
func TestNarinfoWithAHugeReferencesLineStillParses(t *testing.T) {
	var refs strings.Builder
	for i := 0; i < 20000; i++ {
		refs.WriteString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-some-package-1.2.3 ")
	}
	body := "StorePath: /nix/store/aaaa-thing\n" +
		"References: " + refs.String() + "\n" +
		"URL: nar/deadbeef.nar.zst\n"
	ni, err := ParseNarinfo("aaaa.narinfo", strings.NewReader(body))
	if err != nil {
		t.Fatalf("a %d byte References line broke the parse: %v", refs.Len(), err)
	}
	if ni.URL != "nar/deadbeef.nar.zst" {
		t.Errorf("URL after a long References line = %q", ni.URL)
	}
}

func TestNarinfoWithoutTheFieldsWeNeedIsRejected(t *testing.T) {
	for name, body := range map[string]string{
		"no StorePath": "URL: nar/x.nar.zst\n",
		"no URL":       "StorePath: /nix/store/aaaa-thing\n",
	} {
		if _, err := ParseNarinfo("x.narinfo", strings.NewReader(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// fakeCache writes a cache directory by hand, so packing can be tested
// without nix.
func fakeCache(t *testing.T, paths map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "nar"), 0o755); err != nil {
		t.Fatal(err)
	}
	for hash, store := range paths {
		body := "StorePath: " + store + "\nURL: nar/" + hash + ".nar.zst\nSig: k:AAAA==\n"
		if err := os.WriteFile(filepath.Join(dir, hash+".narinfo"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "nar", hash+".nar.zst"),
			[]byte("archive-of-"+hash), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func entries(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		var buf bytes.Buffer
		buf.ReadFrom(tr)
		out[hdr.Name] = buf.String()
	}
	return out
}

// The point of asking a peer what it lacks: only those archives travel.
//
// Every narinfo does, because they are what make the receiver able to resolve
// the closure on its own, and they are small. Sending every NAR instead would
// re-upload a multi-hundred-megabyte environment to a peer that already has
// it, which is what the "missing paths" negotiation exists to prevent.
func TestOnlyTheArchivesThePeerLacksTravel(t *testing.T) {
	cache := fakeCache(t, map[string]string{
		"aaaa": "/nix/store/aaaa-root",
		"bbbb": "/nix/store/bbbb-has-it",
		"cccc": "/nix/store/cccc-lacks-it",
	})
	closure := []string{"/nix/store/bbbb-has-it", "/nix/store/cccc-lacks-it", "/nix/store/aaaa-root"}
	want := []string{"/nix/store/cccc-lacks-it"}

	var buf bytes.Buffer
	if err := Pack(cache, []string{"/nix/store/aaaa-root"}, closure, want, &buf); err != nil {
		t.Fatal(err)
	}
	got := entries(t, buf.Bytes())

	for _, n := range []string{"aaaa.narinfo", "bbbb.narinfo", "cccc.narinfo", ManifestName} {
		if _, ok := got[n]; !ok {
			t.Errorf("archive is missing %s", n)
		}
	}
	if _, ok := got["nar/cccc.nar.zst"]; !ok {
		t.Errorf("the archive the peer lacks did not travel")
	}
	for _, n := range []string{"nar/aaaa.nar.zst", "nar/bbbb.nar.zst"} {
		if _, ok := got[n]; ok {
			t.Errorf("%s travelled although the peer did not ask for it", n)
		}
	}
}

// A peer that cannot answer "which of these do you lack" must be sent
// everything. Treating no answer as an empty list would send metadata with no
// archives at all and break the import on the far side.
func TestAPeerThatCannotAnswerIsSentEverything(t *testing.T) {
	cache := fakeCache(t, map[string]string{
		"aaaa": "/nix/store/aaaa-root",
		"bbbb": "/nix/store/bbbb-dep",
	})
	closure := []string{"/nix/store/aaaa-root", "/nix/store/bbbb-dep"}

	var buf bytes.Buffer
	if err := Pack(cache, []string{"/nix/store/aaaa-root"}, closure, nil, &buf); err != nil {
		t.Fatal(err)
	}
	got := entries(t, buf.Bytes())
	for _, n := range []string{"nar/aaaa.nar.zst", "nar/bbbb.nar.zst"} {
		if _, ok := got[n]; !ok {
			t.Errorf("%s did not travel to a peer that could not say what it has", n)
		}
	}
}

// An archive arrives from another machine. An entry named "../../../etc/nix/
// nix.conf" is not a transfer, and refusing it after the write has happened
// is not refusing it.
func TestAnArchiveCannotWriteOutsideItsDirectory(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := []byte(`{"format":1,"roots":["/nix/store/aaaa-root"]}`)
	tw.WriteHeader(&tar.Header{Name: ManifestName, Mode: 0o644, Size: int64(len(body))})
	tw.Write(body)
	evil := []byte("pwned")
	tw.WriteHeader(&tar.Header{Name: "../escaped.txt", Mode: 0o644, Size: int64(len(evil))})
	tw.Write(evil)
	tw.Close()

	// The escape lands one level up, inside a directory this test owns. It
	// used to reach for the shared temp root, which meant a run that broke
	// safeName on purpose left a file there and every later run of this test
	// failed on the debris rather than on the code.
	parent := t.TempDir()
	dir := filepath.Join(parent, "cache")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Unpack(bytes.NewReader(buf.Bytes()), dir); err == nil {
		t.Fatal("an entry escaping the directory was accepted")
	}
	if _, err := os.Stat(filepath.Join(parent, "escaped.txt")); err == nil {
		t.Fatal("the escaping entry was written before it was refused")
	}
}

func TestAnArchiveWithoutAManifestIsRejected(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "aaaa.narinfo", Mode: 0o644, Size: 3})
	tw.Write([]byte("abc"))
	tw.Close()
	if _, err := Unpack(bytes.NewReader(buf.Bytes()), t.TempDir()); err == nil {
		t.Fatal("an archive with no manifest was accepted")
	}
}

// A future format has to be refused rather than half-read. Misreading one is
// worse than rejecting it: the import would fail somewhere further in, with
// an error about nix rather than about the version.
func TestAnArchiveFromAFutureFormatIsRejected(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := []byte(`{"format":99,"roots":["/nix/store/aaaa-root"]}`)
	tw.WriteHeader(&tar.Header{Name: ManifestName, Mode: 0o644, Size: int64(len(body))})
	tw.Write(body)
	tw.Close()
	_, err := Unpack(bytes.NewReader(buf.Bytes()), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("a format-99 archive gave %v", err)
	}
}

// Pack and Unpack are two halves of one wire format; testing them apart
// leaves the seam untested.
func TestPackedArchiveUnpacksIntoAUsableCache(t *testing.T) {
	cache := fakeCache(t, map[string]string{
		"aaaa": "/nix/store/aaaa-root",
		"bbbb": "/nix/store/bbbb-dep",
	})
	closure := []string{"/nix/store/aaaa-root", "/nix/store/bbbb-dep"}
	var buf bytes.Buffer
	if err := Pack(cache, []string{"/nix/store/aaaa-root"}, closure, nil, &buf); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	m, err := Unpack(bytes.NewReader(buf.Bytes()), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Roots) != 1 || m.Roots[0] != "/nix/store/aaaa-root" {
		t.Fatalf("roots = %v", m.Roots)
	}
	idx, err := Index(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range closure {
		if _, ok := idx[p]; !ok {
			t.Errorf("%s is not in the unpacked cache", p)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "nar", "bbbb.nar.zst"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "archive-of-bbbb" {
		t.Errorf("archive body came through as %q", raw)
	}
}

// Packing a path the cache was never told about must fail loudly. Skipping it
// would produce an archive that looks complete and cannot be imported.
func TestPackingAnUnpublishedPathFails(t *testing.T) {
	cache := fakeCache(t, map[string]string{"aaaa": "/nix/store/aaaa-root"})
	var buf bytes.Buffer
	err := Pack(cache, []string{"/nix/store/aaaa-root"},
		[]string{"/nix/store/aaaa-root", "/nix/store/zzzz-never-published"}, nil, &buf)
	if err == nil {
		t.Fatal("packing a path with no narinfo was accepted")
	}
}
