package tarcodec

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sampleTar builds a small archive with recognisable contents.
func sampleTar(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func firstEntry(t *testing.T, r io.Reader) (string, string) {
	t.Helper()
	tr := tar.NewReader(r)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("reading the archive: %v", err)
	}
	body, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	return hdr.Name, string(body)
}

// TestEveryEncodingIsReadable.
//
// A cluster is upgraded one machine at a time, and in both directions: a new
// submitter meets an old daemon and an old submitter a new one for as long as
// the rollout takes. Workspaces have been plain, then gzip, now zstd, so all
// three arrive.
func TestEveryEncodingIsReadable(t *testing.T) {
	raw := sampleTar(t, "hello.py", "print('hi')")

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(raw)
	gw.Close()

	var zs bytes.Buffer
	zw, err := Writer(&zs)
	if err != nil {
		t.Fatal(err)
	}
	zw.Write(raw)
	zw.Close()

	for name, payload := range map[string][]byte{
		"plain": raw,
		"gzip":  gz.Bytes(),
		"zstd":  zs.Bytes(),
	} {
		rd, done, err := Reader(bytes.NewReader(payload))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		got, body := firstEntry(t, rd)
		done()
		if got != "hello.py" || body != "print('hi')" {
			t.Errorf("%s round-tripped to (%q, %q)", name, got, body)
		}
	}
}

// TestSniffNamesTheEncoding without consuming it, or the archive would be
// short its first bytes.
func TestSniffNamesTheEncoding(t *testing.T) {
	cases := map[Encoding][]byte{
		Zstd:  {0x28, 0xb5, 0x2f, 0xfd, 0, 0},
		Gzip:  {0x1f, 0x8b, 8, 0, 0, 0},
		Plain: []byte("hello.py\x00\x00\x00\x00"),
	}
	for want, payload := range cases {
		br := bufio.NewReader(bytes.NewReader(payload))
		if got := Sniff(br); got != want {
			t.Errorf("sniffed %q, want %q", got, want)
		}
		// Nothing consumed: the next read must still see byte zero.
		b, err := br.Peek(1)
		if err != nil || b[0] != payload[0] {
			t.Errorf("%q: sniffing consumed the first byte", want)
		}
	}
}

// TestASmallArchiveIsLeftAlone. Under the floor the framing and a second pass
// over the disk cost more than the bytes saved on any link worth having.
func TestASmallArchiveIsLeftAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small.tar")
	raw := sampleTar(t, "a.py", "x = 1")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CompressFile(path, CompressFloor); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, raw) {
		t.Error("a small archive was rewritten; under the floor it should travel as it is")
	}
}

// TestALargeArchiveIsCompressedAndStillReadable — the whole point, and the
// failure mode if it is wrong is a workspace that cannot be unpacked.
func TestALargeArchiveIsCompressedAndStillReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.tar")
	body := strings.Repeat("import numpy as np\n", 200_000) // compressible, like source
	raw := sampleTar(t, "big.py", body)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	before := int64(len(raw))

	if err := CompressFile(path, CompressFloor); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= before {
		t.Errorf("compressed to %d bytes from %d; nothing was saved", info.Size(), before)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rd, done, err := Reader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	name, got := firstEntry(t, rd)
	if name != "big.py" || got != body {
		t.Errorf("the archive did not survive the round trip (name %q, %d bytes)",
			name, len(got))
	}
}

// TestAMissingFileIsAnError rather than a silent no-op that would upload
// whatever happened to be at that path.
func TestAMissingFileIsAnError(t *testing.T) {
	if err := CompressFile(filepath.Join(t.TempDir(), "nope.tar"), 0); err == nil {
		t.Error("compressing a file that does not exist reported success")
	}
}
