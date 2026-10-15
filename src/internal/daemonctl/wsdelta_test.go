package daemonctl

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
)

// After one file is edited, the delta archive carries that file only and
// names the rest for the node to fill from its cache.
func TestWriteDeltaTarSendsOnlyEdited(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "ws.tar")
	f, _ := os.Create(src)
	tw := tar.NewWriter(f)
	for _, e := range []struct{ name, body string }{{"./a.py", "a"}, {"./b.py", "b edited"}, {"./c.csv", "c"}} {
		tw.WriteHeader(&tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body)), Typeflag: tar.TypeReg})
		tw.Write([]byte(e.body))
	}
	tw.Close()
	f.Close()

	shas, err := workspaceHashes(src)
	if err != nil || len(shas) != 3 {
		t.Fatalf("hashes = %v, %v", shas, err)
	}
	dst := filepath.Join(dir, "delta.tar")
	skipped, err := writeDeltaTar(src, dst, shas, map[string]bool{shas[1]: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 2 || skipped[0].Name != "./a.py" || skipped[1].Name != "./c.csv" {
		t.Fatalf("skipped = %+v", skipped)
	}

	df, _ := os.Open(dst)
	defer df.Close()
	in, closeIn, err := tarcodec.Reader(df)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIn()
	tr := tar.NewReader(in)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, hdr.Name)
	}
	if len(names) != 1 || names[0] != "./b.py" {
		t.Errorf("delta carries %v, want only ./b.py", names)
	}
}
