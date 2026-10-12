package daemonctl

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// .pipedpeerignore is written gitignore-style. A "dir/" line used to reach tar
// verbatim and match nothing, so a big data directory beside a script went
// into every upload while the ignore file said it would not.
func TestIgnoreFileExcludesDirectoriesWrittenWithASlash(t *testing.T) {
	project := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(project, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("job.py", "print(1)\n")
	write("big/data.csv", "a,b\n")
	write("results/out.txt", "x\n")
	write("build/keep.txt", "nested build kept: /build is anchored\n")
	write("sub/build/keep.txt", "x\n")
	write("notes.log", "x\n")
	write(".pipedpeerignore", "# comment\n\nbig/\nresults/\n/build\n*.log\n")

	tarPath := filepath.Join(t.TempDir(), "ws.tar")
	if err := CreateWorkspaceTar(project, tarPath, ""); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var files []string
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			files = append(files, strings.TrimPrefix(h.Name, "./"))
		}
	}
	sort.Strings(files)
	want := []string{".pipedpeerignore", "job.py", "sub/build/keep.txt"}
	if strings.Join(files, " ") != strings.Join(want, " ") {
		t.Errorf("tar holds %v, want %v", files, want)
	}
}
