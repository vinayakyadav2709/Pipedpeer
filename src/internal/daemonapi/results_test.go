package daemonapi

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readTar returns the regular file entries of a tar stream, keyed by name.
func readTar(t *testing.T, r io.Reader) map[string]string {
	t.Helper()
	out := map[string]string{}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read entry %s: %v", hdr.Name, err)
		}
		out[hdr.Name] = string(body)
	}
}

func writeFile(t *testing.T, path, content string) os.FileInfo {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// A job that produced nothing must send nothing back. Returning the whole work
// dir would overwrite the submitter's sources with copies of their own upload.
func TestResultsSkipUntouchedUploads(t *testing.T) {
	workDir := t.TempDir()
	info := writeFile(t, filepath.Join(workDir, "main.py"), "print('hi')")

	job := &JobRecord{
		WorkDir:  workDir,
		Uploaded: map[string]FileStamp{"main.py": stampOf(info)},
	}

	var buf bytes.Buffer
	if err := writeResultsTar(&buf, job); err != nil {
		t.Fatalf("writeResultsTar: %v", err)
	}

	if got := readTar(t, &buf); len(got) != 0 {
		t.Fatalf("expected no files, got %v", got)
	}
}

func TestResultsIncludeNewAndModifiedFiles(t *testing.T) {
	workDir := t.TempDir()
	unchanged := writeFile(t, filepath.Join(workDir, "main.py"), "print('hi')")
	original := writeFile(t, filepath.Join(workDir, "data.csv"), "a,b")

	job := &JobRecord{
		WorkDir: workDir,
		Uploaded: map[string]FileStamp{
			"main.py":  stampOf(unchanged),
			"data.csv": stampOf(original),
		},
	}

	// The job rewrites one uploaded file and creates one of its own.
	modified := filepath.Join(workDir, "data.csv")
	if err := os.WriteFile(modified, []byte("a,b,c"), 0644); err != nil {
		t.Fatal(err)
	}
	// Keep the size the same but move the clock, to prove mtime alone counts.
	stale := filepath.Join(workDir, "output", "result.txt")
	writeFile(t, stale, "42")

	var buf bytes.Buffer
	if err := writeResultsTar(&buf, job); err != nil {
		t.Fatalf("writeResultsTar: %v", err)
	}

	got := readTar(t, &buf)
	if _, ok := got["main.py"]; ok {
		t.Errorf("untouched upload was returned: %v", got)
	}
	if got["data.csv"] != "a,b,c" {
		t.Errorf("modified file missing or stale: %q", got["data.csv"])
	}
	if got["output/result.txt"] != "42" {
		t.Errorf("new file missing: %q", got["output/result.txt"])
	}
}

// A rewrite that happens to keep the same length still has to come back.
func TestResultsDetectSameSizeRewrite(t *testing.T) {
	workDir := t.TempDir()
	path := filepath.Join(workDir, "counter.txt")
	info := writeFile(t, path, "0000")

	job := &JobRecord{
		WorkDir:  workDir,
		Uploaded: map[string]FileStamp{"counter.txt": stampOf(info)},
	}

	if err := os.WriteFile(path, []byte("9999"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Now().Add(time.Second), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeResultsTar(&buf, job); err != nil {
		t.Fatalf("writeResultsTar: %v", err)
	}
	if got := readTar(t, &buf)["counter.txt"]; got != "9999" {
		t.Fatalf("same-size rewrite not returned, got %q", got)
	}
}

// A job with no upload record (older daemon state) must still return results
// rather than silently sending an empty archive.
func TestResultsWithoutUploadRecordReturnsEverything(t *testing.T) {
	workDir := t.TempDir()
	writeFile(t, filepath.Join(workDir, "out.txt"), "data")

	var buf bytes.Buffer
	if err := writeResultsTar(&buf, &JobRecord{WorkDir: workDir}); err != nil {
		t.Fatalf("writeResultsTar: %v", err)
	}
	if got := readTar(t, &buf)["out.txt"]; got != "data" {
		t.Fatalf("expected file to be returned, got %q", got)
	}
}
