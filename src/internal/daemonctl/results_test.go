package daemonctl

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tarOf(t *testing.T, files map[string]string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range files {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0644,
			Size:     int64(len(body)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func TestExtractResultsClassifiesNewAndUpdated(t *testing.T) {
	outDir := t.TempDir()
	existing := filepath.Join(outDir, "data.csv")
	if err := os.WriteFile(existing, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}

	manifest, err := extractResults(tarOf(t, map[string]string{
		"data.csv":          "new",
		"output/result.txt": "42",
	}), outDir)
	if err != nil {
		t.Fatalf("extractResults: %v", err)
	}

	if len(manifest.Updated) != 1 || manifest.Updated[0] != "data.csv" {
		t.Errorf("expected data.csv reported as updated, got %v", manifest.Updated)
	}
	if len(manifest.New) != 1 || manifest.New[0] != "output/result.txt" {
		t.Errorf("expected result.txt reported as new, got %v", manifest.New)
	}
	if manifest.Count() != 2 {
		t.Errorf("expected 2 files, got %d", manifest.Count())
	}

	got, err := os.ReadFile(existing)
	if err != nil || string(got) != "new" {
		t.Errorf("existing file not overwritten: %q (%v)", got, err)
	}
	nested, err := os.ReadFile(filepath.Join(outDir, "output", "result.txt"))
	if err != nil || string(nested) != "42" {
		t.Errorf("nested file not written: %q (%v)", nested, err)
	}
}

// Results come from another machine, so a path that climbs out of the target
// directory must be refused rather than written.
func TestExtractResultsRejectsPathTraversal(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "project")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}

	_, err := extractResults(tarOf(t, map[string]string{
		"../escaped.txt": "pwned",
	}), outDir)
	if err == nil {
		t.Fatal("expected traversal to be refused")
	}

	if _, statErr := os.Stat(filepath.Join(root, "escaped.txt")); statErr == nil {
		t.Fatal("file was written outside the output directory")
	}
}

func TestExtractResultsEmptyArchive(t *testing.T) {
	manifest, err := extractResults(tarOf(t, nil), t.TempDir())
	if err != nil {
		t.Fatalf("extractResults: %v", err)
	}
	if manifest.Count() != 0 {
		t.Fatalf("expected empty manifest, got %+v", manifest)
	}
}
