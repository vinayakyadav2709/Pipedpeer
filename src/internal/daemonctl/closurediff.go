package daemonctl

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// seedClosure gives a node the store paths it lacks, and reports how many of
// the closure's paths had to cross.
//
// The whole-closure archive is keyed on the top-level store path, so two
// environments differing by one package share none of it: adding pandas to a
// numpy script re-sent python, numpy and the entire interpreter tree. The
// broadcast path used for pool spill has asked peers what they lack since it
// was written; the ordinary `run --remote` path did not, so the common case -
// one person, one machine, a script that grows a dependency - was the one
// paying full price. Measured on a 43-path closure whose peer already held
// 36 of them.
//
// Store paths are content-addressed, which is what makes this safe: a path
// the node already has under that name is that path, and there is no version
// of it that could be stale.
//
// Returns sent=0, ok=false when a diff is not worth it or not possible, and
// the caller sends the archive whole - every failure here is a slower
// transfer rather than a broken one.
func seedClosure(host string, port int, storePath string) (sent, total int, ok bool) {
	paths, err := closurePathsLocal(storePath)
	if err != nil || len(paths) == 0 {
		return 0, 0, false
	}
	missing, ok := askMissing(host, port, paths)
	if !ok {
		// An older daemon does not know the question. Sending the whole
		// closure is what it expects.
		return 0, len(paths), false
	}
	if len(missing) == 0 {
		// Every path is already there. Nothing to send, and nothing to do:
		// the caller's own cache check decides whether to upload a NAR.
		return 0, len(paths), true
	}
	if len(missing) == len(paths) {
		// Nothing in common. A diff would be the whole archive with extra
		// round trips.
		return 0, len(paths), false
	}

	nar, err := os.CreateTemp("", "closurediff-*.nar")
	if err != nil {
		return 0, len(paths), false
	}
	narPath := nar.Name()
	nar.Close()
	defer os.Remove(narPath)

	if err := exportPathsLocal(missing, narPath); err != nil {
		return 0, len(paths), false
	}
	if err := postPartial(host, port, storePath, narPath); err != nil {
		return 0, len(paths), false
	}
	return len(missing), len(paths), true
}

// closurePathsLocal lists every store path the closure needs.
func closurePathsLocal(storePath string) ([]string, error) {
	cmd, cleanup, err := nixstore.Cmd("", "nix-store", "-qR", storePath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nix-store -qR %s: %w: %s", storePath, err,
			strings.TrimSpace(stderr.String()))
	}
	return strings.Fields(string(out)), nil
}

// exportPathsLocal writes a gzipped archive of exactly these paths. nix orders
// an export so that references come before the things that need them, which
// is what lets the far side import a fragment.
func exportPathsLocal(paths []string, destPath string) error {
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()

	cmd, cleanup, err := nixstore.Cmd("", append([]string{"nix-store", "--export"}, paths...)...)
	if err != nil {
		return err
	}
	defer cleanup()
	cmd.Stdout = gz
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// askMissing asks a node which of these paths it does not have. A node that
// cannot answer yields ok=false rather than an empty list, so "no answer" is
// never mistaken for "has everything".
func askMissing(host string, port int, paths []string) ([]string, bool) {
	body, err := json.Marshal(map[string]any{"paths": paths})
	if err != nil {
		return nil, false
	}
	url := fmt.Sprintf("http://%s:%d/v1/store/missing", host, port)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var out struct {
		Missing []string `json:"missing"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil, false
	}
	return out.Missing, true
}

// postPartial hands the fragment to the node's store.
//
// partial=1 matters: the receiving side must not file a fragment away as the
// cache entry for this closure, or it would later forward it to a third node
// as though it were the whole thing.
func postPartial(host string, port int, storePath, narPath string) error {
	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer mp.Close()
		mp.WriteField("store_path", storePath)
		mp.WriteField("partial", "1")
		part, err := mp.CreateFormFile("nar", "closure-diff.nar")
		if err != nil {
			return
		}
		io.Copy(part, f)
	}()

	url := fmt.Sprintf("http://%s:%d/v1/store/import", host, port)
	resp, err := http.Post(url, mp.FormDataContentType(), pr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("partial import rejected: %s", strings.TrimSpace(string(msg)))
	}
	return nil
}
