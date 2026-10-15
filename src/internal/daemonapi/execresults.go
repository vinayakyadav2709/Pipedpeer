package daemonapi

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"

	"github.com/go-chi/chi/v5"
)

// handleJobResults handles GET /v1/jobs/{id}/results.
func (s *Server) handleJobResults(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	job, ok := s.jobs[jobID]
	s.jobsMu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-results.tar", jobID))

	// Results carry whatever the job produced - model checkpoints, CSVs,
	// logs - and travel back over the same link the closure came in on.
	//
	// zstd rather than gzip: several times faster to write at a better ratio,
	// which matters here because the machine doing the compressing is the one
	// the user is waiting on. Content-Encoding says zstd, and the client
	// sniffs anyway, so an older client reading this sees an encoding it does
	// not know and falls through to the magic bytes.
	w.Header().Set("Content-Encoding", "zstd")
	zw, err := tarcodec.Writer(w)
	if err != nil {
		fmt.Fprintf(os.Stderr, "results tar for %s: %v\n", jobID, err)
		return
	}
	defer zw.Close()
	if err := writeResultsTar(zw, job); err != nil {
		// Headers are already out, so the client sees a truncated archive and
		// reports the read error. Log for the node operator.
		fmt.Fprintf(os.Stderr, "results tar for %s failed: %v\n", jobID, err)
	}
}

// writeResultsTar streams the files a job created or modified. Sending the
// whole work dir back would overwrite the submitter's own source files with
// copies of what they just uploaded, and makes every task in a fan-out
// re-transfer the entire project.
// deletedManifestName is the tar entry carrying the list of files the job
// deleted relative to its work dir. The CLI removes exactly these from the
// submitter's folder after extraction — scoped to what this job uploaded,
// so unrelated local files are never touched.
const deletedManifestName = ".pipedpeer-deleted.json"

func writeResultsTar(w io.Writer, job *JobRecord) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	present := map[string]bool{}
	walkErr := filepath.WalkDir(job.WorkDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(job.WorkDir, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		present[rel] = true
		// pipedpeer's own files - the shim, its bytecode, the receipt - are
		// not the job's output and must not land in the user's project.
		if strings.HasPrefix(rel, ".pipedpeer/") {
			return nil
		}

		if prev, uploaded := job.Uploaded[rel]; uploaded && !stampOf(info).changed(prev) {
			return nil
		}

		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return nil
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if walkErr != nil {
		return walkErr
	}

	// Deletion propagation: shipped out for this job but gone by the end of
	// it. Only successful runs ever reach the results endpoint (execution.go
	// returns before downloading on failure), so a crash cannot wipe files.
	var deleted []string
	for rel := range job.Uploaded {
		if !present[rel] {
			deleted = append(deleted, rel)
		}
	}
	if len(deleted) == 0 {
		return nil
	}
	sort.Strings(deleted)
	payload, err := json.Marshal(deleted)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: deletedManifestName, Mode: 0o644, Size: int64(len(payload)),
	}); err != nil {
		return err
	}
	_, err = tw.Write(payload)
	return err
}

// importNAR imports a closure into the local Nix store.
//
// --no-check-sigs is needed on daemons whose store requires signed paths, but
// classic `nix-store --import` only learned that flag in some versions and
// errors out on others. Rather than pin one Nix version, try with the flag and
// fall back when it is not recognised.
func importNAR(ctx context.Context, nar *os.File) ([]byte, error) {
	run := func(args ...string) ([]byte, error) {
		if _, err := nar.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind nar: %w", err)
		}
		// zstd now, gzip from older peers, plain from older still: sniffed.
		src, closeSrc, err := tarcodec.Reader(nar)
		if err != nil {
			return nil, fmt.Errorf("open nar: %w", err)
		}
		defer closeSrc()
		cmd, cleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store"}, args...)...)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		cmd.Stdin = src
		return cmd.CombinedOutput()
	}

	out, err := run("--import", "--no-check-sigs")
	if err == nil {
		return out, nil
	}
	if !strings.Contains(string(out), "unknown flag") {
		return out, err
	}
	return run("--import")
}
