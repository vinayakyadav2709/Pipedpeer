package daemonapi

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/rs/zerolog/log"
)

// handleJobUpload processes POST /v1/jobs/upload.
func (s *Server) handleJobUpload(w http.ResponseWriter, r *http.Request) {
	// Stream the parts straight to their destinations instead of
	// ParseMultipartForm. That call spools every byte past its memory cap to
	// os.TempDir() before the handler sees any of it — on a tmpfs /tmp sized
	// to half of RAM, a workspace with a big dataset filled it and the daemon
	// rejected uploads it had plenty of disk for. The submitter writes the
	// parts in order (workspace, fields, nar), so nothing needs a second pass.
	mr, err := r.MultipartReader()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
		return
	}

	jobID := generateLeaseID()
	jobDir := filepath.Join(s.jobDir, jobID)
	workDir := filepath.Join(jobDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "mkdir: " + err.Error()})
		return
	}
	var storePath, scriptPath, narPath, skipBroadcast, leaseID string
	fail := func(status int, msg string) {
		os.RemoveAll(jobDir)
		// No job will run under this lease: free the slot now. A submitter
		// killed mid-transfer never sends /v1/complete, and the lease would
		// otherwise block this node (a GPU node refuses "no GPU available")
		// until the running-lease TTL.
		if leaseID != "" {
			log.Warn().Str("lease", leaseID).Str("error", msg).Msg("upload failed; releasing its lease")
			s.processComplete(completeRequest{LeaseID: leaseID, Status: "upload failed"})
		}
		writeJSON(w, status, map[string]string{"error": msg})
	}

	var haveWorkspace bool
	var fromCAS []casEntry
	uploaded := make(map[string]FileStamp)

	readField := func(p io.Reader) string {
		b, _ := io.ReadAll(io.LimitReader(p, 4096))
		return string(b)
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			fail(http.StatusBadRequest, "failed to parse form: "+err.Error())
			return
		}

		switch part.FormName() {
		case "lease_id":
			leaseID = readField(part)
		case "store_path":
			storePath = readField(part)
		case "script_path":
			scriptPath = readField(part)
		case "skip_broadcast":
			skipBroadcast = readField(part)
		case "nar":
			// The NAR is optional if this node already has the closure cached
			// (same store path from a previous task in the fan-out).
			if storePath == "" {
				fail(http.StatusBadRequest, "store_path must precede nar")
				return
			}
			narPath, err = s.narCache.store(storePath, part)
			if err != nil {
				fail(http.StatusInternalServerError, "cache nar: "+err.Error())
				return
			}
		case "workspace_cas":
			// Additive: files the submitter left out because this node's
			// workspace CAS has them (it asked /v1/store/missing first).
			if err := json.NewDecoder(io.LimitReader(part, 64<<20)).Decode(&fromCAS); err != nil {
				fail(http.StatusBadRequest, "workspace_cas: "+err.Error())
				return
			}
		case "workspace":
			haveWorkspace = true
			if err := extractWorkspaceTar(part, workDir, uploaded, s.casDir()); err != nil {
				fail(http.StatusInternalServerError, "tar read: "+err.Error())
				return
			}
		}
		part.Close()
	}

	if storePath == "" || scriptPath == "" {
		fail(http.StatusBadRequest, "store_path and script_path are required")
		return
	}
	if !haveWorkspace {
		fail(http.StatusBadRequest, "workspace file required")
		return
	}
	if err := materializeFromCAS(s.casDir(), workDir, fromCAS, uploaded); err != nil {
		fail(http.StatusConflict, err.Error())
		return
	}
	pruneCAS(s.casDir(), casMaxBytes)
	if narPath == "" {
		if cached, _ := s.narCache.narFileFor(storePath); cached == "" && !pathExists(filepath.Join(storePath, "bin", "run")) {
			fail(http.StatusBadRequest, "nar file required (not cached on this node)")
			return
		}
	}
	// If the closure wasn't cached before, it is now (stored above). Resolve the
	// cached NAR path for this job record.
	if narPath == "" {
		narPath, _ = s.narCache.narFileFor(storePath)
	}

	s.jobsMu.Lock()
	if s.jobs == nil {
		s.jobs = make(map[string]*JobRecord)
	}
	s.jobs[jobID] = &JobRecord{
		JobID:      jobID,
		WorkDir:    workDir,
		NarPath:    narPath,
		StorePath:  storePath,
		ScriptPath: scriptPath,
		Status:     "uploaded",
		CreatedAt:  time.Now(),
		Uploaded:   uploaded,
	}
	s.jobsMu.Unlock()
	s.persist()

	// Fan this closure out to healthy peers so pool spill can split a single
	// task across multiple nodes. Synchronous and best-effort: the first pool
	// dispatch must see peers that already hold the closure, else everything
	// stays local; a peer that fails to import is simply not offered spill
	// work, and local always runs its own part.
	//
	// DDP ranks upload directly to their own node and every rank does the same,
	// so the closure is already where it will run — skip the broadcast and keep
	// it off the other (non-executing) peers.
	var warnings []string
	var seed time.Duration
	if skipBroadcast != "1" {
		seedStart := time.Now()
		warnings = s.broadcastClosure(storePath)
		seed = time.Since(seedStart)
	}

	writeJSON(w, http.StatusOK, UploadResponse{JobID: jobID, StorePath: storePath, NodeID: s.nodeID,
		Warnings: warnings, SeedMs: seed.Milliseconds()})
}

// extractWorkspaceTar unpacks the workspace stream into workDir, refusing
// entries that escape it, and records a stamp per extracted file so results
// can later be limited to what the job actually changed. With casDir set,
// each file is also stored in the workspace CAS so the next upload can skip it.
func extractWorkspaceTar(src io.Reader, workDir string, uploaded map[string]FileStamp, casDir string) error {
	// Sniff rather than require: workspaces are zstd now, were gzip before,
	// and a submitter on an older build still sends them plain. A cluster is
	// rarely upgraded all at once and never in one direction, so reading all
	// three removes the flag day.
	in, closeIn, err := tarcodec.Reader(src)
	if err != nil {
		return err
	}
	defer closeIn()
	tr := tar.NewReader(in)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(workDir, hdr.Name)
		cleanTarget := filepath.Clean(target)
		cleanWork := filepath.Clean(workDir) + string(os.PathSeparator)
		if !strings.HasPrefix(cleanTarget, cleanWork) {
			continue
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(target, os.FileMode(hdr.Mode))
		case tar.TypeReg:
			os.MkdirAll(filepath.Dir(target), 0755)
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				log.Warn().Err(err).Str("file", hdr.Name).Msg("workspace: cannot create file; skipped")
				continue
			}
			var dst io.Writer = f
			var cw *casWriter
			if casDir != "" {
				if cw, err = newCASWriter(casDir); err != nil {
					log.Warn().Err(err).Msg("workspace cache: cannot store; the next upload resends this file")
				} else {
					dst = io.MultiWriter(f, cw)
				}
			}
			_, cerr := io.Copy(dst, tr)
			f.Close()
			if cerr != nil {
				if cw != nil {
					cw.abort()
				}
				return fmt.Errorf("extract %s: %w", hdr.Name, cerr)
			}
			if cw != nil {
				if err := cw.commit(); err != nil {
					log.Warn().Err(err).Str("file", hdr.Name).Msg("workspace cache: cannot store")
				}
			}
			if rel, err := filepath.Rel(workDir, cleanTarget); err == nil {
				if info, err := os.Stat(cleanTarget); err == nil {
					uploaded[filepath.ToSlash(rel)] = stampOf(info)
				}
			}
		}
	}
}
