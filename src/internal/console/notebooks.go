package console

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	notebookExt      = ".ipynb"
	notebookListCap  = 500
	maxNotebookBytes = 256 << 20
	untitledAttempts = 10000
)

// NotebookEntry is one row of GET /api/notebooks.
type NotebookEntry struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	Modified int64  `json:"modified"`
	Size     int64  `json:"size"`
}

// NotebookPath is the body of a create request and the reply to create and
// rename.
type NotebookPath struct {
	Path string `json:"path"`
}

// NotebookRename is the body of POST /api/notebooks/rename.
type NotebookRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// NotebookSaved is the reply to a content PUT.
type NotebookSaved struct {
	ETag     string `json:"etag"`
	Modified int64  `json:"modified"`
}

// emptyNotebook is what a new notebook file contains. A notebook with no
// cells is valid nbformat 4.5 and opens in Jupyter and Colab.
const emptyNotebook = `{
 "cells": [],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
`

var skippedDirs = map[string]bool{"node_modules": true, ".pipedpeer": true}

// errBadPath marks a notebook path the client must not use. Its message is
// safe to return.
type errBadPath struct{ reason string }

func (e errBadPath) Error() string { return e.reason }

// resolveNotebook maps a client path to an absolute file inside the notebook
// root. Symlinks are followed before the containment check, so a link that
// points outside the root is rejected rather than followed. The file itself
// need not exist; its nearest existing ancestor must be inside the root.
func (s *Server) resolveNotebook(rel string) (string, error) {
	if rel == "" {
		return "", errBadPath{"path is required"}
	}
	if strings.ContainsRune(rel, 0) || strings.Contains(rel, `\`) {
		return "", errBadPath{"path contains invalid characters"}
	}
	if !strings.HasSuffix(rel, notebookExt) {
		return "", errBadPath{"path must end in " + notebookExt}
	}
	if path.IsAbs(rel) {
		return "", errBadPath{"path must be relative"}
	}
	clean := path.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errBadPath{"path escapes the notebook directory"}
	}
	if path.Base(clean) == notebookExt {
		return "", errBadPath{"notebook needs a name"}
	}

	abs := filepath.Join(s.root, filepath.FromSlash(clean))
	resolved, err := evalExistingPrefix(abs)
	if err != nil {
		return "", errBadPath{"path cannot be resolved"}
	}
	if !within(s.root, resolved) {
		return "", errBadPath{"path escapes the notebook directory"}
	}
	return resolved, nil
}

// evalExistingPrefix resolves symlinks in the longest prefix of p that exists
// and re-attaches the remainder, so paths to files not yet created can still
// be checked for containment.
func evalExistingPrefix(p string) (string, error) {
	rest := ""
	for cur := p; ; {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(resolved, rest), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		// A dangling symlink reports not-exist too; following it on write
		// would create a file wherever it points.
		if info, lerr := os.Lstat(cur); lerr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("dangling symlink: %s", cur)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", err
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

func within(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func (s *Server) relPath(abs string) string {
	rel, err := filepath.Rel(s.root, abs)
	if err != nil {
		return filepath.ToSlash(abs)
	}
	return filepath.ToSlash(rel)
}

func writePathError(w http.ResponseWriter, err error) {
	var bad errBadPath
	if errors.As(err, &bad) {
		writeError(w, http.StatusBadRequest, bad.reason)
		return
	}
	writeFileError(w, err)
}

func writeFileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		writeError(w, http.StatusNotFound, "notebook not found")
	case errors.Is(err, fs.ErrExist):
		writeError(w, http.StatusConflict, "a notebook with that path already exists")
	case errors.Is(err, fs.ErrPermission):
		writeError(w, http.StatusForbidden, "permission denied")
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func etagOf(content []byte) string {
	sum := sha256.Sum256(content)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

// setETag writes the header with its registered spelling; Header.Set would
// send "Etag".
func setETag(w http.ResponseWriter, etag string) {
	w.Header()["ETag"] = []string{etag}
}

func (s *Server) handleNotebookList(w http.ResponseWriter, _ *http.Request) {
	entries := []NotebookEntry{}
	err := filepath.WalkDir(s.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if p != s.root && (strings.HasPrefix(name, ".") || skippedDirs[name]) {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || !strings.HasSuffix(name, notebookExt) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		entries = append(entries, NotebookEntry{
			Path:     s.relPath(p),
			Name:     name,
			Modified: info.ModTime().UnixMilli(),
			Size:     info.Size(),
		})
		if len(entries) >= notebookListCap {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleNotebookCreate(w http.ResponseWriter, r *http.Request) {
	var req NotebookPath
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Path == "" {
		created, err := s.createUntitled()
		if err != nil {
			writeFileError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, NotebookPath{Path: created})
		return
	}
	abs, err := s.resolveNotebook(req.Path)
	if err != nil {
		writePathError(w, err)
		return
	}
	if err := createExclusive(abs, []byte(emptyNotebook)); err != nil {
		writeFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, NotebookPath{Path: s.relPath(abs)})
}

func (s *Server) createUntitled() (string, error) {
	for i := 0; i < untitledAttempts; i++ {
		name := "Untitled" + notebookExt
		if i > 0 {
			name = fmt.Sprintf("Untitled%d%s", i, notebookExt)
		}
		err := createExclusive(filepath.Join(s.root, name), []byte(emptyNotebook))
		if err == nil {
			return name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	return "", errors.New("could not find a free Untitled name")
}

// createExclusive writes a new file and fails with fs.ErrExist if anything is
// already at that path, so two creates cannot both win.
func createExclusive(abs string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(abs)
		return err
	}
	return f.Close()
}

func (s *Server) handleNotebookGet(w http.ResponseWriter, r *http.Request) {
	abs, err := s.resolveNotebook(r.URL.Query().Get("path"))
	if err != nil {
		writePathError(w, err)
		return
	}
	content, err := os.ReadFile(abs)
	if err != nil {
		writeFileError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	setETag(w, etagOf(content))
	_, _ = w.Write(content)
}

func (s *Server) handleNotebookPut(w http.ResponseWriter, r *http.Request) {
	abs, err := s.resolveNotebook(r.URL.Query().Get("path"))
	if err != nil {
		writePathError(w, err)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxNotebookBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "notebook is too large")
		return
	}
	if !json.Valid(body) {
		writeError(w, http.StatusBadRequest, "body is not valid JSON")
		return
	}
	if match := r.Header.Get("If-Match"); match != "" && match != "*" {
		current, err := os.ReadFile(abs)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			writeFileError(w, err)
			return
		}
		if err != nil || etagOf(current) != match {
			writeError(w, http.StatusPreconditionFailed, "notebook changed on disk since it was loaded")
			return
		}
	}
	if err := writeAtomic(abs, body); err != nil {
		writeFileError(w, err)
		return
	}
	info, err := os.Stat(abs)
	if err != nil {
		writeFileError(w, err)
		return
	}
	etag := etagOf(body)
	setETag(w, etag)
	writeJSON(w, http.StatusOK, NotebookSaved{ETag: etag, Modified: info.ModTime().UnixMilli()})
}

// writeAtomic replaces abs with content via a temp file in the same directory
// and a rename, so a reader never sees a half-written notebook.
func writeAtomic(abs string, content []byte) error {
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if info, err := os.Stat(abs); err == nil {
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if _, err := tmp.Write(content); err != nil {
		return cleanup(err)
	}
	if err := tmp.Chmod(mode); err != nil {
		return cleanup(err)
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *Server) handleNotebookDelete(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	abs, err := s.resolveNotebook(rel)
	if err != nil {
		writePathError(w, err)
		return
	}
	if err := os.Remove(abs); err != nil {
		writeFileError(w, err)
		return
	}
	s.kernels.shutdown(s.relPath(abs))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNotebookRename(w http.ResponseWriter, r *http.Request) {
	var req NotebookRename
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	from, err := s.resolveNotebook(req.From)
	if err != nil {
		writePathError(w, err)
		return
	}
	to, err := s.resolveNotebook(req.To)
	if err != nil {
		writePathError(w, err)
		return
	}
	if _, err := os.Stat(from); err != nil {
		writeFileError(w, err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		writeFileError(w, err)
		return
	}
	// Link then unlink instead of rename: rename silently replaces an
	// existing target, and a check-then-rename would race with a create.
	if err := os.Link(from, to); err != nil {
		writeFileError(w, err)
		return
	}
	if err := os.Remove(from); err != nil {
		os.Remove(to)
		writeFileError(w, err)
		return
	}
	s.kernels.rename(s.relPath(from), s.relPath(to))
	writeJSON(w, http.StatusOK, NotebookPath{Path: s.relPath(to)})
}

func decodeBody(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}
