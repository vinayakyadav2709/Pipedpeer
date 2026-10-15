package daemonctl

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/say"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// ExportNAR exports the full runtime closure of a store path to a NAR file.
// Uses the nix multicall binary under the nix-store name (argv[0] set).
func ExportNAR(ctx context.Context, storePath, destPath string) error {
	query, qcleanup, err := nixstore.CmdContext(ctx, "", "nix-store", "-qR", storePath)
	if err != nil {
		return err
	}
	defer qcleanup()
	var qerr bytes.Buffer
	query.Stderr = &qerr
	closureOut, err := query.Output()
	if err != nil {
		// Carry nix's own words: "exit status 1" alone sends you looking in
		// the wrong place.
		return fmt.Errorf("nix-store -qR %s: %w: %s", storePath, err, strings.TrimSpace(qerr.String()))
	}

	paths := strings.Fields(string(closureOut))
	if len(paths) == 0 {
		return fmt.Errorf("empty closure for %s", storePath)
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := tarcodec.Writer(f)
	if err != nil {
		return err
	}

	// Signed before it leaves, so the far side can require a signature
	// instead of being configured to stop asking for one.
	if err := nixsign.EnsureSigned(ctx, paths); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: could not sign the closure (%v); the node may refuse it\n", err)
	}
	export, ecleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store", "--export"}, paths...)...)
	if err != nil {
		return err
	}
	defer ecleanup()
	export.Stdout = gz
	export.Stderr = os.Stderr
	if err := export.Run(); err != nil {
		gz.Close()
		return err
	}
	return gz.Close()
}

// ignoreExcludes turns one .pipedpeerignore line into tar exclude flags.
//
// The file is written gitignore-style, and tar does not read gitignore. A
// directory written "data/" never matched, because tar compares the pattern
// against member names, which carry no trailing slash - so every "dir/" line
// excluded nothing, `pipedpeer init`'s template included, and the demo's
// 1.1 GB CSV went into the upload of every script beside it. A leading slash
// anchors the pattern at the project root, as in gitignore; tar needs that
// spelled as the member name, "./data", and matched anchored.
func ignoreExcludes(line string) []string {
	if line == "" || strings.HasPrefix(line, "#") {
		return nil
	}
	line = strings.TrimRight(line, "/")
	if line == "" {
		return nil
	}
	if strings.HasPrefix(line, "/") {
		return []string{"--anchored", "--exclude=." + line, "--no-anchored"}
	}
	return []string{"--exclude=" + line}
}

// CreateWorkspaceTar creates a tarball of the project directory at destPath.
// It adds the shim's files (sitecustomize.py and its package) under
// .pipedpeer/shim/ inside the archive, so a node that enables the
// interception shim can put that dir on PYTHONPATH and Python auto-imports it
// before the user's first line. A nil shim means no shim is embedded.
func CreateWorkspaceTar(projectDir, destPath string, shim fs.FS) error {
	ignoreFile := filepath.Join(projectDir, ".pipedpeerignore")

	args := []string{"--exclude=.git", "--exclude=__pycache__", "--exclude=.venv",
		"--exclude=venv", "--exclude=env", "--exclude=node_modules"}

	if content, err := os.ReadFile(ignoreFile); err == nil {
		for _, line := range strings.Split(string(content), "\n") {
			args = append(args, ignoreExcludes(strings.TrimSpace(line))...)
		}
	}

	args = append(args, "-cf", destPath, "-C", projectDir, ".")
	if err := termuxexec.Command("tar", args...).Run(); err != nil {
		return err
	}

	if shim != nil {
		if err := appendShim(destPath, shim); err != nil {
			return fmt.Errorf("append shim: %w", err)
		}
	}

	// Compressed last, not at creation: the shim is added with `tar --append`,
	// which cannot open a compressed archive. A workspace is mostly source
	// and crosses the wire on every run, so a second pass over a file already
	// on disk is worth it.
	return tarcodec.CompressFile(destPath, tarcodec.CompressFloor)
}

// appendShim adds the shim's files as .pipedpeer/shim/... members of an
// existing tar archive. It stages them in a temp dir, so the user's project
// directory is never touched.
func appendShim(destPath string, shim fs.FS) error {
	stageDir, err := userdir.Scratch("shim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stageDir)

	if err := os.CopyFS(filepath.Join(stageDir, ".pipedpeer", "shim"), shim); err != nil {
		return err
	}

	cmd := termuxexec.Command("tar", "--append", "--file="+destPath, "-C", stageDir, ".pipedpeer")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// addFile copies a file into the form. wrap, if set, wraps the file reader
// (a progress bar) and returns a func to call once the copy ends.
func addFile(mp *multipart.Writer, fieldName, fileName, filePath string, wrap func(io.Reader) (io.Reader, func())) error {
	w, err := mp.CreateFormFile(fieldName, fileName)
	if err != nil {
		return fmt.Errorf("form field %s: %w", fieldName, err)
	}
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", fieldName, err)
	}
	defer f.Close()
	var r io.Reader = f
	if wrap != nil {
		var done func()
		r, done = wrap(f)
		defer done()
	}
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("send %s: %w", fieldName, err)
	}
	return nil
}

// humanBytes renders a byte count the way the --mem flag accepts it.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0fM", float64(n)/float64(1<<20))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// casEntry is a workspace file left out of the upload because the node's
// workspace CAS already holds it (wire twin of daemonapi's casEntry).
type casEntry struct {
	Name string `json:"name"`
	SHA  string `json:"sha256"`
	Mode int64  `json:"mode"`
}

// workspaceHashes hashes every regular file in a workspace archive, in
// archive order.
// ponytail: re-hashes from the archive each run; add the (path, size, mtime,
// inode) -> sha256 index under userdir.Data() if hashing shows up in profiles.
func workspaceHashes(tarPath string) ([]string, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	in, closeIn, err := tarcodec.Reader(f)
	if err != nil {
		return nil, err
	}
	defer closeIn()
	tr := tar.NewReader(in)
	var out []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		h := sha256.New()
		if _, err := io.Copy(h, tr); err != nil {
			return nil, err
		}
		out = append(out, hex.EncodeToString(h.Sum(nil)))
	}
}

// missingFiles asks the node which of these file hashes its workspace CAS
// lacks. ok is false when the node does not understand the question (an older
// build answers without missing_files) or cannot be asked.
func missingFiles(host string, port int, shas []string) (map[string]bool, bool) {
	body, err := json.Marshal(map[string]any{"paths": []string{}, "files": shas})
	if err != nil {
		return nil, false
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(fmt.Sprintf("http://%s:%d/v1/store/missing", host, port), "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: workspace check on %s:%d failed (%v); sending every file\n", host, port, err)
		return nil, false
	}
	defer resp.Body.Close()
	var out struct {
		MissingFiles *[]string `json:"missing_files"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil || out.MissingFiles == nil {
		return nil, false
	}
	m := make(map[string]bool, len(*out.MissingFiles))
	for _, h := range *out.MissingFiles {
		m[h] = true
	}
	return m, true
}

// writeDeltaTar copies the workspace archive at src to dst, leaving out the
// regular files whose hash (shas, in archive order) the node already holds,
// and returns those as entries for the node to fill from its CAS.
func writeDeltaTar(src, dst string, shas []string, missing map[string]bool) ([]casEntry, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	in, closeIn, err := tarcodec.Reader(f)
	if err != nil {
		return nil, err
	}
	defer closeIn()
	out, err := os.Create(dst)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	zw, err := tarcodec.Writer(out)
	if err != nil {
		return nil, err
	}
	tr, tw := tar.NewReader(in), tar.NewWriter(zw)
	var skipped []casEntry
	i := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag == tar.TypeReg {
			if i >= len(shas) {
				return nil, fmt.Errorf("workspace archive changed while uploading")
			}
			sha := shas[i]
			i++
			if !missing[sha] {
				skipped = append(skipped, casEntry{Name: hdr.Name, SHA: sha, Mode: hdr.Mode})
				continue
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return skipped, out.Close()
}

// workspaceDelta prepares the smallest workspace upload the node accepts: the
// archive to send and the files the node fills from its CAS. Any failure
// falls back to the whole archive, with a warning. cleanup removes the delta.
func workspaceDelta(host string, port int, workspacePath string) (send string, fromCAS []casEntry, cleanup func()) {
	cleanup = func() {}
	shas, err := workspaceHashes(workspacePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: cannot hash the workspace (%v); sending every file\n", err)
		return workspacePath, nil, cleanup
	}
	missing, ok := missingFiles(host, port, shas)
	if !ok {
		return workspacePath, nil, cleanup
	}
	if len(missing) >= len(shas) {
		return workspacePath, nil, cleanup
	}
	dir, err := userdir.Scratch("wsdelta-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: no scratch dir for the workspace delta (%v); sending every file\n", err)
		return workspacePath, nil, cleanup
	}
	cleanup = func() { os.RemoveAll(dir) }
	dst := filepath.Join(dir, "workspace.tar")
	skipped, err := writeDeltaTar(workspacePath, dst, shas, missing)
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: workspace delta failed (%v); sending every file\n", err)
		return workspacePath, nil, cleanup
	}
	say.Printf("      Workspace: sending %d of %d files (the node has the rest)\n", len(shas)-len(skipped), len(shas))
	return dst, skipped, cleanup
}
