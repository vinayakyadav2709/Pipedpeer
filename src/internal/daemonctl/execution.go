package daemonctl

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gorilla/websocket"
)

// ExecConfig is sent to the daemon WebSocket to start execution.
type ExecConfig struct {
	ScriptPath string   `json:"script_path"`
	Args       []string `json:"args"`
	Envs       []string `json:"envs"`
	Isolate    bool     `json:"isolate"`
	StorePath  string   `json:"store_path"`
	GPU        bool     `json:"gpu,omitempty"`
	GPUDevices string   `json:"gpu_devices,omitempty"`
	// Intercept enables the sitecustomize shim on the node.
	Intercept bool `json:"intercept,omitempty"`
	// MemLimitBytes caps the sandbox's memory via cgroups. Everything else
	// about memory here is an estimate checked at admission; this is the only
	// part the kernel enforces, and without it a job that outgrows its
	// estimate takes the whole machine down rather than itself.
	MemLimitBytes int64 `json:"mem_limit_bytes,omitempty"`
}

type outputMessage struct {
	O        string `json:"o,omitempty"`
	E        string `json:"e,omitempty"`
	Error    string `json:"error,omitempty"`
	Done     bool   `json:"done,omitempty"`
	ExitCode int    `json:"exit_code"`
	// PeakMemBytes mirrors the daemon's field so the client can learn the
	// job's real footprint and record it for the historical estimation tier.
	PeakMemBytes int64 `json:"peak_mem_bytes,omitempty"`
}

type uploadResponse struct {
	JobID     string `json:"job_id"`
	StorePath string `json:"store_path"`
}

// UploadJob sends workspace tarball + NAR closure to the daemon.
// If the daemon already has the store path cached (a prior task in the fan-out
// shipped the same closure), the NAR is omitted and only the workspace travels.
func UploadJob(host string, port int, workspacePath, narPath, storePath, scriptPath string, skipBroadcast bool) (*uploadResponse, error) {
	// Export the NAR only when the target daemon lacks the closure: a gzip
	// export of a multi-GB store is seconds-to-minutes of wasted work when a
	// shared store (or a prior fan-out task) already gave every node a copy.
	if narPath != "" && !storeCached(host, port, storePath) {
		if err := ExportNAR(storePath, narPath); err != nil {
			return nil, fmt.Errorf("nix store export failed: %v", err)
		}
	}

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer mp.Close()

		addFile(mp, "workspace", "workspace.tar", workspacePath)
		mp.WriteField("store_path", storePath)
		mp.WriteField("script_path", scriptPath)
		if skipBroadcast {
			mp.WriteField("skip_broadcast", "1")
		}

		if narPath != "" && !storeCached(host, port, storePath) {
			addFile(mp, "nar", "closure.nar", narPath)
		}
	}()

	url := fmt.Sprintf("http://%s:%d/v1/jobs/upload", host, port)
	resp, err := http.Post(url, mp.FormDataContentType(), pr)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload rejected: %s", string(body))
	}

	var result uploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("invalid upload response: %w", err)
	}
	return &result, nil
}

// storeCached asks the daemon whether it already has a closure for storePath.
func storeCached(host string, port int, storePath string) bool {
	url := fmt.Sprintf("http://%s:%d/v1/store?path=%s&runnable=1", host, port, url.QueryEscape(storePath))
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var r struct {
		Cached bool `json:"cached"`
	}
	if json.NewDecoder(resp.Body).Decode(&r) != nil {
		return false
	}
	return r.Cached
}

// StreamExecute connects to the daemon WebSocket and streams job output to stdout/stderr.
// Blocks until the job completes or context is cancelled.
// It returns the peak memory the job used (bytes) as reported by the daemon.
func StreamExecute(ctx context.Context, host string, port int, jobID string, cfg ExecConfig) (int64, string, string, error) {
	url := fmt.Sprintf("ws://%s:%d/v1/jobs/%s/exec", host, port, jobID)

	// The websocket dialer does not go through http.DefaultTransport, so the
	// shared secret has to be attached by hand here; without it the upgrade
	// is refused and the job fails with a bare "bad handshake".
	var wsHeaders http.Header
	if tok := authtoken.Current(); tok != "" {
		wsHeaders = http.Header{authtoken.Header: []string{tok}}
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, wsHeaders)
	if err != nil {
		return 0, "", "", fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(cfg); err != nil {
		return 0, "", "", fmt.Errorf("send config: %w", err)
	}

	var peakBytes int64
	stdoutBuf := new(strings.Builder)
	stderrBuf := new(strings.Builder)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return peakBytes, stdoutBuf.String(), stderrBuf.String(), nil
			}
			return 0, stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("connection lost: %w", err)
		}

		var out outputMessage
		if err := json.Unmarshal(msg, &out); err != nil {
			continue
		}

		if out.Error != "" {
			return 0, stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("remote: %s", out.Error)
		}

		fmt.Print(out.O)
		fmt.Fprint(os.Stderr, out.E)
		stdoutBuf.WriteString(out.O)
		stderrBuf.WriteString(out.E)

		if out.Done {
			if out.PeakMemBytes > 0 {
				peakBytes = out.PeakMemBytes
			}
			if out.ExitCode != 0 {
				return peakBytes, stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("remote job exited with code %d", out.ExitCode)
			}
			return peakBytes, stdoutBuf.String(), stderrBuf.String(), nil
		}
	}
}

// DownloadResults fetches output files from the daemon and saves them to outDir.
func DownloadResults(host string, port int, jobID, outDir string) (*ResultManifest, error) {
	url := fmt.Sprintf("http://%s:%d/v1/jobs/%s/results", host, port, jobID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("download results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download rejected: %s", string(body))
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}

	return extractResults(resp.Body, outDir)
}

// ResultManifest records what a job sent back, so a run can report what it
// touched instead of silently overwriting files in the submitter's project.
type ResultManifest struct {
	JobID   string   `json:"job_id,omitempty"`
	OutDir  string   `json:"out_dir"`
	New     []string `json:"new,omitempty"`
	Updated []string `json:"updated,omitempty"`
	Deleted []string `json:"deleted,omitempty"`
}

// deletedManifestName is the tar entry that carries the deletion list. Must
// match the daemon's writer (internal/daemonapi/execution.go).
const deletedManifestName = ".pipedpeer-deleted.json"

// Count is the total number of files received.
func (m *ResultManifest) Count() int {
	if m == nil {
		return 0
	}
	return len(m.New) + len(m.Updated)
}

// extractResults unpacks a results tar into outDir, classifying each entry as
// new or updated relative to what is already on disk. Entries that would
// escape outDir are refused: the archive comes from another machine.
//
// A .pipedpeer-deleted.json entry lists files the job deleted on the node;
// they are removed locally after everything else is extracted. The list is
// computed daemon-side from exactly what this job uploaded, so only job
// artifacts are ever removed.
func extractResults(body io.Reader, outDir string) (*ResultManifest, error) {
	manifest := &ResultManifest{OutDir: outDir}
	root, err := filepath.Abs(outDir)
	if err != nil {
		return nil, err
	}

	// Sniff, so a daemon on an older build that answers uncompressed still
	// works: a cluster is rarely upgraded all at once. Go's http client
	// already unwraps gzip when it negotiated the encoding itself, but this
	// response declares it outright, so handle it here.
	br := bufio.NewReader(body)
	var rd io.Reader = br
	if magic, err := br.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		rd = gz
	}
	tr := tar.NewReader(rd)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return manifest, fmt.Errorf("read results: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if hdr.Name == deletedManifestName {
			payload, rerr := io.ReadAll(tr)
			if rerr != nil {
				return manifest, fmt.Errorf("read deletion manifest: %w", rerr)
			}
			_ = json.Unmarshal(payload, &manifest.Deleted) // empty/garbled = no deletions
			continue
		}

		target := filepath.Join(root, filepath.FromSlash(hdr.Name))
		if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return manifest, fmt.Errorf("refusing result path outside %s: %s", outDir, hdr.Name)
		}

		_, statErr := os.Stat(target)
		existed := statErr == nil

		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return manifest, err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return manifest, err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return manifest, err
		}
		f.Close()

		if existed {
			manifest.Updated = append(manifest.Updated, hdr.Name)
		} else {
			manifest.New = append(manifest.New, hdr.Name)
		}
	}

	for _, rel := range manifest.Deleted {
		target := filepath.Join(root, filepath.FromSlash(rel))
		if target == root || !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return manifest, fmt.Errorf("refusing deletion outside %s: %s", outDir, rel)
		}
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return manifest, err
		}
	}
	return manifest, nil
}

// ExportNAR exports the full runtime closure of a store path to a NAR file.
// Uses the nix multicall binary under the nix-store name (argv[0] set).
func ExportNAR(storePath, destPath string) error {
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return fmt.Errorf("nix not found in PATH")
	}

	query := &exec.Cmd{
		Path: nixPath,
		Args: []string{"nix-store", "-qR", storePath},
	}
	closureOut, err := query.Output()
	if err != nil {
		return fmt.Errorf("nix-store -qR: %w", err)
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

	gz := gzip.NewWriter(f)
	defer gz.Close()

	export := &exec.Cmd{
		Path:   nixPath,
		Args:   append([]string{"nix-store", "--export"}, paths...),
		Stdout: gz,
		Stderr: os.Stderr,
	}
	return export.Run()
}

// CreateWorkspaceTar creates a tarball of the project directory at destPath.
// It writes a sitecustomize.py into .pipedpeer/shim/ inside the archive, so a
// node that enables the interception shim can put that dir on PYTHONPATH and
// Python auto-imports it before the user's first line. shimContent empty means
// no shim is embedded.
func CreateWorkspaceTar(projectDir, destPath string, shimContent string) error {
	ignoreFile := filepath.Join(projectDir, ".pipedpeerignore")

	args := []string{"--exclude=.git", "--exclude=__pycache__", "--exclude=.venv",
		"--exclude=venv", "--exclude=env", "--exclude=node_modules"}

	if content, err := os.ReadFile(ignoreFile); err == nil {
		for _, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				args = append(args, "--exclude="+line)
			}
		}
	}

	args = append(args, "-cf", destPath, "-C", projectDir, ".")
	if err := exec.Command("tar", args...).Run(); err != nil {
		return err
	}

	if shimContent != "" {
		if err := appendShim(destPath, shimContent); err != nil {
			return fmt.Errorf("append shim: %w", err)
		}
	}

	// Compressed last, not at creation: the shim is added with `tar --append`,
	// which cannot open a gzipped archive. A workspace is mostly source and
	// crosses the wire on every run, so a second pass over a file already on
	// disk is worth it.
	return gzipInPlace(destPath)
}

// gzipInPlace rewrites a file as gzip. The reader sniffs the magic bytes, so
// an older daemon still accepts an uncompressed archive and this can land on
// either side of a cluster first.
func gzipInPlace(path string) error {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()
	tmp := path + ".gz"
	dst, err := os.Create(tmp)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(dst)
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err := gz.Close(); err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// appendShim adds .pipedpeer/shim/sitecustomize.py as a member of an existing
// tar archive. It stages the file in a temp dir and uses GNU tar's transform
// to re-home it, so the user's project directory is never touched.
func appendShim(destPath, content string) error {
	stageDir, err := userdir.Scratch("shim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stageDir)

	stage := filepath.Join(stageDir, "sitecustomize.py")
	if err := os.WriteFile(stage, []byte(content), 0755); err != nil {
		return err
	}

	cmd := exec.Command("tar", "--append", "--file="+destPath,
		"--transform=s,.*,.pipedpeer/shim/sitecustomize.py,", stage)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func addFile(mp *multipart.Writer, fieldName, fileName, filePath string) {
	w, err := mp.CreateFormFile(fieldName, fileName)
	if err != nil {
		return
	}
	f, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer f.Close()
	io.Copy(w, f)
}
