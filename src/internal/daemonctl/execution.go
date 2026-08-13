package daemonctl

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
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
}

type outputMessage struct {
	O        string `json:"o,omitempty"`
	E        string `json:"e,omitempty"`
	Error    string `json:"error,omitempty"`
	Done     bool   `json:"done,omitempty"`
	ExitCode int    `json:"exit_code"`
}

type uploadResponse struct {
	JobID     string `json:"job_id"`
	StorePath string `json:"store_path"`
}

// UploadJob sends workspace tarball + NAR closure to the daemon.
func UploadJob(host string, port int, workspacePath, narPath, storePath, scriptPath string) (*uploadResponse, error) {
	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer mp.Close()

		addFile(mp, "workspace", "workspace.tar", workspacePath)
		addFile(mp, "nar", "closure.nar", narPath)
		mp.WriteField("store_path", storePath)
		mp.WriteField("script_path", scriptPath)
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

// StreamExecute connects to the daemon WebSocket and streams job output to stdout/stderr.
// Blocks until the job completes or context is cancelled.
func StreamExecute(ctx context.Context, host string, port int, jobID string, cfg ExecConfig) error {
	url := fmt.Sprintf("ws://%s:%d/v1/jobs/%s/exec", host, port, jobID)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(cfg); err != nil {
		return fmt.Errorf("send config: %w", err)
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			return fmt.Errorf("connection lost: %w", err)
		}

		var out outputMessage
		if err := json.Unmarshal(msg, &out); err != nil {
			continue
		}

		if out.Error != "" {
			return fmt.Errorf("remote: %s", out.Error)
		}

		fmt.Print(out.O)
		fmt.Fprint(os.Stderr, out.E)

		if out.Done {
			if out.ExitCode != 0 {
				return fmt.Errorf("remote job exited with code %d", out.ExitCode)
			}
			return nil
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
}

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
func extractResults(body io.Reader, outDir string) (*ResultManifest, error) {
	manifest := &ResultManifest{OutDir: outDir}
	root, err := filepath.Abs(outDir)
	if err != nil {
		return nil, err
	}

	tr := tar.NewReader(body)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return manifest, nil
		}
		if err != nil {
			return manifest, fmt.Errorf("read results: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
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

	export := &exec.Cmd{
		Path:   nixPath,
		Args:   append([]string{"nix-store", "--export"}, paths...),
		Stdout: f,
		Stderr: os.Stderr,
	}
	return export.Run()
}

// CreateWorkspaceTar creates a tarball of the project directory at destPath.
func CreateWorkspaceTar(projectDir, destPath string) error {
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
	cmd := exec.Command("tar", args...)
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
