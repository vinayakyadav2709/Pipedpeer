package daemonapi

import (
	"archive/tar"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// ExecConfig is sent by the CLI as the first WebSocket message.
type ExecConfig struct {
	ScriptPath string   `json:"script_path"`
	Args       []string `json:"args"`
	Envs       []string `json:"envs"`
	Isolate    bool     `json:"isolate"`
	StorePath  string   `json:"store_path"`
}

// OutputMessage is streamed from daemon to CLI during execution.
type OutputMessage struct {
	O        string `json:"o,omitempty"`
	E        string `json:"e,omitempty"`
	Error    string `json:"error,omitempty"`
	Done     bool   `json:"done,omitempty"`
	ExitCode int    `json:"exit_code"`
}

// JobRecord tracks a single uploaded job on the daemon.
type JobRecord struct {
	JobID      string
	WorkDir    string
	NarPath    string
	StorePath  string
	ScriptPath string
	Status     string
	CreatedAt  time.Time
}

// UploadResponse is returned by POST /v1/jobs/upload.
type UploadResponse struct {
	JobID     string `json:"job_id"`
	StorePath string `json:"store_path"`
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func defaultJobDir() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "jobs")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "pipedpeer", "jobs")
	}
	return filepath.Join(home, ".local", "share", "pipedpeer", "jobs")
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func buildRunCmd(runPath, workDir string, isolate bool, scriptRelPath string, scriptArgs, envs []string) string {
	if !isolate {
		cmd := "mkdir -p " + shellQuote(workDir) + " && cd " + shellQuote(workDir) + " && " +
			shellQuote(runPath) + " " + shellQuote(scriptRelPath)
		for _, arg := range scriptArgs {
			cmd += " " + shellQuote(arg)
		}
		for _, env := range envs {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				cmd = "export " + parts[0] + "=" + shellQuote(parts[1]) + " && " + cmd
			} else {
				cmd = "export " + parts[0] + "=" + shellQuote(os.Getenv(parts[0])) + " && " + cmd
			}
		}
		return cmd
	}

	homeDir := filepath.Join(filepath.Dir(workDir), "home")
	pathEnv := "/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin"

	bwrapArgs := []string{
		"bwrap",
		"--die-with-parent",
		"--unshare-pid",
		"--unshare-ipc",
		"--unshare-uts",
		"--ro-bind", "/nix", "/nix",
		"--dev", "/dev",
		"--proc", "/proc",
		"--tmpfs", "/tmp",
		"--bind", workDir, "/work",
		"--bind", homeDir, "/home/root",
		"--chdir", "/work",
		"--setenv", "HOME", "/home/root",
		"--setenv", "PATH", pathEnv,
	}

	for _, env := range envs {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			bwrapArgs = append(bwrapArgs, "--setenv", parts[0], parts[1])
		} else {
			bwrapArgs = append(bwrapArgs, "--setenv", parts[0], os.Getenv(parts[0]))
		}
	}

	bwrapArgs = append(bwrapArgs, "--", runPath, scriptRelPath)
	bwrapArgs = append(bwrapArgs, scriptArgs...)

	return "mkdir -p " + shellQuote(workDir) + " " + shellQuote(homeDir) + " && " + strings.Join(bwrapArgs, " ")
}

// handleJobUpload processes POST /v1/jobs/upload.
func (s *Server) handleJobUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
		return
	}

	storePath := r.FormValue("store_path")
	scriptPath := r.FormValue("script_path")
	if storePath == "" || scriptPath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store_path and script_path are required"})
		return
	}

	workspaceFile, _, err := r.FormFile("workspace")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "workspace file required: " + err.Error()})
		return
	}
	defer workspaceFile.Close()

	narFile, _, err := r.FormFile("nar")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "nar file required: " + err.Error()})
		return
	}
	defer narFile.Close()

	jobID := generateLeaseID()
	jobDir := filepath.Join(s.jobDir, jobID)
	workDir := filepath.Join(jobDir, "work")

	if err := os.MkdirAll(workDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "mkdir: " + err.Error()})
		return
	}

	tr := tar.NewReader(workspaceFile)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "tar read: " + err.Error()})
			return
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
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				continue
			}
			io.Copy(f, tr)
			f.Close()
		}
	}

	narPath := filepath.Join(jobDir, "closure.nar")
	narDest, err := os.Create(narPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "save nar: " + err.Error()})
		return
	}
	io.Copy(narDest, narFile)
	narDest.Close()

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
	}
	s.jobsMu.Unlock()

	writeJSON(w, http.StatusOK, UploadResponse{JobID: jobID, StorePath: storePath})
}

// handleJobExec handles WebSocket GET /v1/jobs/{id}/exec.
func (s *Server) handleJobExec(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.jobsMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if job.Status != "uploaded" {
		s.jobsMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("job status is %s, expected uploaded", job.Status)})
		return
	}
	job.Status = "importing"
	s.jobsMu.Unlock()

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return
	}

	var cfg ExecConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		conn.WriteJSON(OutputMessage{Error: "invalid config: " + err.Error()})
		return
	}

	writeWS := func(m OutputMessage) {
		data, _ := json.Marshal(m)
		conn.WriteMessage(websocket.TextMessage, data)
	}

	// Import NAR into local Nix store
	narFile, err := os.Open(job.NarPath)
	if err != nil {
		writeWS(OutputMessage{Error: "open nar: " + err.Error()})
		s.jobsMu.Lock()
		job.Status = "failed"
		s.jobsMu.Unlock()
		return
	}
	defer narFile.Close()

	// Import NAR into local Nix store — uses nix multicall binary
	// invoked as nix-store via argv[0].
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		writeWS(OutputMessage{Error: "nix not found"})
		s.jobsMu.Lock()
		job.Status = "failed"
		s.jobsMu.Unlock()
		return
	}

	importCmd := &exec.Cmd{
		Path:   nixPath,
		Args:   []string{"nix-store", "--import"},
		Stdin:  narFile,
		Stdout: nil,
		Stderr: nil,
	}
	out, err := importCmd.CombinedOutput()
	if err != nil {
		writeWS(OutputMessage{Error: fmt.Sprintf("nix-store --import: %v: %s", err, string(out))})
		s.jobsMu.Lock()
		job.Status = "failed"
		s.jobsMu.Unlock()
		return
	}

	s.jobsMu.Lock()
	job.Status = "running"
	s.jobsMu.Unlock()

	runPath := filepath.Join(cfg.StorePath, "bin", "run")
	runCmd := buildRunCmd(runPath, job.WorkDir, cfg.Isolate, cfg.ScriptPath, cfg.Args, cfg.Envs)

	cmd := exec.Command("sh", "-c", runCmd)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		writeWS(OutputMessage{Error: "start command: " + err.Error()})
		s.jobsMu.Lock()
		job.Status = "failed"
		s.jobsMu.Unlock()
		return
	}

	outCh := make(chan OutputMessage, 64)
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		for m := range outCh {
			data, _ := json.Marshal(m)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			outCh <- OutputMessage{O: scanner.Text() + "\n"}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			outCh <- OutputMessage{E: scanner.Text() + "\n"}
		}
	}()

	cmdDone := make(chan struct{})
	go func() {
		cmd.Wait()
		close(cmdDone)
	}()

	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if cmd.Process != nil {
					cmd.Process.Kill()
				}
				return
			}
		}
	}()

	<-cmdDone

	exitCode := cmd.ProcessState.ExitCode()

	wg.Wait()

	outCh <- OutputMessage{Done: true, ExitCode: exitCode}
	close(outCh)
	<-writerDone
	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))

	s.jobsMu.Lock()
	if exitCode == 0 {
		job.Status = "done"
	} else {
		job.Status = "failed"
	}
	s.jobsMu.Unlock()
}

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

	cmd := exec.Command("tar", "-C", job.WorkDir, "-cf", "-", ".")
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return
	}
}
