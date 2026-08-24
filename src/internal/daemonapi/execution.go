package daemonapi

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
)

// ExecConfig is sent by the CLI as the first WebSocket message.
type ExecConfig struct {
	ScriptPath string   `json:"script_path"`
	Args       []string `json:"args"`
	Envs       []string `json:"envs"`
	Isolate    bool     `json:"isolate"`
	StorePath  string   `json:"store_path"`
	GPU        bool     `json:"gpu,omitempty"`
	GPUDevices string   `json:"gpu_devices,omitempty"` // e.g. "0" or "0,1" or "all"
	// Intercept enables the sitecustomize shim: PYTHONPATH gains the workspace's
	// .pipedpeer/shim dir and the shim's envs are injected.
	Intercept bool `json:"intercept,omitempty"`
}

// OCI config structures for crun bundle generation.
type ociConfig struct {
	OciVersion string     `json:"ociVersion"`
	Process    ociProcess `json:"process"`
	Root       ociRoot    `json:"root"`
	Hostname   string     `json:"hostname"`
	Mounts     []ociMount `json:"mounts"`
	Linux      *ociLinux  `json:"linux,omitempty"`
}
type ociProcess struct {
	Terminal bool     `json:"terminal"`
	User     ociUser  `json:"user"`
	Args     []string `json:"args"`
	Env      []string `json:"env"`
	Cwd      string   `json:"cwd"`
}
type ociUser struct {
	UID int `json:"uid"`
	GID int `json:"gid"`
}
type ociRoot struct {
	Path     string `json:"path"`
	Readonly bool   `json:"readonly"`
}
type ociMount struct {
	Destination string   `json:"destination"`
	Type        string   `json:"type"`
	Source      string   `json:"source"`
	Options     []string `json:"options,omitempty"`
}
type ociLinux struct {
	Namespaces []ociNamespace `json:"namespaces"`
	Devices    []ociDevice    `json:"devices,omitempty"`
}
type ociNamespace struct {
	Type string `json:"type"`
}
type ociDevice struct {
	Path        string `json:"path"`
	Type        string `json:"type"`
	Major       int64  `json:"major"`
	Minor       int64  `json:"minor"`
	FileMode    int    `json:"fileMode,omitempty"`
	Permissions string `json:"permissions,omitempty"`
}

// OutputMessage is streamed from daemon to CLI during execution.
type OutputMessage struct {
	O        string `json:"o,omitempty"`
	E        string `json:"e,omitempty"`
	Error    string `json:"error,omitempty"`
	Done     bool   `json:"done,omitempty"`
	ExitCode int    `json:"exit_code"`
	// PeakMemBytes is the largest RSS (process tree, bytes) seen while running.
	// It is set on the Done frame so the submitter can learn a job's real
	// footprint and feed the historical estimation tier.
	PeakMemBytes int64 `json:"peak_mem_bytes,omitempty"`
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
	// Uploaded stamps every file the submitter sent, keyed by path relative to
	// WorkDir. Results are diffed against it so only what the job actually
	// produced or changed travels back.
	Uploaded map[string]FileStamp
}

// FileStamp identifies a file version well enough to tell whether a job
// rewrote it. Size plus modification time is what tar itself preserves.
type FileStamp struct {
	Size    int64
	ModTime time.Time
}

func stampOf(info os.FileInfo) FileStamp {
	return FileStamp{Size: info.Size(), ModTime: info.ModTime()}
}

// changed reports whether the file on disk differs from what was uploaded.
func (f FileStamp) changed(other FileStamp) bool {
	return f.Size != other.Size || !f.ModTime.Equal(other.ModTime)
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

// pathExists reports whether a path exists on disk. Used to short-circuit NAR
// re-imports when a prior task already materialised the closure.
func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// envsContain reports whether any env entry in envs starts with the given
// key= prefix, so the daemon never clobbers a submitter-provided variable.
func envsContain(envs []string, prefix string) bool {
	for _, e := range envs {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

// gpuDriverLibDir returns the directory holding the GPU driver's shared
// libraries (libcuda.so.1), or "" if none is found. The container runtime
// mounts them at a host-specific path; this probes the common ones.
func gpuDriverLibDir() string {
	candidates := []string{"/usr/lib64", "/usr/lib/x86_64-linux-gnu", "/usr/local/nvidia/lib64"}
	for _, dir := range candidates {
		if _, err := os.Stat(filepath.Join(dir, "libcuda.so.1")); err == nil {
			return dir
		}
	}
	return ""
}

func buildNonIsolatedCmd(runPath, workDir string, scriptRelPath string, scriptArgs, envs []string) string {
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

	// The NAR is optional if this node already has the closure cached (same
	// store path from a previous task in the fan-out). When present, cache it.
	var narPath string
	if narFile, _, err := r.FormFile("nar"); err == nil {
		defer narFile.Close()
		narPath, err = s.narCache.store(storePath, narFile)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "cache nar: " + err.Error()})
			return
		}
	} else if cached, _ := s.narCache.narFileFor(storePath); cached == "" && !pathExists(filepath.Join(storePath, "bin", "run")) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "nar file required (not cached on this node)"})
		return
	}

	jobID := generateLeaseID()
	jobDir := filepath.Join(s.jobDir, jobID)
	workDir := filepath.Join(jobDir, "work")

	if err := os.MkdirAll(workDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "mkdir: " + err.Error()})
		return
	}

	uploaded := make(map[string]FileStamp)
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
			if rel, err := filepath.Rel(workDir, cleanTarget); err == nil {
				if info, err := os.Stat(cleanTarget); err == nil {
					uploaded[filepath.ToSlash(rel)] = stampOf(info)
				}
			}
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
	if r.FormValue("skip_broadcast") != "1" {
		s.broadcastClosure(storePath)
	}

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

	// If the closure's run entrypoint already exists in the local store, a
	// prior task (or a shared /nix/store volume) already materialised it —
	// skip the (expensive) NAR open + re-import entirely.
	runEntry := filepath.Join(job.StorePath, "bin", "run")
	if !pathExists(runEntry) {
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

		out, err := importNAR(nixPath, narFile)
		if err != nil {
			writeWS(OutputMessage{Error: fmt.Sprintf("nix-store --import: %v: %s", err, string(out))})
			s.jobsMu.Lock()
			job.Status = "failed"
			s.jobsMu.Unlock()
			return
		}
	}

	s.jobsMu.Lock()
	job.Status = "running"
	s.jobsMu.Unlock()

	runPath := filepath.Join(cfg.StorePath, "bin", "run")

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

	var exitCode int
	var peakBytes int64

	// Driver libs for the GPU (libcuda.so.1 et al) are mounted by the container
	// runtime into a host-specific dir (/usr/lib64 on Fedora hosts, etc.) that
	// may not be in the job's ld search path. torch dlopens them at runtime, so
	// put the dir on LD_LIBRARY_PATH for GPU jobs (the run wrapper prepends its
	// nix libs, so this is additive).
	if cfg.GPU {
		if dir := gpuDriverLibDir(); dir != "" && !envsContain(cfg.Envs, "LD_LIBRARY_PATH=") {
			cfg.Envs = append(cfg.Envs, "LD_LIBRARY_PATH="+dir+os.Getenv("LD_LIBRARY_PATH"))
		}
	}

	// Interception: put the workspace shim on PYTHONPATH and enable it. The
	// isolated path rebuilds env from scratch below, so this only feeds the
	// non-isolated branch; both honour cfg.Intercept. The --intercept flag
	// is gone (always on); a job can still opt out per run by passing
	// PIPEDPEER_SHIM=0 in its own Envs.
	if cfg.Intercept && !envsContain(cfg.Envs, "PIPEDPEER_SHIM=0") {
		cfg.Envs = append(cfg.Envs,
			"PYTHONPATH="+filepath.Join(job.WorkDir, ".pipedpeer", "shim"),
			"PIPEDPEER_SHIM=1",
			fmt.Sprintf("PIPEDPEER_DAEMON_URL=http://127.0.0.1:%d", s.selfPort),
			"PIPEDPEER_STORE_PATH="+cfg.StorePath,
			"PIPEDPEER_NODE_ID="+s.nodeID,
		)
		// The shim gates remote spill on PIPEDPEER_NUM_SHARDS != "0": the
		// number of nodes that share this closure (1 at first exec, peers
		// count once broadcastClosure has run). DDP runs set their own
		// RANK/WORLD_SIZE/MASTER_ADDR/PEERS via cfg.Envs and must not be
		// overridden here.
		if !envsContain(cfg.Envs, "PIPEDPEER_NUM_SHARDS=") {
			cfg.Envs = append(cfg.Envs,
				fmt.Sprintf("PIPEDPEER_NUM_SHARDS=%d", s.pool.spillPeerCount(cfg.StorePath)+1))
		}
	}

	// Isolation needs crun. If setup never managed to install it, degrade to
	// unisolated execution with a loud warning instead of failing the job —
	// the sandbox is a hardening layer, not a functional requirement.
	if cfg.Isolate {
		if _, err := exec.LookPath("crun"); err != nil {
			outCh <- OutputMessage{E: "[pipedpeer] crun not found on this node — running unisolated\n"}
			cfg.Isolate = false
		}
	}

	if !cfg.Isolate {
		runCmd := buildNonIsolatedCmd(runPath, job.WorkDir, cfg.ScriptPath, cfg.Args, cfg.Envs)
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

		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				outCh <- OutputMessage{O: scanner.Text() + "\n"}
			}
			if err := scanner.Err(); err != nil {
				outCh <- OutputMessage{Error: "stdout scanner: " + err.Error()}
			}
		}()
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				outCh <- OutputMessage{E: scanner.Text() + "\n"}
			}
			if err := scanner.Err(); err != nil {
				outCh <- OutputMessage{Error: "stderr scanner: " + err.Error()}
			}
		}()
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond)
		cmd.Wait()
		peakBytes = tracker.stop()
		exitCode = cmd.ProcessState.ExitCode()
	} else {
		bundleDir := filepath.Join(filepath.Dir(job.WorkDir), "oci-bundle")
		rootfsDir := filepath.Join(bundleDir, "rootfs")
		os.MkdirAll(filepath.Join(rootfsDir, "dev"), 0755)
		os.MkdirAll(filepath.Join(rootfsDir, "proc"), 0755)
		os.MkdirAll(filepath.Join(rootfsDir, "sys"), 0755)

		args := append([]string{runPath, cfg.ScriptPath}, cfg.Args...)
		env := []string{
			"HOME=/home/root",
			"PATH=/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin",
		}
		_ = cfg.Intercept // shim envs already in cfg.Envs above
		for _, e := range cfg.Envs {
			parts := strings.SplitN(e, "=", 2)
			if len(parts) == 2 {
				// Inside the sandbox the workdir is mounted at /work; rewrite
				// any host-path env (PYTHONPATH for the shim) to match.
				if strings.HasPrefix(parts[1], job.WorkDir) {
					e = parts[0] + "=/work" + strings.TrimPrefix(parts[1], job.WorkDir)
				}
				env = append(env, e)
			} else {
				env = append(env, parts[0]+"="+os.Getenv(parts[0]))
			}
		}

		homeDir := filepath.Join(filepath.Dir(job.WorkDir), "home")
		os.MkdirAll(homeDir, 0755)

		ociCfg := ociConfig{
			OciVersion: "1.0.2",
			Process: ociProcess{
				Terminal: false,
				User:     ociUser{UID: 0, GID: 0},
				Args:     args,
				Env:      env,
				Cwd:      "/work",
			},
			Root:     ociRoot{Path: "rootfs", Readonly: true},
			Hostname: "pipedpeer",
			Mounts: []ociMount{
				{Destination: "/nix", Type: "bind", Source: "/nix", Options: []string{"rbind", "ro"}},
				{Destination: "/proc", Type: "proc", Source: "proc"},
				{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=755"}},
				// Python multiprocessing creates its SemLock in /dev/shm and
				// mmaps it (POSIX shm), which needs a writable, executable tmpfs
				// — the plain /dev tmpfs can't serve both. A dedicated /dev/shm
				// with sticky mode 1777 is what a stock Linux distro ships.
				{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"rw", "nosuid", "nodev", "mode=1777"}},
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev"}},
				{Destination: "/work", Type: "bind", Source: job.WorkDir, Options: []string{"rbind", "rw"}},
				{Destination: "/home/root", Type: "bind", Source: homeDir, Options: []string{"rbind", "rw"}},
			},
			Linux: &ociLinux{
				Namespaces: []ociNamespace{
					{Type: "pid"}, {Type: "ipc"}, {Type: "uts"}, {Type: "mount"},
				},
			},
		}

		if cfg.GPU {
			gpuInfo := gpu.Detect()
			for _, d := range gpuInfo.Devices {
				ociCfg.Linux.Devices = append(ociCfg.Linux.Devices, ociDevice{
					Path: d.Path, Type: "c",
					Major: d.Major, Minor: d.Minor,
					Permissions: "rwm",
				})
			}
			// Determine which GPU devices to expose
			gpuDevices := "all"
			if cfg.GPUDevices != "" {
				gpuDevices = cfg.GPUDevices
			} else if gpuInfo.Count > 1 {
				// With multiple GPUs, use the one the lease reserved for this task
				// (cfg.GPUDevices is set by the coordinator from the lease); no
				// reservation means the GPU with most free VRAM.
				if gpuDevices == "all" {
					devices := gpu.PerDevice()
					bestIdx := -1
					var bestFree int64
					for _, d := range devices {
						if d.MemoryFreeBytes > bestFree {
							bestFree = d.MemoryFreeBytes
							bestIdx = d.Index
						}
					}
					if bestIdx >= 0 {
						gpuDevices = strconv.Itoa(bestIdx)
					}
				}
			}
			switch gpuInfo.Vendor {
			case gpu.VendorNVIDIA:
				ociCfg.Process.Env = append(ociCfg.Process.Env,
					fmt.Sprintf("NVIDIA_VISIBLE_DEVICES=%s", gpuDevices),
					"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
				)
			case gpu.VendorAMD:
				ociCfg.Process.Env = append(ociCfg.Process.Env,
					"ROCR_VISIBLE_DEVICES=0",
					"HSA_OVERRIDE_GFX_VERSION=0",
				)
			case gpu.VendorIntel:
				ociCfg.Process.Env = append(ociCfg.Process.Env,
					"ONEAPI_DEVICE_SELECTOR=level_zero:*",
				)
			}
		}

		data, _ := json.MarshalIndent(ociCfg, "", "  ")
		os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

		containerID := "pp-" + jobID
		exec.Command("crun", "delete", "-f", containerID).Run()
		defer exec.Command("crun", "delete", "-f", containerID).Run()
		defer os.RemoveAll(bundleDir)

		cmd := exec.Command("crun", "run", "--bundle", bundleDir, containerID)
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		if err := cmd.Start(); err != nil {
			writeWS(OutputMessage{Error: "start crun: " + err.Error()})
			s.jobsMu.Lock()
			job.Status = "failed"
			s.jobsMu.Unlock()
			return
		}

		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				outCh <- OutputMessage{O: scanner.Text() + "\n"}
			}
			if err := scanner.Err(); err != nil {
				outCh <- OutputMessage{Error: "stdout scanner: " + err.Error()}
			}
		}()
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				outCh <- OutputMessage{E: scanner.Text() + "\n"}
			}
			if err := scanner.Err(); err != nil {
				outCh <- OutputMessage{Error: "stderr scanner: " + err.Error()}
			}
		}()
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond)
		cmd.Wait()
		peakBytes = tracker.stop()
		exitCode = cmd.ProcessState.ExitCode()
	}

	outCh <- OutputMessage{Done: true, ExitCode: exitCode, PeakMemBytes: peakBytes}
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
	s.persist()
	releaseHeapAfterWork()
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

	if err := writeResultsTar(w, job); err != nil {
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
func importNAR(nixPath string, nar *os.File) ([]byte, error) {
	run := func(args ...string) ([]byte, error) {
		if _, err := nar.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind nar: %w", err)
		}
		var src io.Reader = nar
		// The NAR is gzipped at export time (torch is a 6.6GB uncompressed
		// closure); decompress it for nix-store --import. Fall back to raw in
		// case a peer has a legacy uncompressed copy.
		br := bufio.NewReader(nar)
		if magic, err := br.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
			zr, zerr := gzip.NewReader(br)
			if zerr != nil {
				return nil, fmt.Errorf("open gzip nar: %w", zerr)
			}
			defer zr.Close()
			src = zr
		} else {
			src = br
		}
		cmd := &exec.Cmd{
			Path:  nixPath,
			Args:  append([]string{"nix-store"}, args...),
			Stdin: src,
		}
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
