package daemonapi

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/pipedpeer/pipedpeer/internal/userns"
	"github.com/rs/zerolog/log"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// MemLimitBytes caps the sandbox via cgroups; 0 leaves it uncapped.
	// Everything else about memory here is an estimate checked at admission;
	// this is the only part the kernel enforces.
	MemLimitBytes int64 `json:"mem_limit_bytes,omitempty"`
}

// OCI config structures for crun bundle generation.
type ociConfig struct {
	OciVersion string     `json:"ociVersion"`
	Process    ociProcess `json:"process"`
	Root       ociRoot    `json:"root"`
	Hostname   string     `json:"hostname,omitempty"`
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
	Namespaces  []ociNamespace `json:"namespaces"`
	Devices     []ociDevice    `json:"devices,omitempty"`
	Resources   *ociResources  `json:"resources,omitempty"`
	CgroupsPath string         `json:"cgroupsPath,omitempty"`
}

// ociResources carries the only memory bound the kernel actually enforces.
// Admission control and the 40%-of-free-RAM chunk rule are both estimates
// made before the fact; a job that outgrows its estimate used to take the
// machine down with it, because nothing below those estimates said no.
type ociResources struct {
	Memory *ociMemory `json:"memory,omitempty"`
}

type ociMemory struct {
	Limit int64 `json:"limit,omitempty"`
	// Swap is pinned to Limit so the job cannot escape the cap by swapping,
	// which on a machine with swap turns an OOM into thrashing that takes
	// every other job on the node down with it.
	Swap int64 `json:"swap,omitempty"`
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
	// OOMKilled says the kernel killed the job for exceeding MemLimitBytes,
	// so the client can say that instead of reporting a signal number.
	OOMKilled     bool  `json:"oom_killed,omitempty"`
	MemLimitBytes int64 `json:"mem_limit_bytes,omitempty"`
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
		return filepath.Join(userdir.Data(), "jobs")
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

// gpuDriverLibs lists the host GPU driver's shared objects. Only these get
// into the sandbox: bind-mounting the whole directory would put the host's
// glibc ahead of the closure's on the search path, and the two are not
// interchangeable.
//
// The libraries are versioned (libcuda.so.1 -> libcuda.so.580.65.06) and torch
// dlopens the SONAME, so both the link and its target have to travel.
func gpuDriverLibs(dir string) []string {
	if dir == "" {
		return nil
	}
	var libs []string
	for _, pattern := range []string{"libcuda.so*", "libnvidia-*.so*", "libnvcuvid.so*"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			continue
		}
		libs = append(libs, matches...)
	}
	return libs
}

// gpuDriverMountDir is where the driver libraries land inside the sandbox.
// NixOS uses this path for exactly this purpose, so a closure that already
// looks there finds them without help.
const gpuDriverMountDir = "/run/opengl-driver/lib"

func buildNonIsolatedCmd(runPath, workDir string, scriptRelPath string, scriptArgs, envs []string) string {
	// Same reasoning as the sandbox env: stdout is a pipe, and python would
	// otherwise sit on a full training run's prints until exit.
	cmd := "export PYTHONUNBUFFERED=1 && mkdir -p " + shellQuote(workDir) + " && cd " + shellQuote(workDir) + " && " +
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
	fail := func(status int, msg string) {
		os.RemoveAll(jobDir)
		writeJSON(w, status, map[string]string{"error": msg})
	}

	var storePath, scriptPath, narPath, skipBroadcast string
	var haveWorkspace bool
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
		case "workspace":
			haveWorkspace = true
			if err := extractWorkspaceTar(part, workDir, uploaded); err != nil {
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
	if skipBroadcast != "1" {
		s.broadcastClosure(storePath)
	}

	writeJSON(w, http.StatusOK, UploadResponse{JobID: jobID, StorePath: storePath})
}

// extractWorkspaceTar unpacks the workspace stream into workDir, refusing
// entries that escape it, and records a stamp per extracted file so results
// can later be limited to what the job actually changed.
func extractWorkspaceTar(src io.Reader, workDir string, uploaded map[string]FileStamp) error {
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
		out, err := importNAR(narFile)
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
		// The job talks back to this daemon for every intercepted primitive,
		// so it needs the token too. Without it interception would 401 and
		// fall back to local work, which looks like a slow job rather than a
		// misconfiguration.
		if tok := authtoken.Current(); tok != "" && !envsContain(cfg.Envs, authtoken.EnvVar+"=") {
			cfg.Envs = append(cfg.Envs, authtoken.EnvVar+"="+tok)
		}

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
	// With a private nix store the sandbox is not a hardening layer, it is
	// what puts the store at /nix: the closure's binaries all name
	// /nix/store/... paths that do not exist on this machine outside the
	// mount namespace. Degrading to unisolated there does not produce a less
	// safe run, it produces a run that cannot start, so say so instead.
	sandboxRequired := nixstore.Private()

	if cfg.Isolate {
		if _, err := exec.LookPath("crun"); err != nil {
			if sandboxRequired {
				writeWS(OutputMessage{Error: "this node keeps its nix store in the user's " +
					"data directory, which only the sandbox can mount at /nix — but crun is " +
					"not installed, so nothing can run. Install it with `pipedpeer setup`."})
				s.jobsMu.Lock()
				job.Status = "failed"
				s.jobsMu.Unlock()
				return
			}
			outCh <- OutputMessage{E: "[pipedpeer] crun not found on this node — running unisolated\n"}
			cfg.Isolate = false
		} else if ok, why := userns.Available(); !ok {
			// crun's own message for this is "unshare: Operation not
			// permitted", which names neither the cause nor the fix, and the
			// three possible causes need three different fixes.
			outCh <- OutputMessage{E: "[pipedpeer] this node cannot create a user namespace, " +
				"so jobs run unisolated.\n    " + why + "\n"}
			usernsOnce.Do(func() { log.Warn().Str("reason", why).Msg("no user namespace: jobs run unisolated") })
			if sandboxRequired {
				writeWS(OutputMessage{Error: "this node keeps its nix store in the user's " +
					"data directory, which only the sandbox can mount at /nix — but this " +
					"machine refuses user namespaces, so nothing can run.\n" + why})
				s.jobsMu.Lock()
				job.Status = "failed"
				s.jobsMu.Unlock()
				return
			}
			cfg.Isolate = false
		}
	}

	// Tracked across the branch so a job killed by its own memory cap can be
	// reported as that, rather than as a bare "exited with code 137" the user
	// has no way to interpret.
	var capBytes int64
	var capParent string
	var oomBefore int64

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

		// HOME has to exist, not just be named: torch resolves a cache
		// directory under it during `import torch`.
		os.MkdirAll(filepath.Join(rootfsDir, "home", "root"), 0755)

		args := append([]string{runPath, cfg.ScriptPath}, cfg.Args...)
		env := []string{
			"HOME=/home/root",
			// There is no /etc/passwd in this rootfs, so getpass.getuser()
			// has nothing to fall back to and raises. torch calls it while
			// importing, which turned every job here into "OSError: No
			// username set in the environment" - an error about the sandbox
			// wearing the costume of a problem with the user's code.
			"USER=root",
			"LOGNAME=root",
			"PATH=/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin",
			// stdout is a pipe in here, so python block-buffers and a whole
			// training run prints nothing until exit; the audience needs it live.
			"PYTHONUNBUFFERED=1",
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

		q := sandboxQuirks()
		ociCfg := ociConfig{
			OciVersion: "1.0.2",
			Process: ociProcess{
				Terminal: false,
				User:     ociUser{UID: 0, GID: 0},
				Args:     args,
				Env:      env,
				Cwd:      "/work",
			},
			Root: ociRoot{Path: "rootfs", Readonly: true},
			// Hostname only when the machine allows sethostname; inside an
			// unprivileged container it does not, and crun fails the job.
			Hostname: hostnameFor(q),
			Mounts: []ociMount{
				// Source, not destination: with a private store the files
				// live in the user's data directory, but the paths baked into
				// every binary still say /nix, so that is where they must
				// appear inside the sandbox.
				{Destination: "/nix", Type: "bind", Source: nixstore.HostNixDir(), Options: []string{"rbind", "ro"}},
				procMountFor(q),
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
				Namespaces: namespacesFor(q),
			},
		}

		// The one memory bound the kernel enforces. Everything else is an
		// estimate made before the job ran: admission checks a forecast, and
		// the 40%-of-free-RAM chunk rule bounds a payload, but neither can
		// stop a job that simply allocates more than it said it would. Until
		// now nothing did, so one job's runaway allocation was every other
		// job on the node's problem too.
		parent, canCap, why := cgroups.Prepare()
		limit := s.grantableMemory(cfg.MemLimitBytes, jobID)
		if applyMemLimit(&ociCfg, limit, parent, canCap, jobID) {
			log.Info().Int64("bytes", limit).Str("job", jobID).
				Str("cgroup", parent).Msg("job memory capped by cgroup")
			capBytes, capParent = limit, parent
			// memory.events is hierarchical, so the parent's counter covers
			// every job cgroup beneath it. Snapshotting it here turns "some
			// job was OOM-killed at some point" into "a kill happened while
			// this job ran", which is what makes the attribution safe.
			oomBefore = oomKills(parent)
		} else if !canCap {
			// One warning per daemon, not per job: the reason never changes
			// within a process, and a silent unlimited sandbox is exactly the
			// thing this whole path exists to stop.
			noCapOnce.Do(func() {
				log.Warn().Str("reason", why).Msg("jobs run without an enforced memory limit; " +
					"start the daemon with `pipedpeer start` on a systemd machine to get one")
			})
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

			// The device nodes alone are not enough: without libcuda.so.1 the
			// runtime has nothing to dlopen, so torch reports no CUDA device
			// and silently trains on the CPU — a 17x slowdown that looks like
			// a working run. nvidia-container-toolkit would bridge these, but
			// crun on its own does not, and crun is the only runtime we
			// require.
			if libs := gpuDriverLibs(gpuDriverLibDir()); len(libs) > 0 {
				for _, lib := range libs {
					ociCfg.Mounts = append(ociCfg.Mounts, ociMount{
						Destination: filepath.Join(gpuDriverMountDir, filepath.Base(lib)),
						Type:        "bind",
						Source:      lib,
						Options:     []string{"rbind", "ro"},
					})
				}
				ociCfg.Process.Env = append(ociCfg.Process.Env,
					"LD_LIBRARY_PATH="+gpuDriverMountDir)
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

	done := OutputMessage{Done: true, ExitCode: exitCode, PeakMemBytes: peakBytes}
	// 137 is SIGKILL. With a cap in force and a fresh kill recorded under our
	// cgroup, the cause is the cap; without the counter moving it could just
	// as well have been an operator's kill -9, so it is not claimed.
	if exitCode == 137 && capBytes > 0 && oomKills(capParent) > oomBefore {
		done.OOMKilled = true
		done.MemLimitBytes = capBytes
	}
	outCh <- done
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
func importNAR(nar *os.File) ([]byte, error) {
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
		cmd, cleanup, err := nixstore.Cmd("", append([]string{"nix-store"}, args...)...)
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

// noCapOnce keeps the "no memory limit" warning to one line per daemon.
var noCapOnce sync.Once

// usernsOnce keeps the "no user namespace" warning to one line per daemon.
var usernsOnce sync.Once

// applyMemLimit puts a cgroup memory cap on the bundle, and reports whether
// it did. parent comes from cgroups.Prepare, which has already proved that a
// child of it gets a writable memory.max; split out from bundle assembly so
// the decision can be tested on a machine with no delegated hierarchy.
//
// Swap is pinned to the same value deliberately. Left alone, a job that hits
// its cap starts swapping instead of failing, and thrashing takes every other
// job on the node down with it rather than just itself.
// grantableMemory bounds a job's ceiling by what the machine can actually
// spare, and reports when it had to.
//
// The ceiling arrives from the submitter as twice the estimate - deliberately
// generous, because killing a job that would have finished is a worse failure
// than the runaway it prevents. What was missing is that generosity has to
// stop somewhere, and "twice the estimate" knows nothing about the machine it
// lands on. A job admitted for 4 GB was then permitted 7.5 GB by the kernel,
// and with a second daemon on the same host doing likewise, the two ceilings
// added up to more memory than existed: the machine went out of memory and
// the kernel picked a victim outside either cgroup.
//
// So the ceiling is clamped to what is free right now, counting what daemons
// sharing this machine have promised. Never below the estimate that was
// admitted, though - admission already decided that much would fit, and a
// ceiling under it would kill the job on arrival for a shortage that appeared
// after it was let in.
func (s *Server) grantableMemory(requested int64, jobID string) int64 {
	if requested <= 0 {
		return requested
	}
	// The submitter doubles the estimate, so half of what it asked for is the
	// figure admission approved.
	admitted := requested / 2
	spare := s.AvailableForJob()
	if spare >= requested {
		return requested
	}
	granted := spare
	if granted < admitted {
		granted = admitted
	}
	if granted >= requested {
		return requested
	}
	log.Info().Int64("requested", requested).Int64("granted", granted).
		Int64("machine_spare", spare).Str("job", jobID).
		Msg("job ceiling lowered to what this machine can spare")
	return granted
}

func applyMemLimit(cfg *ociConfig, limit int64, parent string, canCap bool, jobID string) bool {
	if limit <= 0 || !canCap || cfg.Linux == nil {
		return false
	}
	cfg.Linux.Resources = &ociResources{
		Memory: &ociMemory{Limit: limit, Swap: limit},
	}
	cfg.Linux.CgroupsPath = filepath.Join(parent, jobID)
	return true
}

// oomKills reads the cumulative oom_kill counter for a cgroup and everything
// under it. Unreadable counts as zero: the caller only ever compares two
// samples, and a missing file makes the comparison decline to attribute
// rather than attribute wrongly.
func oomKills(parent string) int64 {
	if parent == "" {
		return 0
	}
	data, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", parent, "memory.events"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "oom_kill" {
			n, err := strconv.ParseInt(f[1], 10, 64)
			if err == nil {
				return n
			}
		}
	}
	return 0
}
