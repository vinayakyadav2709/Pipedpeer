package daemonapi

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
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
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

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
	// LeaseID binds the submitter's lease to this job: the node keeps it while
	// the job runs and releases it when the job ends, renewals or not.
	LeaseID string `json:"lease_id,omitempty"`
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
	// PeakMemBytes is the largest PSS (process tree, bytes) seen while running.
	// It is set on the Done frame so the submitter can learn a job's real
	// footprint and feed the historical estimation tier.
	PeakMemBytes int64 `json:"peak_mem_bytes,omitempty"`
	// Cancelled is set on the Done frame when the client's cancel frame
	// stopped the job; ExitCode is still whatever the process returned.
	Cancelled bool `json:"cancelled,omitempty"`
	// Timing splits the node's share of the run into its parts. Set on the
	// Done frame, so the submitter can tell a slow script from a slow
	// sandbox or a closure import.
	Timing *ExecTiming `json:"timing,omitempty"`
	// Receipt is the shim's account of the run - what it shipped, what it
	// kept, and why - when the job was intercepted and wrote one.
	Receipt json.RawMessage `json:"receipt,omitempty"`
}

// ExecTiming is how a node spent one execution, in milliseconds.
type ExecTiming struct {
	// ImportMs is importing the closure into this node's store; zero when it
	// was already there.
	ImportMs int64 `json:"import_ms"`
	// SetupMs is everything between the request and the script starting,
	// the import excluded: the sandbox, the memory cap, the environment.
	SetupMs int64 `json:"setup_ms"`
	// RunMs is the script itself, from its start to its exit.
	RunMs int64 `json:"run_ms"`
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
	// Detached: the client left without cancelling and the job ran on.
	Detached bool
	// Cancelled: the client's cancel frame stopped the job.
	Cancelled  bool
	FinishedAt time.Time
	// Pgid and PgidStart (its leader's start, ms since the epoch): an
	// unisolated job's process group, so a restarted daemon can stop a job
	// that outlived the old one, and nothing else (pids get reused).
	Pgid      int   `json:",omitempty"`
	PgidStart int64 `json:",omitempty"`
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
	// NodeID (additive) is this node's id, so the submitter's run record
	// names the receiver of env_send by id.
	NodeID string `json:"node_id,omitempty"`
	// Warnings are for the submitter's terminal: peers that refused the
	// closure, and why, so a run that stays local says what kept it there.
	Warnings []string `json:"warnings,omitempty"`
	// SeedMs is how long the upload spent giving the closure to this node's
	// peers before answering, so the submitter's timeline can show it apart
	// from the transfer itself.
	SeedMs int64 `json:"seed_ms,omitempty"`
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// defaultJobDir holds the daemon's own working state: per-job work dirs,
// daemon-state.json and the NAR cache. It is deliberately not
// jobhistory.BaseDir() (<data>/pipedpeer/jobs): the daemon prunes this dir
// hourly by mtime, and sharing it would delete the CLI's job history.
func defaultJobDir() string {
	return filepath.Join(userdir.Data(), "work")
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

// pathExists reports whether a path exists on disk. Used to short-circuit NAR
// re-imports when a prior task already materialised the closure.
func pathExists(p string) bool {
	if host := nixstore.HostPath(p); host != p {
		// Lstat: links inside the store point at other /nix paths, which
		// resolve only inside the namespace.
		_, err := os.Lstat(host)
		return err == nil
	}
	_, err := os.Stat(p)
	return err == nil
}

// withClosureNodes adds PIPEDPEER_CLOSURE_NODES unless the submitter set it.
func withClosureNodes(envs []string, nodes int) []string {
	if envsContain(envs, "PIPEDPEER_CLOSURE_NODES=") {
		return envs
	}
	return append(envs, fmt.Sprintf("PIPEDPEER_CLOSURE_NODES=%d", nodes))
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

// addGPUSysModule exposes /sys/module read-only to a GPU job. libcuda and
// NVML read /sys/module/nvidia*; without it cuInit returns 304 and torch
// quietly falls back to the CPU (bench/gpu_test.go found this).
func addGPUSysModule(c *ociConfig, rootfsDir, jobID string) {
	const dir = "/sys/module"
	if _, err := os.Stat(dir); err != nil {
		log.Warn().Err(err).Str("job", jobID).Msg("GPU job without /sys/module: CUDA will not initialise in the sandbox")
		return
	}
	if err := os.MkdirAll(filepath.Join(rootfsDir, "sys", "module"), 0755); err != nil {
		log.Warn().Err(err).Str("job", jobID).Msg("creating the /sys/module mount point")
	}
	c.Mounts = append(c.Mounts, ociMount{Destination: dir, Type: "bind", Source: dir, Options: []string{"rbind", "ro"}})
}

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

	started := time.Now()
	if cfg.LeaseID != "" {
		// The lease now lives as long as the job, not as long as the
		// submitter keeps renewing it: a detached submitter renews nothing.
		s.bindLease(cfg.LeaseID, jobID)
	}

	// Every frame is recorded in the job's output log before the websocket
	// sees it, so `pipedpeer attach` replays exactly what a live client got.
	jobLog := s.jobOutput(jobID)

	// Timing for the done frame: closure import, sandbox setup, the script.
	execStart := time.Now()
	var receiptPath string
	var importDur, runDur time.Duration
	var runStart time.Time

	outCh := make(chan OutputMessage, 64)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		// Keep draining after the client goes away: the output readers block
		// on outCh, and the job cannot finish until they do.
		var lost bool
		for m := range outCh {
			if m.Done {
				jobLog.Finish(m)
			} else {
				jobLog.Append(m)
			}
			if lost {
				continue
			}
			data, _ := json.Marshal(m)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				lost = true
			}
		}
	}()

	// The client may send one more frame, {"cancel":true}. A connection that
	// ends without it (detach, crash, network loss) leaves the job running.
	cancelCh := make(chan struct{})
	var finished atomic.Bool
	var cancelOnce sync.Once
	requestCancel := func() {
		if finished.Load() {
			return
		}
		cancelOnce.Do(func() {
			log.Info().Str("job", jobID).Msg("cancel requested by the submitter")
			close(cancelCh)
		})
	}
	// An attach client's cancel frame stops the job the same way.
	jobLog.SetCancel(requestCancel)
	go s.readExecClient(conn, job, jobID, cancelCh, requestCancel, &finished)
	cancelled := func() bool {
		select {
		case <-cancelCh:
			return true
		default:
			return false
		}
	}

	// finish sends the done frame, records how the job ended and lets the
	// lease go. Every path out of this handler past this point goes through it.
	finish := func(done OutputMessage) {
		wasCancelled := cancelled()
		finished.Store(true)
		done.Done = true
		done.Cancelled = wasCancelled
		outCh <- done
		close(outCh)
		<-writerDone
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))

		status := "done"
		switch {
		case wasCancelled:
			status = "cancelled"
		case done.ExitCode != 0:
			status = "failed"
		}
		s.jobsMu.Lock()
		job.Status = status
		job.Cancelled = wasCancelled
		job.FinishedAt = time.Now()
		s.jobsMu.Unlock()
		if cfg.LeaseID != "" {
			s.releaseJobLease(jobID, status)
		}
		s.persist()

		secs := time.Since(started).Round(100 * time.Millisecond).Seconds()
		if wasCancelled {
			log.Info().Str("job", jobID).Float64("secs", secs).Msg("job cancelled")
		} else {
			log.Info().Str("job", jobID).Int("exit", done.ExitCode).Float64("secs", secs).
				Str("peak", humanize.IBytes(uint64(max(done.PeakMemBytes, 0)))).Msg("job finished")
		}
	}
	// fail ends a job that never got a process to run.
	fail := func(msg string) {
		outCh <- OutputMessage{Error: msg}
		finish(OutputMessage{ExitCode: 1})
	}

	// If the closure's run entrypoint already exists in the local store, a
	// prior task (or a shared /nix/store volume) already materialised it —
	// skip the (expensive) NAR open + re-import entirely.
	runEntry := filepath.Join(job.StorePath, "bin", "run")
	if !pathExists(runEntry) {
		importStart := time.Now()
		narFile, err := os.Open(job.NarPath)
		if err != nil {
			fail("open nar: " + err.Error())
			return
		}
		defer narFile.Close()

		// Import NAR into local Nix store — uses nix multicall binary
		// invoked as nix-store via argv[0].
		importCtx, stopImport := context.WithCancel(context.Background())
		go func() {
			select {
			case <-cancelCh:
				stopImport()
			case <-importCtx.Done():
			}
		}()
		out, err := importNAR(importCtx, narFile)
		stopImport()
		if err != nil {
			fail(fmt.Sprintf("nix-store --import: %v: %s", err, string(out)))
			return
		}
		importDur = time.Since(importStart)
	}

	if cancelled() {
		// Cancelled while the closure was importing: nothing to stop, so do
		// not start it. 130 is what a shell reports for a job stopped by Ctrl+C.
		finish(OutputMessage{ExitCode: 130})
		return
	}

	s.jobsMu.Lock()
	job.Status = "running"
	s.jobsMu.Unlock()
	log.Info().Str("job", jobID).Str("script", cfg.ScriptPath).Bool("gpu", cfg.GPU).Msg("job started")

	runPath := filepath.Join(cfg.StorePath, "bin", "run")

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

	cfg.Envs = append(cfg.Envs, jobInfoEnv(cfg.Envs, s.nodeID, cpuLimitCores("/sys/fs/cgroup", cgroups.Self()))...)

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
		)
		// The shim writes its receipt here at exit and the Done frame carries
		// it back, so every run's record says what was distributed and why.
		if !envsContain(cfg.Envs, "PIPEDPEER_RECEIPT=") {
			receiptPath = filepath.Join(job.WorkDir, ".pipedpeer", "receipt.json")
			_ = os.Remove(receiptPath)
			cfg.Envs = append(cfg.Envs, "PIPEDPEER_RECEIPT="+receiptPath)
		}
		// The job talks back to this daemon for every intercepted primitive,
		// so it needs the token too. Without it interception would 401 and
		// fall back to local work, which looks like a slow job rather than a
		// misconfiguration.
		if tok := authtoken.Current(); tok != "" && !envsContain(cfg.Envs, authtoken.EnvVar+"=") {
			cfg.Envs = append(cfg.Envs, authtoken.EnvVar+"="+tok)
		}

		// The shim gates remote spill on PIPEDPEER_CLOSURE_NODES != "0": the
		// number of nodes that share this closure (1 at first exec, peers
		// count once broadcastClosure has run). DDP runs set their own
		// RANK/WORLD_SIZE/MASTER_ADDR/PEERS via cfg.Envs and must not be
		// overridden here.
		cfg.Envs = withClosureNodes(cfg.Envs, s.pool.spillPeerCount(cfg.StorePath)+1)
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
				fail("this node keeps its nix store in the user's " +
					"data directory, which only the sandbox can mount at /nix — but crun is " +
					"not installed, so nothing can run. Install it with `pipedpeer setup`.")
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
				fail("this node keeps its nix store in the user's " +
					"data directory, which only the sandbox can mount at /nix — but this " +
					"machine refuses user namespaces, so nothing can run.\n" + why)
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
		// Its own process group, so a cancel reaches python and everything
		// it started, not just the shell.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		runStart = time.Now()
		if err := cmd.Start(); err != nil {
			fail("start command: " + err.Error())
			return
		}
		s.recordJobGroup(job, cmd.Process.Pid)

		procDone := make(chan struct{})
		go superviseCancel(jobID, cancelCh, procDone, signalGroup(jobID, cmd.Process.Pid))
		readers := streamOutput(stdout, stderr, outCh)
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond)
		readers.Wait()
		cmd.Wait()
		runDur = time.Since(runStart)
		close(procDone)
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
		limit := s.grantableMemory(cfg.MemLimitBytes, cfg.LeaseID, jobID)
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
			addGPUSysModule(&ociCfg, rootfsDir, jobID)
			// Fresh, not the 5 s cache: admission must see the device as it is now.
			gpuInfo := gpu.DetectFresh()
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
			ociCfg.Process.Env = append(ociCfg.Process.Env, gpuVisibleEnv(gpuInfo, cfg.GPUDevices)...)
		}

		data, _ := json.MarshalIndent(ociCfg, "", "  ")
		os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

		containerID := "pp-" + jobID
		exec.Command("crun", "delete", "-f", containerID).Run()
		defer exec.Command("crun", "delete", "-f", containerID).Run()
		defer os.RemoveAll(bundleDir)

		cmd := exec.Command("crun", "run", "--bundle", bundleDir, containerID)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		runStart = time.Now()
		if err := cmd.Start(); err != nil {
			fail("start crun: " + err.Error())
			return
		}

		procDone := make(chan struct{})
		go superviseCancel(jobID, cancelCh, procDone, signalContainer(jobID, containerID, cmd.Process.Pid))
		readers := streamOutput(stdout, stderr, outCh)
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond)
		readers.Wait()
		cmd.Wait()
		runDur = time.Since(runStart)
		close(procDone)
		peakBytes = tracker.stop()
		exitCode = cmd.ProcessState.ExitCode()
	}

	done := OutputMessage{ExitCode: exitCode, PeakMemBytes: peakBytes,
		Timing: &ExecTiming{
			ImportMs: importDur.Milliseconds(),
			SetupMs:  (runStart.Sub(execStart) - importDur).Milliseconds(),
			RunMs:    runDur.Milliseconds(),
		}}
	if receiptPath != "" {
		switch b, err := os.ReadFile(receiptPath); {
		case os.IsNotExist(err):
			// The shim writes a receipt only when it intercepted something.
		case err != nil:
			log.Warn().Err(err).Str("job", jobID).Msg("cannot read the shim's receipt; the run has none")
		case !json.Valid(b):
			log.Warn().Str("job", jobID).Int("bytes", len(b)).Msg("shim receipt is not valid JSON; the run has none")
		default:
			done.Receipt = b
		}
	}
	// 137 is SIGKILL. With a cap in force and a fresh kill recorded under our
	// cgroup, the cause is the cap; without the counter moving it could just
	// as well have been an operator's kill -9, so it is not claimed.
	if exitCode == 137 && capBytes > 0 && !cancelled() && oomKills(capParent) > oomBefore {
		done.OOMKilled = true
		done.MemLimitBytes = capBytes
	}
	finish(done)
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
//
// The admitted figure is the lease's reservation. The ceiling is not always
// twice it: with no estimate, or under the floor, the submitter sends a fixed
// cap, so halving the request misjudged what admission approved.
func (s *Server) grantableMemory(requested int64, leaseID, jobID string) int64 {
	if requested <= 0 {
		return requested
	}
	var admitted int64
	if leaseID != "" {
		s.mu.Lock()
		if l, ok := s.leases[leaseID]; ok {
			admitted = l.MemBytes
		}
		s.mu.Unlock()
	}
	if admitted <= 0 {
		// No lease to read (a run placed without one) or it reserved no
		// memory: half the ceiling, the usual doubling, is the best guess.
		admitted = requested / 2
		log.Debug().Str("job", jobID).Str("lease", leaseID).Int64("requested", requested).
			Msg("no lease reservation for this job; taking half its ceiling as admitted")
	}
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

// streamOutput forwards a job's stdout and stderr to outCh as raw chunks, so
// a progress bar redrawing its line with \r reaches the submitter as it
// happens, not at the next newline. A chunk never ends inside a UTF-8
// character: the incomplete tail is carried into the next one. The caller
// must wait on the returned group before cmd.Wait: Wait closes the pipes as
// soon as the process exits, and a reader still draining the last bytes then
// fails with "file already closed" — which the submitter reads as a failed
// job and reschedules, even though the script exited 0.
func streamOutput(stdout, stderr io.Reader, outCh chan<- OutputMessage) *sync.WaitGroup {
	var wg sync.WaitGroup
	forward := func(r io.Reader, name string, msg func(string) OutputMessage) {
		defer wg.Done()
		buf := make([]byte, 4096)
		carry := 0
		for {
			n, err := r.Read(buf[carry:])
			n += carry
			cut := completeUTF8(buf[:n])
			if cut > 0 {
				outCh <- msg(string(buf[:cut]))
			}
			carry = copy(buf, buf[cut:n])
			if err != nil {
				if carry > 0 {
					outCh <- msg(string(buf[:carry]))
				}
				if !errors.Is(err, io.EOF) {
					outCh <- OutputMessage{Error: name + " read: " + err.Error()}
				}
				return
			}
		}
	}
	wg.Add(2)
	go forward(stdout, "stdout", func(l string) OutputMessage { return OutputMessage{O: l} })
	go forward(stderr, "stderr", func(l string) OutputMessage { return OutputMessage{E: l} })
	return &wg
}

// completeUTF8 returns how much of b can be sent without splitting a UTF-8
// character: all of it, or up to the start of an incomplete trailing one.
func completeUTF8(b []byte) int {
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if utf8.FullRune(b[i:]) {
				return len(b)
			}
			return i
		}
	}
	return len(b)
}

// readExecClient reads the frames a client sends after the config frame.
// {"cancel":true} calls requestCancel. A connection that ends without one while
// the job still runs (close 4001 "detach", a crash, a lost network) marks the
// job detached and leaves it running: only an explicit cancel kills a job.
func (s *Server) readExecClient(conn *websocket.Conn, job *JobRecord, jobID string,
	cancelCh <-chan struct{}, requestCancel func(), finished *atomic.Bool) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if finished.Load() {
				return
			}
			select {
			case <-cancelCh:
				return
			default:
			}
			s.jobsMu.Lock()
			job.Detached = true
			s.jobsMu.Unlock()
			s.persist()
			log.Info().Str("job", jobID).Str("why", err.Error()).Msg("client left; job keeps running")
			return
		}
		var f struct {
			Cancel bool `json:"cancel"`
		}
		if err := json.Unmarshal(msg, &f); err != nil || !f.Cancel {
			log.Warn().Str("job", jobID).Str("frame", string(msg)).Msg("ignoring unexpected frame from exec client")
			continue
		}
		requestCancel()
	}
}

// superviseCancel stops a running job once cancelCh closes: SIGTERM so the
// script can clean up, SIGKILL if it is still there 2 s later.
func superviseCancel(jobID string, cancelCh, procDone <-chan struct{}, signal func(syscall.Signal)) {
	select {
	case <-procDone:
		return
	case <-cancelCh:
	}
	signal(syscall.SIGTERM)
	select {
	case <-procDone:
	case <-time.After(2 * time.Second):
		log.Warn().Str("job", jobID).Msg("job ignored SIGTERM for 2 s; killing it")
		signal(syscall.SIGKILL)
	}
}

// signalGroup signals the process group led by pid (the job's shell).
func signalGroup(jobID string, pid int) func(syscall.Signal) {
	return func(sig syscall.Signal) {
		if err := syscall.Kill(-pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			log.Warn().Err(err).Str("job", jobID).Int("pgid", pid).Str("signal", sig.String()).
				Msg("could not signal the job's process group")
		}
	}
}

// signalContainer signals every process in the job's crun container. Inside
// the container's pid namespace the script is pid 1, which ignores SIGTERM
// unless it installs a handler, so --all (the whole cgroup) is tried first.
// If crun cannot do it, SIGKILL falls back to `crun delete -f` and SIGTERM to
// crun's own process group.
func signalContainer(jobID, containerID string, crunPid int) func(syscall.Signal) {
	group := signalGroup(jobID, crunPid)
	return func(sig syscall.Signal) {
		name := "TERM"
		if sig == syscall.SIGKILL {
			name = "KILL"
		}
		out, err := exec.Command("crun", "kill", "--all", containerID, name).CombinedOutput()
		if err == nil {
			return
		}
		log.Warn().Err(err).Str("job", jobID).Str("output", strings.TrimSpace(string(out))).
			Msg("crun kill --all failed; falling back")
		if sig == syscall.SIGKILL {
			if out, err := exec.Command("crun", "delete", "-f", containerID).CombinedOutput(); err != nil {
				log.Warn().Err(err).Str("job", jobID).Str("output", strings.TrimSpace(string(out))).
					Msg("crun delete -f failed; killing crun's process group")
				group(sig)
			}
			return
		}
		group(sig)
	}
}

// gpuVisibleEnv picks the device a job sees: the one the lease reserved
// (leased, set by the coordinator), else the detector's Best. The same index
// goes to NVIDIA_VISIBLE_DEVICES or ROCR_VISIBLE_DEVICES by vendor.
func gpuVisibleEnv(info gpu.Info, leased string) []string {
	dev := leased
	if dev == "" {
		if info.Best < 0 {
			return nil
		}
		dev = strconv.Itoa(info.Best)
	}
	switch info.Vendor {
	case gpu.VendorNVIDIA:
		return []string{"NVIDIA_VISIBLE_DEVICES=" + dev, "NVIDIA_DRIVER_CAPABILITIES=compute,utility"}
	case gpu.VendorAMD:
		return []string{"ROCR_VISIBLE_DEVICES=" + dev, "HSA_OVERRIDE_GFX_VERSION=0"}
	}
	return nil
}

// jobInfoEnv is what a job is told about where it runs: this node's id (the
// shim's run record names machines by it) and, when the node is limited, its
// CPU limit in cores. The sandbox mounts no /sys, so the shim cannot read the
// cgroup itself. A value the submitter set wins.
func jobInfoEnv(envs []string, nodeID string, cpuCores float64) []string {
	var out []string
	if nodeID != "" && !envsContain(envs, "PIPEDPEER_NODE_ID=") {
		out = append(out, "PIPEDPEER_NODE_ID="+nodeID)
	}
	if cpuCores > 0 && !envsContain(envs, "PIPEDPEER_CPU_LIMIT=") {
		out = append(out, "PIPEDPEER_CPU_LIMIT="+strconv.FormatFloat(cpuCores, 'f', -1, 64))
	}
	return out
}

// cpuLimitCores is the tightest cpu.max quota/period on cg and its ancestors
// under root (cgroup v2), in cores; 0 when none is set. Jobs get no CPU quota
// of their own, so the daemon's chain is the job's limit.
func cpuLimitCores(root, cg string) float64 {
	if cg == "" {
		return 0
	}
	var best float64
	for dir := filepath.Join(root, cg); ; dir = filepath.Dir(dir) {
		if b, err := os.ReadFile(filepath.Join(dir, "cpu.max")); err == nil {
			f := strings.Fields(string(b))
			if len(f) == 2 && f[0] != "max" {
				q, qerr := strconv.ParseFloat(f[0], 64)
				p, perr := strconv.ParseFloat(f[1], 64)
				if qerr == nil && perr == nil && p > 0 && (best == 0 || q/p < best) {
					best = q / p
				}
			}
		}
		if dir == filepath.Clean(root) || dir == filepath.Dir(dir) {
			return best
		}
	}
}
