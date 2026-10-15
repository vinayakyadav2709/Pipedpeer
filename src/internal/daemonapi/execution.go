package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/rs/zerolog/log"

	"github.com/gorilla/websocket"
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
