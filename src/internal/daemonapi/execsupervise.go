package daemonapi

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog/log"

	"github.com/gorilla/websocket"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
)

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

// jobCgroupDir is the cgroup directory of a capped job, "" when it has none.
func jobCgroupDir(parent, jobID string) string {
	if parent == "" {
		return ""
	}
	return filepath.Join("/sys/fs/cgroup", parent, jobID)
}
