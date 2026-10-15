package daemonapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/userns"
	"github.com/rs/zerolog/log"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
)

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

	// A full-node phone has no user namespaces: its sandbox is a proot view
	// plus rlimits (phone.SandboxShell), weaker than crun and said so once.
	var phoneBox string
	if cfg.Isolate && phone.InProot() {
		box, err := phone.SandboxShell(job.WorkDir, s.grantableMemory(cfg.MemLimitBytes, cfg.LeaseID, jobID), shellQuote)
		if err != nil {
			fail("phone sandbox: " + err.Error())
			return
		}
		phoneBoxOnce.Do(func() { log.Warn().Msg(phone.SandboxNote) })
		phoneBox, cfg.Isolate = box, false
	}

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
		runCmd := buildNonIsolatedCmd(phoneBox, runPath, job.WorkDir, cfg.ScriptPath, cfg.Args, cfg.Envs)
		cmd := termuxexec.Command("sh", "-c", runCmd)
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
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond, "")
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
		termuxexec.Command("crun", "delete", "-f", containerID).Run()
		defer termuxexec.Command("crun", "delete", "-f", containerID).Run()
		defer os.RemoveAll(bundleDir)

		cmd := termuxexec.Command("crun", "run", "--bundle", bundleDir, containerID)
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
		tracker := newPeakMemTracker(int32(cmd.Process.Pid), 500*time.Millisecond, jobCgroupDir(capParent, jobID))
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

// phoneBoxOnce logs the phone sandbox note once per daemon.
var phoneBoxOnce sync.Once
