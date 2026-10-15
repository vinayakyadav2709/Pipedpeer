package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// placementWait is how long a run waits for a node to become able to take
// it. PIPEDPEER_REMOTE_WAIT overrides it; 0 takes one look. Ten minutes: a
// class's runs queue behind each other, and 15 s failed them while the
// previous job still held the memory (13-finale needed 10m).
func placementWait() time.Duration {
	if v, err := time.ParseDuration(os.Getenv("PIPEDPEER_REMOTE_WAIT")); err == nil && v >= 0 {
		return v
	}
	return 10 * time.Minute
}

// placementProgress prints why no node can take the run yet: once when the
// wait starts, then every 30 s while it lasts.
func placementProgress(what string, window time.Duration) func(coordinator.PlacementDecision, time.Duration) {
	return func(d coordinator.PlacementDecision, waited time.Duration) {
		if waited == 0 {
			fmt.Printf("[coordinator] no eligible node %syet; waiting up to %s (%s)\n", what, window, d.RejectionLine())
			return
		}
		fmt.Printf("[coordinator] still waiting for a node %s(%s of %s): %s\n", what, waited, window, d.RejectionLine())
	}
}

// acceptWithin asks for admission until it is granted, the refusal is not
// about memory, or the window runs out.
//
// Memory is the one refusal that clears on its own. A run started straight
// after another one finds the previous job's memory still being handed back,
// and its admission check fails by a few hundred megabytes: on the bench
// rig, every second back-to-back run of 02_numpy_heavy was refused in under
// a tenth of a second while the first one, with the same estimate, ran.
func acceptWithin(ctx context.Context, window time.Duration, accept func() (*daemonctl.AcceptResponse, error)) (*daemonctl.AcceptResponse, error) {
	start := time.Now()
	deadline := start.Add(window)
	var nextReport time.Time
	for {
		resp, err := accept()
		if err == nil || !strings.Contains(err.Error(), "insufficient memory") || time.Now().After(deadline) {
			return resp, err
		}
		if nextReport.IsZero() {
			fmt.Printf("[coordinator] %v; waiting up to %s for memory to free...\n", err, window)
			nextReport = time.Now().Add(coordinator.WaitProgressEvery)
		} else if time.Now().After(nextReport) {
			fmt.Printf("[coordinator] still waiting (%s of %s): %v\n", time.Since(start).Truncate(time.Second), window, err)
			nextReport = time.Now().Add(coordinator.WaitProgressEvery)
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(time.Second):
		}
	}
}

func newRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run <script.py> [args...]",
		Short: "Submit and execute a task on a remote node",
		Long: `Submit a Python script for remote execution. Auto-discovers nodes if --host is not specified.

The first argument is the script path; the remaining arguments are passed
through to the script itself (like python). Use -- to pass flags that start
with a dash: pipedpeer run train.py -- --lr 0.1`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptPath := args[0]
			args = args[1:]
			daemonHost, _ := cmd.Flags().GetString("host")
			daemonPort, _ := cmd.Flags().GetInt("port")
			jobName, _ := cmd.Flags().GetString("job-name")
			isolate, _ := cmd.Flags().GetBool("isolate")
			checkOnly, _ := cmd.Flags().GetBool("check-only")
			cold, _ := cmd.Flags().GetBool("cold")
			detach, _ := cmd.Flags().GetBool("detach")
			pythonVersion, _ := cmd.Flags().GetString("python")
			registryURL, _ := cmd.Flags().GetString("registry")
			memOverride, _ := cmd.Flags().GetString("mem")
			envs, _ := cmd.Flags().GetStringSlice("env")
			pkgs, _ := cmd.Flags().GetStringSlice("pkg")
			remote, _ := cmd.Flags().GetBool("remote")
			offload, _ := cmd.Flags().GetBool("offload")
			strategy, _ := cmd.Flags().GetString("strategy")
			distribute, _ := cmd.Flags().GetString("distribute")
			switch distribute {
			case "", "auto":
				// cost models decide — the default.
			case "force":
				// Skip the shim's cost models: every interceptable op ships to
				// the cluster even when local would win. Demo knob, not a
				// performance mode.
				envs = append(envs, "PIPEDPEER_DISTRIBUTE=force")
			default:
				return fmt.Errorf("--distribute must be auto or force, got %q", distribute)
			}
			gpuMode, _ := cmd.Flags().GetString("gpu")
			gpuID, _ := cmd.Flags().GetString("gpu-id")
			gpuMemStr, _ := cmd.Flags().GetString("gpu-mem")
			requiredCores, _ := cmd.Flags().GetInt("cores")
			ddpNodes, _ := cmd.Flags().GetInt("ddp")
			ddpPort, _ := cmd.Flags().GetInt("ddp-port")
			var targetID string
			// This machine's daemon endpoint — stamped into job envs so the
			// executing node can sink the orchestrator behind real workers
			// in spill order (weak-orchestrator).
			submitter := net.JoinHostPort(detectLocalIP(), strconv.Itoa(daemonPort))

			// GPU requirement:
			//   force  — only GPU nodes are eligible
			//   prefer — GPU nodes first, CPU fallback
			//   off    — CPU only
			//   ""     — infer from the script's imports (torch, cupy, ...)
			var requireGPU, preferGPU bool
			switch gpuMode {
			case "force":
				requireGPU, preferGPU = true, true
			case "prefer":
				preferGPU = true
			case "off":
			case "":
				if pythondeps.HasGPUImports(scriptPath) {
					preferGPU = true
					fmt.Println("[coordinator] GPU-accelerated libraries detected — preferring GPU nodes")
				}
			default:
				return fmt.Errorf("invalid --gpu value %q (want force, prefer, or off)", gpuMode)
			}
			requestGPU := requireGPU || preferGPU
			requiredGPUMem := resourceest.ParseMemString(gpuMemStr)

			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}

			wasStarted, err := daemonctl.EnsureStarted(nodeID.NodeID, daemonPort)
			if err != nil {
				return err
			}
			if wasStarted {
				fmt.Println("started daemon")
			}

			// --offload: distributed pieces (pool/joblib chunks, pandas and
			// numpy offloads, DDP ranks) keep off this machine, decided by its
			// machine key - never its address, since an internet peer is
			// 127.0.0.1:<port>. The script itself still runs here (or on the
			// --remote node, whose spreading then skips this machine).
			offloadKey := ""
			if offload {
				offloadKey = heartbeat.Machine()
				if offloadKey == "" {
					fmt.Fprintln(os.Stderr, "warning: --offload: cannot read this machine's key; distributed work may still run here")
				} else {
					envs = append(envs, "PIPEDPEER_OFFLOAD="+offloadKey)
				}
			}

			userMem := resourceest.ParseMemString(memOverride)
			resReq := resourceest.EstimateFromScript(scriptPath, nil, userMem)
			fmt.Printf("[coordinator] Resource estimate: %s (tier=%s)\n",
				resourceest.FormatBytes(resReq.MemBytes), resReq.Tier)

			if checkOnly {
				return runCheckOnly(checkOnlyOptions{
					ScriptPath:     scriptPath,
					DaemonHost:     daemonHost,
					DaemonPort:     daemonPort,
					Remote:         remote,
					RequireGPU:     requireGPU,
					PreferGPU:      preferGPU,
					RequiredGPUMem: requiredGPUMem,
					RequiredCores:  requiredCores,
					DDPNodes:       ddpNodes,
					Pkgs:           pkgs,
					PythonVersion:  pythonVersion,
					RegistryURL:    registryURL,
					Strategy:       strategy,
					JobName:        jobName,
					NodeID:         nodeID,
					EstMemBytes:    resReq.MemBytes,
					EstTier:        resReq.Tier,
				})
			}

			// Ctrl+C, a kill or a closed terminal stops the job everywhere
			// (local script, pool chunks on peers, --remote job, every DDP
			// rank). A second Ctrl+C exits at once.
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer stop()
			go exitOnSecondSignal(ctx, stop)

			// DDP: --ddp N caps the rank count; without it, DDP auto-activates
			// when the script uses torch.distributed and every eligible node
			// joins the ring. Min is always 1 (--ddp 1 is a plain run).
			// ponytail: no redundancy margin yet — on a large cluster with
			// flaky nodes, all-in means one death kills the run. If that ever
			// bites, add a heuristic here (spare ranks, or eligible-N).
			// ddpOffload: a DDP script under --offload trains on other
			// machines only, as a ring or, with one of them, placed there
			// like --remote.
			ddpOffload := false
			if ddpNodes > 1 || (ddpNodes == 0 && pythondeps.HasDistributedImports(scriptPath)) {
				exclude := offloadKey
				eligible := ddpEligibleCountExcept(daemonPort, requireGPU, requiredGPUMem, resReq.MemBytes, exclude)
				if exclude != "" && eligible == 0 {
					fmt.Println("[coordinator] warning: --offload: no other machine can take a rank, so this machine trains")
					exclude = ""
					eligible = ddpEligibleCount(daemonPort, requireGPU, requiredGPUMem, resReq.MemBytes)
				}
				ddpOffload = exclude != ""
				worldSize := eligible
				if ddpNodes > 1 {
					worldSize = min(ddpNodes, eligible)
				}
				if worldSize >= 2 {
					if detach {
						fmt.Println("detach is not supported for DDP/map runs; Ctrl+C stops it")
					}
					_, stopSuspend := watchSuspend(ctx, false)
					err := runDDP(ddpRunOptions{
						Ctx:            ctx,
						Nodes:          worldSize,
						DaemonPort:     daemonPort,
						MasterPort:     ddpPort,
						ScriptPath:     scriptPath,
						ScriptArgs:     args,
						JobName:        jobName,
						Isolate:        isolate,
						RegistryURL:    registryURL,
						PythonVersion:  pythonVersion,
						Pkgs:           pkgs,
						Envs:           envs,
						GPUDevices:     gpuID,
						RequireGPU:     requireGPU,
						PreferGPU:      preferGPU,
						RequiredGPUMem: requiredGPUMem,
						RequiredCores:  requiredCores,
						NoSelf:         remote || ddpOffload,
						ExcludeMachine: exclude,
						Strategy:       strategy,
						MemOverride:    memOverride,
						MaxRanks:       max(ddpNodes, 0),
					})
					stopSuspend()
					// A ring that measures slower than one machine falls
					// through to the ordinary single-node path below.
					if !errors.Is(err, errRingNotWorthIt) {
						return runOutcome(err)
					}
					fmt.Printf("[coordinator] running on one node: every ring available measures slower\n")
				} else if ddpNodes > 1 {
					fmt.Printf("[coordinator] fewer than 2 eligible nodes — running single-node\n")
				}
			}

			estimatedMemBytes := resReq.MemBytes
			estimationTier := resReq.Tier
			var placementSource, placementReason string
			var placementDegraded bool
			var placementCandidates int

			// --gpu force never runs on a machine without a GPU. The
			// single-node default is this machine, and a GPU-less one (a lab
			// PC submitting to the GPU laptop, or a DDP ring that fell back to
			// one node) got "insufficient memory" or ran on its CPU. Such a
			// run is placed like --remote instead, on GPU nodes only.
			gpuPlace := requireGPU && !remote && !ddpOffload && daemonHost == "" && gpu.Detect().Vendor == gpu.VendorNone
			if remote || ddpOffload || gpuPlace {
				if remote {
					fmt.Println("[coordinator] --remote: discovering a node other than this machine...")
				} else if ddpOffload {
					fmt.Println("[coordinator] --offload: placing the training on a node other than this machine...")
				} else {
					fmt.Println("[coordinator] --gpu force: this machine has no GPU; placing the run on a GPU node...")
				}

				rrFn := func(count int) int {
					url := fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", daemonPort, count)
					resp, err := http.Get(url)
					if err != nil {
						return 0
					}
					defer resp.Body.Close()
					var result struct{ Index int }
					json.NewDecoder(resp.Body).Decode(&result)
					return result.Index
				}

				coord := coordinator.New(coordinator.Config{
					RegistryURL:      registryURL,
					SelfIdentity:     nodeID,
					SelfSSH:          fmt.Sprintf("root@%s:22", detectLocalIP()),
					SelfDaemon:       daemonPort,
					SelfLoad:         heartbeat.CollectLoad(0, 0),
					JobName:          jobName,
					RequiredMemBytes: resReq.MemBytes,
					RequiredCores:    requiredCores,
					RequiredGPUMem:   requiredGPUMem,
					NoSelf:           remote || ddpOffload,
					ExcludeMachine:   offloadKey,
					RequireGPU:       requireGPU,
					PreferGPU:        preferGPU,
					Strategy:         strategy,
					RoundRobinFn:     rrFn,
				})

				// A remote run must fail loudly when there is no node at all
				// instead of queuing forever: the user can just drop --remote.
				// PIPEDPEER_REMOTE_WAIT overrides the window; 0 takes one look.
				wait := placementWait()
				decision := coord.FindNodeWithin(ctx, wait, time.Second, placementProgress("", wait))
				if decision.ChosenNode.NodeID == "" {
					if why := decision.RejectionSummary(); why != "" {
						fmt.Print("[coordinator] nodes turned down:\n" + why)
					}
					if ddpOffload {
						return fmt.Errorf("no other node can take this training — run without --offload to train on this machine")
					}
					if !remote {
						return fmt.Errorf("no GPU node can take this run — drop --gpu force to run it on a CPU")
					}
					return fmt.Errorf("no eligible node found — run without --remote to execute on this machine")
				}

				scriptArgs := args

				// The node tells us which GPU it reserved; honour that unless
				// the user pinned devices explicitly with --gpu-id.
				var reservedGPU atomic.Int64
				reservedGPU.Store(-1)
				queuedAt := time.Now()
				// The placement behind the current attempt: its lease keeps
				// the job alive, and its decision goes in the run's record so
				// `job --explain` can show it.
				var placed atomic.Pointer[coordinator.LeaseResult]

				detachCh, stopSuspend := watchSuspend(ctx, true)
				defer stopSuspend()

				executor := func(host string, port int, targetNodeID string) error {
					var decision coordinator.PlacementDecision
					var leaseID string
					if r := placed.Load(); r != nil {
						decision = r.Decision
						leaseID = r.LeaseID
					}
					devices := gpuID
					if devices == "" {
						if idx := reservedGPU.Load(); idx >= 0 {
							devices = strconv.FormatInt(idx, 10)
						}
					}
					err := app.Run(ctx, app.Options{
						ScriptPath:        scriptPath,
						DaemonHost:        host,
						DaemonPort:        port,
						SelfDaemonPort:    daemonPort,
						Submitter:         submitter,
						TargetID:          targetNodeID,
						JobName:           jobName,
						Isolate:           isolate,
						Intercept:         true,
						GPU:               requestGPU,
						GPUDevices:        devices,
						PythonVersion:     pythonVersion,
						Envs:              envs,
						Pkgs:              pkgs,
						ScriptArgs:        scriptArgs,
						PlacementSource:   string(decision.Source),
						DegradedMode:      decision.DegradedMode,
						CandidateCount:    len(decision.Candidates),
						PlacementReason:   decision.Reason,
						Strategy:          strategy,
						Candidates:        coordinator.ExplainCandidates(decision),
						EstimatedMemBytes: estimatedMemBytes,
						EstimationTier:    estimationTier,
						Cold:              cold,
						Detach:            detach,
						DetachCh:          detachCh,
						LeaseID:           leaseID,
						QueuedAt:          queuedAt,
					})
					if app.IsPermanent(err) {
						return coordinator.Permanent(err)
					}
					return err
				}

				statusFn := func(msg string) {
					fmt.Printf("[coordinator] %s\n", msg)
				}

				coord.SetOnPlacement(func(r *coordinator.LeaseResult) {
					reservedGPU.Store(int64(r.GPUIndex))
					placed.Store(r)
				})

				result, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
				if err != nil {
					return runOutcome(err)
				}

				fmt.Printf("[coordinator] Task completed successfully on node %s\n",
					result.NodeID[:min(8, len(result.NodeID))])
				return nil
			}

			// Default: execute on this machine (chunks may still spill to peers
			// through the shim). --host overrides with an explicit target.
			if daemonHost == "" {
				daemonHost = "127.0.0.1"
				targetID = nodeID.NodeID
				placementSource = "self"
			} else {
				resolvedHost, resolvedPort, resolvedID, err := resolveExplicitTarget(daemonPort, daemonHost)
				if err != nil {
					return err
				}
				daemonHost, daemonPort, targetID = resolvedHost, resolvedPort, resolvedID
				placementSource = "explicit"
			}

			acceptResp, err := acceptWithin(ctx, placementWait(), func() (*daemonctl.AcceptResponse, error) {
				return daemonctl.CheckRemoteAcceptance(daemonHost, daemonPort, targetID, jobName, nodeID.NodeID, estimatedMemBytes)
			})
			if err != nil {
				return err
			}
			fmt.Printf("remote daemon accepted job (lease=%s, expires=%s)\n",
				acceptResp.LeaseID[:min(8, len(acceptResp.LeaseID))], acceptResp.ExpiresAt)

			if err := daemonctl.CommitLease(daemonHost, daemonPort, acceptResp.LeaseID); err != nil {
				return fmt.Errorf("lease commit failed: %w", err)
			}
			fmt.Println("lease committed — starting execution")

			detachCh, stopSuspend := watchSuspend(ctx, true)
			defer stopSuspend()

			runErr := app.Run(ctx, app.Options{
				ScriptPath:        scriptPath,
				DaemonHost:        daemonHost,
				DaemonPort:        daemonPort,
				Submitter:         submitter,
				TargetID:          targetID,
				JobName:           jobName,
				Isolate:           isolate,
				Intercept:         true,
				GPU:               requestGPU,
				GPUDevices:        gpuID,
				PythonVersion:     pythonVersion,
				Envs:              envs,
				Pkgs:              pkgs,
				ScriptArgs:        args,
				PlacementSource:   placementSource,
				DegradedMode:      placementDegraded,
				CandidateCount:    placementCandidates,
				PlacementReason:   placementReason,
				EstimatedMemBytes: estimatedMemBytes,
				EstimationTier:    estimationTier,
				Cold:              cold,
				Detach:            detach,
				DetachCh:          detachCh,
				LeaseID:           acceptResp.LeaseID,
			})

			// A detached job still holds its lease; the node releases it
			// when the job ends.
			if !errors.Is(runErr, daemonctl.ErrDetached) {
				completeStatus := jobhistory.StateSucceeded
				switch {
				case errors.Is(runErr, context.Canceled):
					completeStatus = jobhistory.StateCancelled
				case runErr != nil:
					completeStatus = jobhistory.StateFailed
				}
				if err := daemonctl.CompleteLease(daemonHost, daemonPort, acceptResp.LeaseID, completeStatus); err != nil {
					fmt.Fprintf(os.Stderr, "could not complete lease %s: %v\n", acceptResp.LeaseID, err)
				}
			}

			return runOutcome(runErr)
		},
	}
	cmd.Flags().String("host", "", "Daemon host:port, or a registered node ID")
	cmd.Flags().Int("port", 38080, "Daemon API port")
	cmd.Flags().String("job-name", "", "Optional job name")
	cmd.Flags().Bool("isolate", true, "Run in OCI sandbox (crun)")
	cmd.Flags().String("gpu", "", "GPU mode: force (GPU node required), prefer (GPU first, CPU fallback), off (CPU only). Default: infer from script imports")
	cmd.Flags().String("gpu-id", "", "Pin specific GPU device IDs, e.g. '0' or '0,1' (default: whichever device the node reserves)")
	cmd.Flags().String("gpu-mem", "", "Minimum free VRAM required on a single GPU, e.g. '4G'")
	cmd.Flags().Int("cores", 0, "Minimum free CPU cores required on the target node")
	cmd.Flags().Bool("check-only", false, "Check imports, environment, placement and closure without running the script")
	cmd.Flags().Bool("cold", false, "Send the environment as if the node had none, to time a first run")
	cmd.Flags().Bool("detach", false, "Start the job, print its id and return; reattach with pipedpeer attach <id>")
	cmd.Flags().Bool("remote", false, "Run on another machine in the cluster (any node except this one); without this the script runs on this machine")
	cmd.Flags().Bool("offload", false, "Keep distributed work off this machine; the script itself still runs here. Pool/joblib/executor chunks, pandas and numpy offloads and DDP ranks go to other machines (this machine only when none can take them, with a warning). With --remote, the chosen node's spreading skips this machine too")
	cmd.Flags().String("python", "", "Python version")
	cmd.Flags().String("registry", viper.GetString("REGISTRY"), "Registry URL")
	cmd.Flags().StringSliceP("env", "e", nil, "Environment variables (e.g., -e API_KEY=123)")
	cmd.Flags().String("distribute", "auto", "Distribution policy: auto (cost model decides, never slower) or force (always ship interceptable ops to the cluster; for demos)")
	cmd.Flags().StringSlice("pkg", nil, "Packages or requirements.txt")
	cmd.Flags().String("mem", "", "Memory requirement override")
	cmd.Flags().String("strategy", "smart", "Placement strategy: smart (default) or round-robin")
	_ = cmd.Flags().MarkHidden("strategy") // round-robin is only set internally by the python shorthand
	cmd.Flags().Int("ddp", 0, "Distributed training: run up to this many ranks across the cluster (default: every eligible node joins the ring), DDP init transparently handled by the shim")
	cmd.Flags().Int("ddp-port", 0, "DDP rendezvous master port (default: 29500, next free up to +9; pin an exact port to override)")
	return cmd
}

// resolveExplicitTarget turns a --host value into (host, port, nodeID).
// "host:port" is used as-is, with the node ID discovered from its /health
// endpoint; a bare string that matches a registered node ID resolves to that
// node's endpoint; otherwise it is treated as a hostname.
func resolveExplicitTarget(daemonPort int, hostArg string) (string, int, string, error) {
	if strings.Contains(hostArg, ":") {
		var health struct {
			NodeID string `json:"node_id"`
		}
		resp, err := http.Get(fmt.Sprintf("http://%s/health", hostArg))
		if err != nil {
			return "", 0, "", fmt.Errorf("could not reach daemon at %s: %w", hostArg, err)
		}
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(&health)
		host, portStr, _ := strings.Cut(hostArg, ":")
		port, _ := strconv.Atoi(portStr)
		return host, port, health.NodeID, nil
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort))
	if err == nil {
		var all []registry.NodeRecord
		if err := json.NewDecoder(resp.Body).Decode(&all); err == nil {
			for _, n := range all {
				if n.NodeID == hostArg || strings.HasPrefix(n.NodeID, hostArg) {
					resp.Body.Close()
					return ddpExtractHost(n.SSHEndpoint), n.DaemonPort, n.NodeID, nil
				}
			}
		}
		resp.Body.Close()
	}
	return hostArg, daemonPort, "", nil
}

// runOutcome turns a run's error into the command's result: a detach is
// success (the job keeps running), a cancel says so in one line.
func runOutcome(err error) error {
	switch {
	case errors.Is(err, daemonctl.ErrDetached):
		return nil
	case errors.Is(err, context.Canceled):
		fmt.Println("cancelled — remote work stopped")
		return errCancelReported
	}
	return err
}

// errCancelReported fails the command (exit 1) without a second error line.
var errCancelReported = errors.New("cancelled")

// watchSuspend turns Ctrl+Z (SIGTSTP) into a detach for a single run: the
// returned channel closes on the first one. Where detach is not offered
// (DDP, map) Ctrl+Z says so and the run carries on. stop gives Ctrl+Z back
// to the shell.
func watchSuspend(ctx context.Context, detachable bool) (<-chan struct{}, func()) {
	detach := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTSTP)
	done := make(chan struct{})
	go func() {
		closed := false
		for {
			select {
			case <-sig:
				if !detachable {
					fmt.Println("detach is not supported for DDP/map runs; Ctrl+C stops it")
				} else if !closed {
					closed = true
					close(detach)
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	var once sync.Once
	return detach, func() {
		once.Do(func() {
			signal.Stop(sig)
			close(done)
		})
	}
}

// exitOnSecondSignal waits for the first Ctrl+C (which cancels ctx and lets
// the command clean up), then exits at once with a line on the second.
func exitOnSecondSignal(ctx context.Context, stop context.CancelFunc) {
	<-ctx.Done()
	stop()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	<-sig
	fmt.Fprintln(os.Stderr, "\nsecond interrupt: exiting now")
	os.Exit(130)
}
