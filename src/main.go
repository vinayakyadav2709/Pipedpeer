package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/discovery"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/ping"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/setup"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "__daemon__" {
		runDaemon(os.Args[2:])
		return
	}

	rootCmd := &cobra.Command{
		Use:   "pipedpeer",
		Short: "Decentralized compute orchestrator",
		Long:  "Pipedpeer — submit, schedule, and execute compute tasks across a peer-to-peer mesh.",
	}

	viper.SetEnvPrefix("PIPEDPEER")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	rootCmd.AddCommand(
		newStartCmd(),
		newStopCmd(),
		newStatusCmd(),
		newSetupCmd(),
		newRunCmd(),
		newDashboardCmd(),
		newJobsCmd(),
		newJobCmd(),
		newInitCmd(),
		newRegistryCmd(),
		newNodesCmd(),
		newPingCmd(),
		newTrafficCmd(),
		newTasksCmd(),
		newMapCmd(),
	)

	rootCmd.RunE = newRunCmd().RunE

	// Pre-process args to support `pipedpeer python script.py [args...]` shorthand
	args := os.Args[1:]
	if len(args) >= 2 && args[0] == "python" {
		scriptPath := args[1]
		if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: script file '%s' does not exist.\n", scriptPath)
			os.Exit(1)
		}

		// Rewrite into: run <script> --strategy round-robin --remote --isolate=false
		newArgs := []string{
			"run",
			scriptPath,
			"--strategy", "round-robin",
			"--remote",
			"--isolate=false",
		}

		// Append any remaining args
		if len(args) > 2 {
			newArgs = append(newArgs, args[2:]...)
		}
		args = newArgs
	}

	rootCmd.SetArgs(args)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Create a .pipedpeerignore file in the current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			content := `.git/
__pycache__/
.venv/
venv/
env/
node_modules/
`
			if err := os.WriteFile(".pipedpeerignore", []byte(content), 0644); err != nil {
				return err
			}
			fmt.Println("Created .pipedpeerignore")
			return nil
		},
	}
}

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the local daemon process",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := cmd.Flags().GetInt("port")
			maxConcurrent, _ := cmd.Flags().GetInt("max-concurrent")
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			if st := daemonctl.Status(); st.Running {
				fmt.Printf("daemon already running\n")
				return nil
			}
			if err := daemonctl.Start(nodeID.NodeID, port, maxConcurrent); err != nil {
				return err
			}
			if maxConcurrent > 0 {
				fmt.Printf("started daemon (node-id=%s, port=%d, max-concurrent=%d)\n",
					nodeID.ShortID(), port, maxConcurrent)
			} else {
				fmt.Printf("started daemon (node-id=%s, port=%d)\n", nodeID.ShortID(), port)
			}
			return nil
		},
	}
	cmd.Flags().Int("port", 38080, "Local daemon port")
	cmd.Flags().Int("max-concurrent", 0,
		"Maximum tasks this node accepts at once (0 = unlimited; also settable via PIPEDPEER_MAX_CONCURRENT)")
	return cmd
}

func newStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the local daemon process",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := daemonctl.Stop(); err != nil {
				return err
			}
			fmt.Println("stopped daemon")
			return nil
		},
	}
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show daemon status",
		RunE: func(cmd *cobra.Command, args []string) error {
			st := daemonctl.Status()
			if !st.Running {
				fmt.Println("daemon stopped")
				return nil
			}
			fmt.Printf("daemon running (pid=%d node-id=%s port=%d)\n", st.PID, st.NodeID, st.Port)
			return nil
		},
	}
}

func newSetupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Check prerequisites, install missing, and start daemon",
		Long: `Checks for required system tools (nix, tar, bash), optionally installs
missing ones, creates a node identity, and starts the local daemon.

Use --no-install to only check without modifying the system.
Use -y to skip the install confirmation prompt.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			autoYes, _ := cmd.Flags().GetBool("yes")
			noInstall, _ := cmd.Flags().GetBool("no-install")
			port, _ := cmd.Flags().GetInt("port")

			_, err := setup.Run(autoYes, noInstall, port)
			return err
		},
	}
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation, auto-install")
	cmd.Flags().Bool("no-install", false, "Check only, don't install anything")
	cmd.Flags().Int("port", 38080, "Daemon port")
	return cmd
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
			pythonVersion, _ := cmd.Flags().GetString("python")
			registryURL, _ := cmd.Flags().GetString("registry")
			memOverride, _ := cmd.Flags().GetString("mem")
			envs, _ := cmd.Flags().GetStringSlice("env")
			pkgs, _ := cmd.Flags().GetStringSlice("pkg")
			remote, _ := cmd.Flags().GetBool("remote")
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

			userMem := resourceest.ParseMemString(memOverride)
			resReq := resourceest.EstimateFromScript(scriptPath, nil, userMem)
			fmt.Printf("[coordinator] Resource estimate: %s (tier=%s)\n",
				resourceest.FormatBytes(resReq.MemBytes), resReq.Tier)

			// DDP: --ddp N caps the rank count; without it, DDP auto-activates
			// when the script uses torch.distributed and every eligible node
			// joins the ring. Min is always 1 (--ddp 1 is a plain run).
			// ponytail: no redundancy margin yet — on a large cluster with
			// flaky nodes, all-in means one death kills the run. If that ever
			// bites, add a heuristic here (spare ranks, or eligible-N).
			if ddpNodes > 1 || (ddpNodes == 0 && pythondeps.HasDistributedImports(scriptPath)) {
				eligible := ddpEligibleCount(daemonPort, requireGPU, requiredGPUMem, resReq.MemBytes)
				worldSize := eligible
				if ddpNodes > 1 {
					worldSize = min(ddpNodes, eligible)
				}
				if worldSize >= 2 {
					return runDDP(ddpRunOptions{
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
						NoSelf:         remote,
						Strategy:       strategy,
						MemOverride:    memOverride,
					})
				}
				if ddpNodes > 1 {
					fmt.Printf("[coordinator] fewer than 2 eligible nodes — running single-node\n")
				}
			}

			estimatedMemBytes := resReq.MemBytes
			estimationTier := resReq.Tier
			var placementSource, placementReason string
			var placementDegraded bool
			var placementCandidates int

			if remote {
				fmt.Println("[coordinator] --remote: discovering a node other than this machine...")

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
					NoSelf:           true,
					RequireGPU:       requireGPU,
					PreferGPU:        preferGPU,
					Strategy:         strategy,
					RoundRobinFn:     rrFn,
				})

				// A remote run must fail loudly when there is no node at all
				// instead of queuing forever: the user can just drop --remote.
				decision := coord.FindNode()
				if decision.ChosenNode.NodeID == "" {
					return fmt.Errorf("no eligible node found — run without --remote to execute on this machine")
				}

				if checkOnly {
					fmt.Printf("[coordinator] Would place on node %s (%s)\n",
						decision.ChosenNode.NodeID[:min(8, len(decision.ChosenNode.NodeID))], decision.Reason)
					fmt.Println("checks complete")
					return nil
				}

				ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
				defer cancel()

				scriptArgs := args

				// The node tells us which GPU it reserved; honour that unless
				// the user pinned devices explicitly with --gpu-id.
				var reservedGPU atomic.Int64
				reservedGPU.Store(-1)

				executor := func(host string, port int, targetNodeID string) error {
					devices := gpuID
					if devices == "" {
						if idx := reservedGPU.Load(); idx >= 0 {
							devices = strconv.FormatInt(idx, 10)
						}
					}
					return app.Run(app.Options{
						ScriptPath:        scriptPath,
						DaemonHost:        host,
						DaemonPort:        port,
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
						PlacementSource:   "coordinator",
						EstimatedMemBytes: estimatedMemBytes,
						EstimationTier:    estimationTier,
					})
				}

				statusFn := func(msg string) {
					fmt.Printf("[coordinator] %s\n", msg)
				}

				coord.SetOnPlacement(func(r *coordinator.LeaseResult) {
					reservedGPU.Store(int64(r.GPUIndex))
				})

				result, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
				if err != nil {
					return err
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

			acceptResp, err := daemonctl.CheckRemoteAcceptance(daemonHost, daemonPort, targetID, jobName, nodeID.NodeID, estimatedMemBytes)
			if err != nil {
				return err
			}
			fmt.Printf("remote daemon accepted job (lease=%s, expires=%s)\n",
				acceptResp.LeaseID[:min(8, len(acceptResp.LeaseID))], acceptResp.ExpiresAt)

			if err := daemonctl.CommitLease(daemonHost, daemonPort, acceptResp.LeaseID); err != nil {
				return fmt.Errorf("lease commit failed: %w", err)
			}
			fmt.Println("lease committed — starting execution")

			if checkOnly {
				fmt.Println("checks complete")
				_ = daemonctl.CompleteLease(daemonHost, daemonPort, acceptResp.LeaseID, "cancelled")
				return nil
			}

			runErr := app.Run(app.Options{
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
			})

			completeStatus := jobhistory.StateSucceeded
			if runErr != nil {
				completeStatus = jobhistory.StateFailed
			}
			_ = daemonctl.CompleteLease(daemonHost, daemonPort, acceptResp.LeaseID, completeStatus)

			return runErr
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
	cmd.Flags().Bool("check-only", false, "Only check acceptance, don't execute")
	cmd.Flags().Bool("remote", false, "Run on another machine in the cluster (any node except this one); without this the script runs on this machine")
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

type ddpRunOptions struct {
	Nodes          int
	DaemonPort     int
	MasterPort     int
	ScriptPath     string
	ScriptArgs     []string
	JobName        string
	Isolate        bool
	RegistryURL    string
	PythonVersion  string
	Pkgs           []string
	Envs           []string
	GPUDevices     string
	RequireGPU     bool
	PreferGPU      bool
	RequiredGPUMem int64
	RequiredCores  int
	NoSelf         bool
	Strategy       string
	MemOverride    string
}

// runDDP launches a transparent multi-rank training run: rank 0 is placed by
// the coordinator, ranks 1..K-1 come from the daemon's healthy node store,
// and every rank gets RANK/WORLD_SIZE/MASTER_ADDR/MASTER_PORT plus
// PIPEDPEER_DDP=1, which makes the sitecustomize shim initialize the torch
// process group and wrap DistributedDataParallel without touching user code.
func runDDP(o ddpRunOptions) error {
	absScript, err := filepath.Abs(o.ScriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		return err
	}
	if _, err := daemonctl.EnsureStarted(nodeID.NodeID, o.DaemonPort); err != nil {
		return err
	}

	userMem := resourceest.ParseMemString(o.MemOverride)
	resReq := resourceest.EstimateFromScript(o.ScriptPath, nil, userMem)

	rrFn := func(count int) int {
		url := fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", o.DaemonPort, count)
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
		RegistryURL:      o.RegistryURL,
		SelfIdentity:     nodeID,
		SelfSSH:          fmt.Sprintf("root@%s:22", detectLocalIP()),
		SelfDaemon:       o.DaemonPort,
		SelfLoad:         heartbeat.CollectLoad(0, 0),
		JobName:          o.JobName,
		RequiredMemBytes: resReq.MemBytes,
		RequiredCores:    o.RequiredCores,
		RequiredGPUMem:   o.RequiredGPUMem,
		NoSelf:           o.NoSelf,
		RequireGPU:       o.RequireGPU,
		PreferGPU:        o.PreferGPU,
		Strategy:         o.Strategy,
		RoundRobinFn:     rrFn,
	})

	decision := coord.FindNode()
	rank0 := decision.ChosenNode
	if rank0.NodeID == "" {
		return fmt.Errorf("no node eligible for rank 0: %s", decision.Reason)
	}
	if rank0.DaemonPort == 0 {
		rank0.DaemonPort = o.DaemonPort
	}

	peers := make([]registry.NodeRecord, 0, o.Nodes-1)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", o.DaemonPort))
	if err == nil {
		var all []registry.NodeRecord
		if jerr := json.NewDecoder(resp.Body).Decode(&all); jerr == nil {
			var gpuPeers, cpuPeers []registry.NodeRecord
			for _, n := range all {
				if n.State != "healthy" || n.NodeID == rank0.NodeID {
					continue
				}
				if o.RequireGPU && !ddpHasGPU(n) {
					continue
				}
				if o.PreferGPU && ddpHasGPU(n) {
					gpuPeers = append(gpuPeers, n)
				} else {
					cpuPeers = append(cpuPeers, n)
				}
			}
			// Prefer GPU peers when GPU work is wanted (force or inferred),
			// falling back to CPU peers only if there aren't enough GPU ones —
			// a DDP all-reduce must not mix devices silently.
			sort.Slice(gpuPeers, func(i, j int) bool {
				return gpuPeers[i].Load.CPUPercent < gpuPeers[j].Load.CPUPercent
			})
			sort.Slice(cpuPeers, func(i, j int) bool {
				return cpuPeers[i].Load.CPUPercent < cpuPeers[j].Load.CPUPercent
			})
			peers = append(gpuPeers, cpuPeers...)
		}
		resp.Body.Close()
	}
	if len(peers) < o.Nodes-1 {
		return fmt.Errorf("need %d healthy peers for ranks 1..%d (found %d) — join more nodes or lower --ddp",
			o.Nodes-1, o.Nodes-1, len(peers))
	}
	peers = peers[:o.Nodes-1]

	masterAddr := ddpExtractHost(rank0.SSHEndpoint)
	if masterAddr == "" || masterAddr == "127.0.0.1" || masterAddr == "localhost" {
		masterAddr = detectLocalIP()
	}
	masterPort := o.MasterPort
	if masterPort == 0 {
		// ponytail: fixed base port (torch convention 29500) so a firewall is
		// opened ONCE for the small range instead of per-run. The port is
		// negotiated live against the leader host: connection refused =
		// nothing listening = safe to claim; occupied = next in range. Two
		// simultaneous DDP runs whose leaders land on the same machine fit
		// as long as they stay inside the span — beyond that, pin an exact
		// --ddp-port. Future zero-firewall option: tunnel the rendezvous
		// through the daemons' already-open 38080 channel (~a day of work,
		// makes the orchestrator a relay, extra hop to debug).
		masterPort = ddpPickMasterPort(masterAddr, 29500, 10)
	}

	fmt.Printf("=== Distributed training: %d ranks ===\n", o.Nodes)
	fmt.Printf("rank 0: %s (%s:%d)\n", rank0.NodeID[:min(8, len(rank0.NodeID))], masterAddr, rank0.DaemonPort)
	for i, n := range peers {
		fmt.Printf("rank %d: %s (%s:%d)\n", i+1, n.NodeID[:min(8, len(n.NodeID))],
			ddpExtractHost(n.SSHEndpoint), n.DaemonPort)
	}
	fmt.Printf("MASTER_ADDR=%s MASTER_PORT=%d backend=gloo\n\n", masterAddr, masterPort)

	env, err := app.BuildEnvironment(absScript, app.EnvOptions{
		PythonVersion: o.PythonVersion,
		Pkgs:          o.Pkgs,
		Intercept:     true, // DDP transparency lives in the sitecustomize shim
	}, func(step int, title string) {
		if step == 1 {
			fmt.Printf("[%d/7] %s\n", step, title)
		} else {
			fmt.Printf("\n[%d/7] %s\n", step, title)
		}
	})
	if err != nil {
		return err
	}
	defer env.Close()

	jobSet := "ddp-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if o.JobName != "" {
		jobSet = "ddp-" + o.JobName
	}

	ranks := make([]registry.NodeRecord, 0, o.Nodes)
	ranks = append(ranks, rank0)
	ranks = append(ranks, peers...)

	errs := make(chan error, o.Nodes)
	for i, n := range ranks {
		go func(i int, n registry.NodeRecord) {
			rankEnvs := append([]string(nil), o.Envs...)
			rankEnvs = append(rankEnvs,
				"PIPEDPEER_DDP=1",
				fmt.Sprintf("PIPEDPEER_RANK=%d", i),
				fmt.Sprintf("PIPEDPEER_WORLD_SIZE=%d", o.Nodes),
				fmt.Sprintf("RANK=%d", i),
				fmt.Sprintf("WORLD_SIZE=%d", o.Nodes),
				fmt.Sprintf("MASTER_ADDR=%s", masterAddr),
				fmt.Sprintf("MASTER_PORT=%d", masterPort),
				fmt.Sprintf("PIPEDPEER_SUBMITTER=%s", net.JoinHostPort(detectLocalIP(), strconv.Itoa(o.DaemonPort))),
			)
			opts := app.Options{
				ScriptPath:        absScript,
				DaemonHost:        ddpExtractHost(n.SSHEndpoint),
				DaemonPort:        n.DaemonPort,
				TargetID:          n.NodeID,
				JobName:           o.JobName,
				JobSet:            jobSet,
				Isolate:           o.Isolate,
				Intercept:         true,
				SkipBroadcast:     true,
				GPU:               o.PreferGPU || o.RequireGPU,
				GPUDevices:        o.GPUDevices,
				Mode:              "script",
				PythonVersion:     o.PythonVersion,
				Envs:              rankEnvs,
				Pkgs:              o.Pkgs,
				ScriptArgs:        o.ScriptArgs,
				ResultsDir:        filepath.Join(".", "results", fmt.Sprintf("ddp-rank-%d", i)),
				PlacementSource:   "ddp",
				EstimatedMemBytes: resReq.MemBytes,
				EstimationTier:    resReq.Tier,
			}
			errs <- app.RunTask(env, app.Task{Options: opts, Script: absScript, Label: fmt.Sprintf("rank-%d", i)})
		}(i, n)
	}

	var firstErr error
	for i := 0; i < o.Nodes; i++ {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = fmt.Errorf("rank %d failed: %w", i, err)
		}
	}
	if firstErr != nil {
		fmt.Printf("[ddp] hint: if a rank timed out rendezvousing on %s:%d, the port range is likely firewalled — open it once with `sudo ufw allow 29500:29510/tcp` on the rank-0 machine, or pin an exact port with --ddp-port\n",
			masterAddr, masterPort)
	}
	return firstErr
}

// ddpPickMasterPort picks the DDP rendezvous port by probing the leader host
// live: an accepted dial means occupied (skip to the next), connection refused
// means nothing is listening there yet — safe to claim for rank 0's store. A
// dial timeout is ambiguous (firewall drop) and indistinguishable across the
// whole range, so return the base and let the shim's bounded rendezvous
// timeout surface it as a clear failure instead of hanging forever.
func ddpPickMasterPort(host string, base, span int) int {
	for p := base; p < base+span; p++ {
		c, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(p)), time.Second)
		if err != nil {
			return p
		}
		c.Close()
	}
	return base
}

func ddpHasGPU(n registry.NodeRecord) bool {
	if n.Capabilities["gpu"] != "" && n.Capabilities["gpu"] != "none" {
		return true
	}
	return n.Load.GPUModel != "" || len(n.Load.GPUs) > 0
}

// ddpEligibleCount returns how many healthy nodes could host a DDP rank.
// GPU nodes count when they fit the VRAM requirement; CPU nodes count as a
// fallback when they fit the job's memory (plan C1: GPU preferred, CPU only
// when it fits). requireGPU (--gpu force) hard-excludes CPU nodes.
func ddpEligibleCount(daemonPort int, requireGPU bool, requiredGPUMem, requiredMem int64) int {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var all []registry.NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return 0
	}
	count := 0
	for _, n := range all {
		if n.State != "healthy" {
			continue
		}
		gpuOK := ddpHasGPU(n) &&
			(requiredGPUMem <= 0 || n.Load.GPUMemBytes-n.Load.GPUMemUsedBytes >= requiredGPUMem)
		cpuOK := !requireGPU &&
			(requiredMem <= 0 || n.Load.AvailableMemBytes >= requiredMem)
		if gpuOK || cpuOK {
			count++
		}
	}
	return count
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

func ddpExtractHost(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = s[idx+1:]
	}
	if idx := strings.Index(s, ":"); idx >= 0 {
		s = s[:idx]
	}
	return s
}

func newMapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "map",
		Short: "Scatter one script across many shards/inputs and gather the results",
		Long: "Build a Python environment once, then run the script as many tasks across " +
			"the cluster, one per input file / args-file line / data shard. Each task gets " +
			"its own results directory and PIPEDPEER_SHARD_ID/NUM_SHARDS envs.",
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptPath, _ := cmd.Flags().GetString("script")
			daemonPort, _ := cmd.Flags().GetInt("port")
			registryURL, _ := cmd.Flags().GetString("registry")
			isolate, _ := cmd.Flags().GetBool("isolate")
			gpuMode, _ := cmd.Flags().GetString("gpu")
			gpuID, _ := cmd.Flags().GetString("gpu-id")
			gpuMemStr, _ := cmd.Flags().GetString("gpu-mem")
			remote, _ := cmd.Flags().GetBool("remote")
			strategy, _ := cmd.Flags().GetString("strategy")
			envs, _ := cmd.Flags().GetStringSlice("env")
			pkgs, _ := cmd.Flags().GetStringSlice("pkg")
			pythonVersion, _ := cmd.Flags().GetString("python")
			concurrency, _ := cmd.Flags().GetInt("concurrency")
			resultsDir, _ := cmd.Flags().GetString("results-dir")
			reduce, _ := cmd.Flags().GetString("reduce")

			if scriptPath == "" {
				return fmt.Errorf("--script is required")
			}

			spec := app.MapSpec{
				ScriptPath:  scriptPath,
				Inputs:      mustStringSlice(cmd, "inputs"),
				ArgsFile:    mustString(cmd, "args-file"),
				SplitSource: mustString(cmd, "input"),
				Split:       mustString(cmd, "split"),
			}

			tasks, shardDir, err := app.ResolveMapTasks(spec)
			if err != nil {
				return err
			}
			if shardDir != "" {
				defer os.RemoveAll(shardDir)
			}
			if len(tasks) == 0 {
				return fmt.Errorf("no tasks to run")
			}

			if resultsDir == "" {
				resultsDir = filepath.Join(".", "results", "map-"+strconv.FormatInt(time.Now().UnixNano(), 36))
			}
			if err := os.MkdirAll(resultsDir, 0755); err != nil {
				return err
			}

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
			if _, err := daemonctl.EnsureStarted(nodeID.NodeID, daemonPort); err != nil {
				return err
			}

			userMem := resourceest.ParseMemString(mustString(cmd, "mem"))
			resReq := resourceest.EstimateFromScript(scriptPath, nil, userMem)
			fmt.Printf("[coordinator] Resource estimate: %s (tier=%s)\n",
				resourceest.FormatBytes(resReq.MemBytes), resReq.Tier)

			rrFn := func(count int) int {
				url := fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", daemonPort, count)
				resp, err := http.Get(url)
				if err != nil {
					return 0
				}
				defer resp.Body.Close()
				var r struct{ Index int }
				json.NewDecoder(resp.Body).Decode(&r)
				return r.Index
			}

			coord := coordinator.New(coordinator.Config{
				RegistryURL:      registryURL,
				SelfIdentity:     nodeID,
				SelfSSH:          fmt.Sprintf("root@%s:22", detectLocalIP()),
				SelfDaemon:       daemonPort,
				SelfLoad:         heartbeat.CollectLoad(0, 0),
				JobName:          "map",
				RequiredMemBytes: resReq.MemBytes,
				RequiredGPUMem:   requiredGPUMem,
				NoSelf:           remote,
				RequireGPU:       requireGPU,
				PreferGPU:        preferGPU,
				Strategy:         strategy,
				RoundRobinFn:     rrFn,
			})

			env, err := app.BuildEnvironment(scriptPath, app.EnvOptions{
				PythonVersion: pythonVersion,
				Pkgs:          pkgs,
			}, func(step int, title string) {
				if step == 1 {
					fmt.Printf("[%d/7] %s\n", step, title)
				} else {
					fmt.Printf("\n[%d/7] %s\n", step, title)
				}
			})
			if err != nil {
				return err
			}
			defer env.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
			defer cancel()

			base := app.Options{
				DaemonHost:        "",
				DaemonPort:        daemonPort,
				Submitter:         net.JoinHostPort(detectLocalIP(), strconv.Itoa(daemonPort)),
				Isolate:           isolate,
				GPU:               requestGPU,
				GPUDevices:        gpuID,
				Envs:              envs,
				Pkgs:              pkgs,
				PlacementSource:   "coordinator",
				EstimatedMemBytes: resReq.MemBytes,
				EstimationTier:    resReq.Tier,
			}

			run := func(task app.MapTask, o app.Options) (string, error) {
				o.ScriptPath = scriptPath
				executor := func(host string, port int, targetNodeID string) error {
					o.DaemonHost = host
					o.DaemonPort = port
					o.TargetID = targetNodeID
					return app.RunTask(env, app.Task{Options: o, Script: scriptPath, Label: fmt.Sprintf("task-%d", task.ShardID)})
				}
				statusFn := func(msg string) {}
				res, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
				if err != nil {
					return "", err
				}
				return res.NodeID, nil
			}

			return app.RunMap(tasks, base, concurrency, resultsDir, env, run, reduce)
		},
	}
	cmd.Flags().String("script", "", "Path to Python script (required)")
	cmd.Flags().StringSlice("inputs", nil, "Glob pattern(s); one task per matched file")
	cmd.Flags().String("args-file", "", "File with one task's args per line")
	cmd.Flags().String("input", "", "Record-oriented file to split (csv/jsonl/ndjson/txt)")
	cmd.Flags().String("split", "parts:auto", "Split strategy: rows:N or parts:N (or parts:auto)")
	cmd.Flags().Int("concurrency", 0, "Max concurrent tasks (default: all)")
	cmd.Flags().String("results-dir", "", "Where per-task results are written")
	cmd.Flags().String("reduce", "", "Script run locally over gathered results")
	cmd.Flags().Int("port", 38080, "Daemon API port")
	cmd.Flags().Bool("isolate", true, "Run in OCI sandbox (crun)")
	cmd.Flags().Bool("remote", false, "Only scatter tasks to other machines (excludes this one)")
	cmd.Flags().String("gpu", "", "GPU mode: force, prefer, off")
	cmd.Flags().String("gpu-id", "", "Pin GPU device IDs")
	cmd.Flags().String("gpu-mem", "", "Minimum free VRAM on a single GPU")
	cmd.Flags().String("registry", viper.GetString("REGISTRY"), "Registry URL")
	cmd.Flags().StringSliceP("env", "e", nil, "Environment variables (e.g., -e API_KEY=123)")
	cmd.Flags().StringSlice("pkg", nil, "Packages or requirements.txt")
	cmd.Flags().String("python", "", "Python version")
	cmd.Flags().String("mem", "", "Memory requirement override")
	cmd.Flags().String("strategy", "smart", "Placement strategy: smart (default) or round-robin")
	_ = cmd.Flags().MarkHidden("strategy") // round-robin is only set internally by the python shorthand
	return cmd
}

func mustString(cmd *cobra.Command, name string) string {
	v, _ := cmd.Flags().GetString(name)
	return v
}

func mustStringSlice(cmd *cobra.Command, name string) []string {
	v, _ := cmd.Flags().GetStringSlice(name)
	return v
}

func newDashboardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Live unified view of all workers, peers, and recent tasks (Ctrl+C / q to exit)",
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort, _ := cmd.Flags().GetInt("port")
			interval, _ := cmd.Flags().GetDuration("interval")

			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}

			daemonctl.EnsureStarted(nodeID.NodeID, daemonPort)

			st := daemonctl.Status()
			m := dashModel{
				daemonPort: daemonPort,
				nodeID:     nodeID,
				interval:   interval,
				running:    st.Running,
			}
			m.refresh()

			p := tea.NewProgram(m, tea.WithAltScreen())
			_, err = p.Run()
			return err
		},
	}
	cmd.Flags().Int("port", 38080, "Daemon API port")
	cmd.Flags().Duration("interval", 2*time.Second, "Refresh interval (e.g. 1s, 2s, 500ms)")
	return cmd
}
func newJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "List recent job history",
		RunE: func(cmd *cobra.Command, args []string) error {
			limit, _ := cmd.Flags().GetInt("limit")
			historyPath := jobhistory.BaseDir()
			if err := os.MkdirAll(historyPath, 0755); err != nil {
				return err
			}
			items, err := jobhistory.List(limit)
			if err != nil {
				return err
			}
			if len(items) == 0 {
				fmt.Println("no jobs found yet (run a non-check-only job first)")
				fmt.Printf("history path: %s\n", historyPath)
				return nil
			}
			fmt.Printf("history path: %s\n", historyPath)
			fmt.Println("ID\tSTATUS\tMODE\tJOB\tJOB_SET\tTARGET\tHOST\tDURATION_MS\tSTARTED")
			for _, it := range items {
				mode := "fg"
				if it.Detached {
					mode = "bg"
				}
				fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", it.ID, it.Status, mode, it.JobName, it.JobSet, it.TargetID, it.Remote, it.DurationMs, it.StartedAt)
			}
			return nil
		},
	}
	pruneCmd := &cobra.Command{
		Use:   "prune",
		Short: "Delete job history entries not touched within --older-than",
		RunE: func(cmd *cobra.Command, args []string) error {
			olderThan, _ := cmd.Flags().GetDuration("older-than")
			n, err := jobhistory.Prune(olderThan)
			if err != nil {
				return err
			}
			fmt.Printf("pruned %d job history entr(ies) older than %s\n", n, olderThan)
			return nil
		},
	}
	pruneCmd.Flags().Duration("older-than", 7*24*time.Hour,
		"Delete entries untouched for this long")
	cmd.AddCommand(pruneCmd)
	cmd.Flags().Int("limit", 20, "Max jobs to show")
	return cmd
}

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Show details for a specific job",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			withOutput, _ := cmd.Flags().GetBool("output")
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			r, dir, err := jobhistory.ReadRecord(id)
			if err != nil {
				return err
			}
			fmt.Printf("id: %s\n", r.ID)
			fmt.Printf("status: %s\n", r.Status)
			fmt.Printf("error: %s\n", r.Error)
			fmt.Printf("started_at: %s\n", r.StartedAt)
			fmt.Printf("finished_at: %s\n", r.FinishedAt)
			fmt.Printf("duration_ms: %d\n", r.DurationMs)
			fmt.Printf("script_path: %s\n", r.ScriptPath)
			fmt.Printf("remote: %s\n", r.Remote)
			fmt.Printf("target_id: %s\n", r.TargetID)
			fmt.Printf("detached: %t\n", r.Detached)
			fmt.Printf("isolate: %t\n", r.Isolate)
			fmt.Printf("job_name: %s\n", r.JobName)
			fmt.Printf("run_host: %s\n", r.RunHost)
			fmt.Printf("store_path: %s\n", r.StorePath)
			fmt.Printf("remote_job_dir: %s\n", r.RemoteJobDir)
			fmt.Printf("local_sync_root: %s\n", r.LocalSyncRoot)
			fmt.Printf("received_files: %d\n", r.ReceivedFiles)
			fmt.Printf("new_files: %d\n", r.NewFiles)
			fmt.Printf("updated_files: %d\n", r.UpdatedFiles)
			fmt.Printf("deleted_files: %d\n", r.DeletedFiles)
			fmt.Printf("unchanged_files: %d\n", r.UnchangedFiles)
			fmt.Printf("manifest_path: %s\n", r.ManifestPath)
			fmt.Printf("history_dir: %s\n", dir)
			if withOutput {
				stdout, _ := jobhistory.ReadOptionalText(dir, "stdout.log")
				stderr, _ := jobhistory.ReadOptionalText(dir, "stderr.log")
				remoteLogs, _ := jobhistory.ReadOptionalText(dir, "remote_logs.txt")
				fmt.Println("--- stdout ---")
				if strings.TrimSpace(stdout) == "" {
					fmt.Println("(empty)")
				} else {
					fmt.Println(stdout)
				}
				fmt.Println("--- stderr ---")
				if strings.TrimSpace(stderr) == "" {
					fmt.Println("(empty)")
				} else {
					fmt.Println(stderr)
				}
				if strings.TrimSpace(remoteLogs) != "" {
					fmt.Println("--- remote logs ---")
					fmt.Println(remoteLogs)
				}
			}
			fmt.Printf("artifacts: %s\n", filepath.Join(dir, "metadata.json"))
			return nil
		},
	}
	cmd.Flags().String("id", "", "Job ID")
	cmd.Flags().Bool("output", false, "Print stdout/stderr")
	return cmd
}

func newRegistryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "Start a standalone node registry server",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := cmd.Flags().GetInt("port")
			reg := registry.New(registry.DefaultConfig())
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
			go func() {
				<-sig
				reg.Stop()
				os.Exit(0)
			}()
			fmt.Printf("Registry listening on :%d\n", port)
			return reg.ListenAndServe(port)
		},
	}
	cmd.Flags().Int("port", 38090, "Registry listen port")
	return cmd
}

func newNodesCmd() *cobra.Command {
	// Bare "pipedpeer nodes" lists; subcommands manage membership.
	listRun := func(cmd *cobra.Command, args []string) error {
		daemonPort, _ := cmd.Flags().GetInt("port")
		return printNodes(daemonPort)
	}

	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List cluster nodes and manage manual registrations",
		Long:  "Nodes are auto-discovered on the LAN. Use 'nodes add' to register workers that discovery cannot reach.",
		RunE:  listRun,
	}
	cmd.PersistentFlags().Int("port", 38080, "Local daemon API port")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all known nodes",
		RunE:  listRun,
	}

	addCmd := &cobra.Command{
		Use:   "add <host> <port>",
		Short: "Register a worker node",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort, _ := cmd.Flags().GetInt("port")
			host := args[0]
			port, err := strconv.Atoi(args[1])
			if err != nil || port < 1 || port > 65535 {
				return fmt.Errorf("invalid port: %s", args[1])
			}
			body, _ := json.Marshal(map[string]interface{}{"host": host, "port": port})
			resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort),
				"application/json", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("daemon unreachable: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return fmt.Errorf("daemon rejected: %d", resp.StatusCode)
			}
			fmt.Printf("added node %s:%d\n", host, port)
			return nil
		},
	}

	removeCmd := &cobra.Command{
		Use:   "remove [host]",
		Short: "Remove nodes (--all clears everything, or give a host for manual entries)",
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort, _ := cmd.Flags().GetInt("port")
			all, _ := cmd.Flags().GetBool("all")

			target := "_all"
			if !all {
				if len(args) != 1 {
					return fmt.Errorf("requires <host> argument (or use --all)")
				}
				target = args[0]
			}

			req, _ := http.NewRequest("DELETE",
				fmt.Sprintf("http://127.0.0.1:%d/v1/nodes/%s", daemonPort, target), nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("daemon unreachable: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return fmt.Errorf("daemon rejected: %d", resp.StatusCode)
			}
			if all {
				fmt.Println("removed all nodes")
			} else {
				fmt.Printf("removed nodes at %s\n", target)
			}
			return nil
		},
	}
	removeCmd.Flags().Bool("all", false, "Remove all nodes (both manual and auto-discovered)")

	cmd.AddCommand(listCmd, addCmd, removeCmd)
	return cmd
}

// printNodes renders the daemon's view of the cluster, including the hardware
// dimensions the scheduler actually weighs.
func printNodes(daemonPort int) error {
	nodes, err := fetchClusterNodes(daemonPort)
	if err != nil {
		fmt.Println("No nodes found (daemon unreachable — start it with: pipedpeer start)")
		return nil
	}
	if len(nodes) == 0 {
		fmt.Println("No nodes found")
		return nil
	}

	fmt.Printf("%-10s %-12s %-10s %-6s %-6s %-10s %-14s %s\n",
		"NODE_ID", "HOST", "STATE", "JOBS", "CORES", "MEM AVAIL", "GPU", "SOURCE")
	for _, n := range nodes {
		gpuDesc := "-"
		if name := n.Capabilities["gpu_name"]; name != "" {
			gpuDesc = truncate(name, 14)
		}
		host := n.Capabilities["hostname"]
		if host == "" {
			host = hostFromEndpoint(n.SSHEndpoint)
		}
		cores := n.Capabilities["cpu_cores"]
		if cores == "" {
			cores = "-"
		}
		fmt.Printf("%-10s %-12s %-10s %-6d %-6s %-10s %-14s %s\n",
			shortID(n.NodeID), truncate(host, 12), n.State, n.Load.ActiveJobs, cores,
			resourceest.FormatBytes(n.Load.AvailableMemBytes), gpuDesc, n.Source)
	}
	return nil
}

func newTasksCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tasks",
		Aliases: []string{"ps"},
		Short:   "List tasks running across the cluster and the node running each",
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort, _ := cmd.Flags().GetInt("port")
			watch, _ := cmd.Flags().GetBool("watch")
			interval, _ := cmd.Flags().GetDuration("interval")

			render := func() error {
				tasks, err := fetchClusterTasks(daemonPort)
				if err != nil {
					return err
				}
				if len(tasks) == 0 {
					fmt.Println("no tasks running")
					return nil
				}
				fmt.Printf("%-10s %-16s %-20s %-10s %-10s %-8s %s\n",
					"LEASE", "NODE", "JOB", "STATE", "SUBMITTER", "GPU", "AGE")
				for _, t := range tasks {
					gpu := "-"
					if t.GPUIndex >= 0 {
						gpu = strconv.Itoa(t.GPUIndex)
					}
					job := t.JobName
					if job == "" {
						job = "-"
					}
					fmt.Printf("%-10s %-16s %-20s %-10s %-10s %-8s %s\n",
						shortID(t.LeaseID), t.Hostname, truncate(job, 20), t.State,
						shortID(t.Submitter), gpu, t.Age.Truncate(time.Second))
				}
				return nil
			}

			if !watch {
				return render()
			}
			for {
				fmt.Print("\033[H\033[2J") // clear screen
				if err := render(); err != nil {
					fmt.Printf("error: %v\n", err)
				}
				time.Sleep(interval)
			}
		},
	}
	cmd.Flags().Int("port", 38080, "Local daemon API port")
	cmd.Flags().Bool("watch", false, "Refresh continuously")
	cmd.Flags().Duration("interval", 2000*time.Millisecond, "Refresh interval (e.g. 500ms, 2000ms)")
	return cmd
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// newTrafficCmd renders this node's pool-map traffic ledger from the daemon
// log: every chunk received (and from whom), every chunk forwarded to a peer,
// and every fan-out split. Run it on any node to see that node's side of the
// ledger. The shim's side (what your run sent) is in the job output instead:
// pipedpeer job --id <ID> --output.
func newTrafficCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "traffic",
		Short: "Show this node's pool-map traffic ledger",
		Long:  "Prints the [pool] lines from this node's daemon log.",
		RunE: func(cmd *cobra.Command, args []string) error {
			watch, _ := cmd.Flags().GetBool("watch")
			limit, _ := cmd.Flags().GetInt("limit")
			logFile := daemonctl.DefaultPaths().LogFile
			if _, err := os.Stat(logFile); err != nil {
				return fmt.Errorf("daemon log not found at %s (is the daemon running?)", logFile)
			}
			if watch {
				return tailTraffic(logFile)
			}
			data, err := os.ReadFile(logFile)
			if err != nil {
				return err
			}
			var out []string
			for _, line := range strings.Split(string(data), "\n") {
				if strings.Contains(line, "[pool]") {
					out = append(out, line)
				}
			}
			if len(out) > limit {
				out = out[len(out)-limit:]
			}
			for _, line := range out {
				fmt.Println(line)
			}
			return nil
		},
	}
	cmd.Flags().BoolP("watch", "f", false, "Follow new entries (tail -f)")
	cmd.Flags().Int("limit", 50, "Max entries to show")
	return cmd
}

// tailTraffic follows the daemon log from the end, printing [pool] lines as
// they arrive. Ponytail: a plain poll loop; log file is written by the
// daemonctl-managed process, no rotation handling (the daemon re-opens on
// restart and Start() truncates).
func tailTraffic(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	buf := make([]byte, 4096)
	var pending []byte
	for {
		n, err := f.Read(buf)
		if n > 0 {
			pending = append(pending, buf[:n]...)
			for {
				idx := bytes.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := pending[:idx]
				pending = pending[idx+1:]
				if bytes.Contains(line, []byte("[pool]")) {
					fmt.Println(string(line))
				}
			}
		}
		if err != nil {
			time.Sleep(time.Second)
		}
	}
}

func newPingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ping <host:port> [host:port ...]",
		Short: "Live health dashboard for workers",
		Long:  "Polls /health on each worker every second and displays a live status table.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			interval, _ := cmd.Flags().GetDuration("interval")
			return ping.Run(args, interval)
		},
	}
	cmd.Flags().Duration("interval", 1000*time.Millisecond, "Poll interval (e.g. 1000ms)")
	return cmd
}

func runDaemon(args []string) {
	log := logging.WithComponent("daemon")

	var port int
	var maxConcurrent = -1
	var registryURL, sshEndpoint, natsURL string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &port)
				i++
			}
		case "--max-concurrent":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &maxConcurrent)
				i++
			}
		case "--registry":
			if i+1 < len(args) {
				registryURL = args[i+1]
				i++
			}
		case "--ssh-endpoint":
			if i+1 < len(args) {
				sshEndpoint = args[i+1]
				i++
			}
		case "--nats-url":
			if i+1 < len(args) {
				natsURL = args[i+1]
				i++
			}
		}
	}
	if port == 0 {
		port = 38080
	}
	if registryURL == "" {
		registryURL = os.Getenv("PIPEDPEER_REGISTRY")
	}
	if natsURL == "" {
		natsURL = os.Getenv("PIPEDPEER_NATS_URL")
	}
	if maxConcurrent < 0 {
		maxConcurrent = 0 // unlimited unless configured
		if v := os.Getenv("PIPEDPEER_MAX_CONCURRENT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				maxConcurrent = n
			} else {
				log.Warn().Str("value", v).Msg("ignoring invalid PIPEDPEER_MAX_CONCURRENT")
			}
		}
	}

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load identity")
	}

	server := daemonapi.New(nodeID.NodeID)
	server.EnablePersistence()
	server.SetMaxConcurrentJobs(maxConcurrent)
	if v := os.Getenv("PIPEDPEER_SERIAL_CLOSURE_IMPORTS"); v == "1" {
		server.SetSerialClosureImports(true)
	}

	var bus *natsbus.Bus
	natsCfg := natsbus.Config{
		URL:      natsURL,
		Embedded: natsURL == "",
		Name:     "pipedpeer-daemon-" + nodeID.ShortID(),
	}
	bus, err = natsbus.New(natsCfg)
	if err != nil {
		log.Warn().Err(err).Msg("NATS unavailable, running HTTP-only")
	} else {
		if natsCfg.Embedded {
			log.Info().Str("url", bus.ClientURL()).Msg("started embedded NATS server")
		} else {
			log.Info().Str("url", natsURL).Msg("connected to external NATS")
		}
		if err := server.BindNATS(bus); err != nil {
			log.Warn().Err(err).Msg("failed to bind NATS subscriptions")
		}
	}

	var hbClient *heartbeat.Client
	sshEp := sshEndpoint
	if sshEp == "" {
		sshEp = fmt.Sprintf("root@%s:22", detectLocalIP())
	}
	if bus != nil {
		hbClient = heartbeat.NewNATSClient(bus, nodeID, sshEp, port)
	} else if registryURL != "" {
		hbClient = heartbeat.NewClient(registryURL, nodeID, sshEp, port)
	}
	if hbClient != nil {
		hbClient.SetJobCounter(server.ActiveJobs)
		hbClient.SetReservedMemFn(server.ReservedMem)
		_ = hbClient.Start()
	}

	advertiser := discovery.NewAdvertiser(discovery.ServiceInfo{
		NodeID:      nodeID.NodeID,
		DaemonPort:  port,
		SSHEndpoint: sshEp,
		Arch:        nodeID.Arch,
		Hostname:    nodeID.Hostname,
	})
	_ = advertiser.Start()

	// Teach the daemon who it is and how to find peers. Both feed the node
	// store, which is what the orchestrator reads when placing a task.
	server.SetSelfInfo(sshEp, port, heartbeat.Capabilities(nodeID))
	server.SetDiscoverFn(func() []daemonapi.NodeDiscovered {
		found := discovery.Discover(1 * time.Second)
		if len(found) == 0 {
			// ponytail: UDP broadcast first (free when the network allows
			// it); fall back to a direct /health sweep of the local subnet
			// only when it comes up empty (AP client isolation, multicast
			// filtering).
			found = discovery.DiscoverTCP(port, 3*time.Second)
		}
		out := make([]daemonapi.NodeDiscovered, 0, len(found))
		for _, n := range found {
			out = append(out, daemonapi.NodeDiscovered{
				NodeID:      n.NodeID,
				SSHEndpoint: n.SSHEndpoint,
				DaemonPort:  n.DaemonPort,
				Arch:        n.Arch,
				Hostname:    n.Hostname,
			})
		}
		return out
	})

	// Reap leases whose submitter never came back, so a crashed CLI does not
	// permanently consume a slot on this node's concurrency budget.
	server.StartSweeper()

	server.StartPeerPoller(10 * time.Second)

	// Multi-node interception: allow pool-map chunks to fan out to healthy
	// peers that share this closure. Local always participates, so a remote
	// node adds capacity but never slows a run down.
	server.EnablePoolSpill()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		server.StopWarmWorkers()
		if hbClient != nil {
			hbClient.Stop()
		}
		advertiser.Stop()
		if bus != nil {
			bus.Close()
		}
		os.Exit(0)
	}()

	log.Info().Int("port", port).Str("node_id", nodeID.ShortID()).
		Int("max_concurrent", maxConcurrent).Msg("daemon starting")
	if err := server.ListenAndServe(port); err != nil {
		log.Fatal().Err(err).Msg("daemon listen failed")
	}
}

// detectLocalIP returns the address peers should use to reach this machine.
// It is what the node advertises as its own endpoint and what a DDP rank 0
// hands out as MASTER_ADDR, so an address nobody can dial is worse than
// useless — every peer connects and hangs.
//
// Enumerating by interface rather than InterfaceAddrs matters: a /32 parked on
// lo (Tailscale leaves 198.18.x there) is not in 127.0.0.0/8, so IsLoopback
// passes it, and lo is interface index 1 so it is the first thing returned.
// That address then reached peers as this node's endpoint.
//
// Preference order is by how widely routable an address is: a LAN address
// beats a VPN one, because peers that can reach both should take the direct
// path.
func detectLocalIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	var lan, other string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
				continue
			}
			// 198.18.0.0/15 is the RFC 2544 benchmarking range: never a real
			// peer-reachable address, and exactly what gets parked on lo.
			if ip[0] == 198 && (ip[1] == 18 || ip[1] == 19) {
				continue
			}
			if ip.IsPrivate() && lan == "" {
				lan = ip.String()
			} else if other == "" {
				other = ip.String()
			}
		}
	}

	if lan != "" {
		return lan
	}
	if other != "" {
		return other
	}
	return "127.0.0.1"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
