package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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
	"github.com/pipedpeer/pipedpeer/internal/peers"
	"github.com/pipedpeer/pipedpeer/internal/ping"
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
		newJobsCmd(),
		newJobCmd(),
		newInitCmd(),
		newRegistryCmd(),
		newNodesCmd(),
		newPeersCmd(),
		newPingCmd(),
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

		// Rewrite into: run --script <script> --strategy round-robin --no-self --isolate=false
		newArgs := []string{
			"run",
			"--script", scriptPath,
			"--strategy", "round-robin",
			"--no-self",
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
			port, _ := cmd.Flags().GetInt("daemon-port")
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			wasStarted, err := daemonctl.EnsureStarted(nodeID.NodeID, port)
			if err != nil {
				return err
			}
			if wasStarted {
				fmt.Printf("started daemon (node-id=%s, port=%d)\n", nodeID.ShortID(), port)
			} else {
				fmt.Printf("daemon already running\n")
			}
			return nil
		},
	}
	cmd.Flags().Int("daemon-port", 38080, "Local daemon port")
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
		Use:   "run",
		Short: "Submit and execute a task on a remote node",
		Long:  "Submit a Python script for remote execution. Auto-discovers nodes if --host is not specified.",
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptPath, _ := cmd.Flags().GetString("script")
			daemonHost, _ := cmd.Flags().GetString("host")
			targetID, _ := cmd.Flags().GetString("target-id")
			daemonPort, _ := cmd.Flags().GetInt("daemon-port")
			jobName, _ := cmd.Flags().GetString("job-name")
			isolate, _ := cmd.Flags().GetBool("isolate")
			checkOnly, _ := cmd.Flags().GetBool("check-only")
			mode, _ := cmd.Flags().GetString("mode")
			pythonVersion, _ := cmd.Flags().GetString("python")
			registryURL, _ := cmd.Flags().GetString("registry")
			memOverride, _ := cmd.Flags().GetString("mem")
			envs, _ := cmd.Flags().GetStringSlice("env")
			pkgs, _ := cmd.Flags().GetStringSlice("pkg")
			noSelf, _ := cmd.Flags().GetBool("no-self")
			strategy, _ := cmd.Flags().GetString("strategy")

			if scriptPath == "" {
				return fmt.Errorf("--script is required")
			}

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

			var placementSource, placementReason string
			var placementDegraded bool
			var placementCandidates int
			var estimatedMemBytes int64
			var estimationTier string

			if daemonHost == "" {
				fmt.Println("[coordinator] No --host specified, discovering nodes...")

				userMem := resourceest.ParseMemString(memOverride)
				resReq := resourceest.EstimateFromScript(scriptPath, nil, userMem)
				estimatedMemBytes = resReq.MemBytes
				estimationTier = resReq.Tier
				fmt.Printf("[coordinator] Resource estimate: %s (tier=%s)\n",
					resourceest.FormatBytes(resReq.MemBytes), resReq.Tier)

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
					RequiredMemBytes: resReq.MemBytes,
					NoSelf:           noSelf,
					Strategy:         strategy,
					RoundRobinFn:     rrFn,
					DiscoverFn: func() []registry.NodeRecord {
						return discovery.DiscoverAsNodeRecords(1 * time.Second)
					},
				})

				if checkOnly {
					decision := coord.FindNode()
					fmt.Printf("[coordinator] Would place on node %s (%s)\n",
						decision.ChosenNode.NodeID[:min(8, len(decision.ChosenNode.NodeID))], decision.Reason)
					fmt.Println("checks complete")
					return nil
				}

				ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
				defer cancel()

				scriptArgs := args

				executor := func(host string, port int, targetNodeID string) error {
					return app.Run(app.Options{
						ScriptPath:        scriptPath,
						DaemonHost:        host,
						DaemonPort:        port,
						TargetID:          targetNodeID,
						JobName:           jobName,
						Isolate:           isolate,
						Mode:              mode,
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

				result, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
				if err != nil {
					return err
				}

				fmt.Printf("[coordinator] Task completed successfully on node %s\n",
					result.NodeID[:min(8, len(result.NodeID))])
				return nil
			}

			placementSource = "explicit"

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
				TargetID:          targetID,
				JobName:           jobName,
				Isolate:           isolate,
				Mode:              mode,
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
	cmd.Flags().String("script", "", "Path to Python script")
	cmd.Flags().String("host", "", "Daemon host (replaces --remote)")
	cmd.Flags().String("target-id", "", "Remote node ID")
	cmd.Flags().Int("daemon-port", 38080, "Daemon API port")
	cmd.Flags().String("job-name", "", "Optional job name")
	cmd.Flags().Bool("isolate", true, "Run in bubblewrap sandbox")
	cmd.Flags().Bool("check-only", false, "Only check acceptance, don't execute")
	cmd.Flags().String("mode", "script", "Execution mode")
	cmd.Flags().String("python", "", "Python version")
	cmd.Flags().String("registry", viper.GetString("REGISTRY"), "Registry URL")
	cmd.Flags().StringSliceP("env", "e", nil, "Environment variables (e.g., -e API_KEY=123)")
	cmd.Flags().StringSlice("pkg", nil, "Packages or requirements.txt")
	cmd.Flags().String("mem", "", "Memory requirement override")
	cmd.Flags().Bool("no-self", false, "Exclude self-node from placement")
	cmd.Flags().String("strategy", "smart", "Placement strategy: smart (default) or round-robin")
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
			fmt.Println("ID\tSTATUS\tMODE\tTARGET\tHOST\tDURATION_MS\tSTARTED")
			for _, it := range items {
				mode := "fg"
				if it.Detached {
					mode = "bg"
				}
				fmt.Printf("%s\t%s\t%s\t%s\t%s\t%d\t%s\n", it.ID, it.Status, mode, it.TargetID, it.Remote, it.DurationMs, it.StartedAt)
			}
			return nil
		},
	}
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
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List known nodes (from registry and LAN discovery)",
		RunE: func(cmd *cobra.Command, args []string) error {
			registryURL, _ := cmd.Flags().GetString("registry")
			nodeID, _ := identity.GetOrCreate()
			fmt.Println("Discovering LAN nodes...")
			discovered := discovery.DiscoverAsNodeRecords(1 * time.Second)

			var regNodes []registry.NodeRecord
			if registryURL != "" {
				resp, err := http.Get(registryURL + "/v1/nodes")
				if err == nil {
					defer resp.Body.Close()
					_ = json.NewDecoder(resp.Body).Decode(&regNodes)
				} else {
					fmt.Printf("Warning: registry unreachable: %v\n", err)
				}
			}

			seen := make(map[string]registry.NodeRecord)
			for _, n := range regNodes {
				seen[n.NodeID] = n
			}
			for _, n := range discovered {
				if _, exists := seen[n.NodeID]; !exists {
					seen[n.NodeID] = n
				}
			}

			if len(seen) == 0 {
				fmt.Println("No nodes found (only self is available)")
				fmt.Printf("  (self) %s  %s  %s\n", nodeID.ShortID(), nodeID.Arch, nodeID.Hostname)
				return nil
			}

			fmt.Printf("%-10s %-12s %-16s %-6s %-6s %-5s %s\n",
				"NODE_ID", "STATE", "ARCH", "CPU%", "MEM%", "JOBS", "ENDPOINT")
			for _, n := range seen {
				prefix := "  "
				if n.NodeID == nodeID.NodeID {
					prefix = "* "
				}
				shortID := n.NodeID
				if len(shortID) > 8 {
					shortID = shortID[:8]
				}
				fmt.Printf("%s%-8s %-12s %-16s %-6.0f %-6.0f %-5d %s\n",
					prefix, shortID, n.State, n.Capabilities["arch"],
					n.Load.CPUPercent, n.Load.MemPercent, n.Load.ActiveJobs,
					n.SSHEndpoint)
			}
			return nil
		},
	}
	cmd.Flags().String("registry", viper.GetString("REGISTRY"), "Registry URL")
	return cmd
}

func newPeersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "peers",
		Short: "Manage manually registered peers",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "add <host> <port>",
			Short: "Add a peer by host and port",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				host := args[0]
				var port int
				fmt.Sscanf(args[1], "%d", &port)
				if port < 1 || port > 65535 {
					return fmt.Errorf("invalid port: %s", args[1])
				}
				if err := peers.Add(host, port); err != nil {
					return err
				}
				fmt.Printf("added peer %s:%d\n", host, port)
				return nil
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "List all peers",
			RunE: func(cmd *cobra.Command, args []string) error {
				list, err := peers.List()
				if err != nil {
					return err
				}
				if len(list) == 0 {
					fmt.Println("no peers added. Use: pipedpeer peers add <host> <port>")
					return nil
				}
				for _, p := range list {
					fmt.Printf("%s:%d\n", p.Host, p.Port)
				}
				return nil
			},
		},
		&cobra.Command{
			Use:   "remove <host>",
			Short: "Remove all peers at the given host",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := peers.Remove(args[0]); err != nil {
					return err
				}
				fmt.Printf("removed peers at %s\n", args[0])
				return nil
			},
		},
	)

	return cmd
}

func newPingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ping <host:port> [host:port ...]",
		Short: "Live health dashboard for workers",
		Long:  "Polls /health on each worker every second and displays a live status table.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			intervalSec, _ := cmd.Flags().GetInt("interval")
			return ping.Run(args, time.Duration(intervalSec)*time.Second)
		},
	}
	cmd.Flags().Int("interval", 1, "Poll interval in seconds")
	return cmd
}

func runDaemon(args []string) {
	log := logging.WithComponent("daemon")

	var port int
	var registryURL, sshEndpoint, natsURL string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &port)
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

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load identity")
	}

	server := daemonapi.New(nodeID.NodeID)

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

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		if hbClient != nil {
			hbClient.Stop()
		}
		advertiser.Stop()
		if bus != nil {
			bus.Close()
		}
		os.Exit(0)
	}()

	log.Info().Int("port", port).Str("node_id", nodeID.ShortID()).Msg("daemon starting")
	if err := server.ListenAndServe(port); err != nil {
		log.Fatal().Err(err).Msg("daemon listen failed")
	}
}

func detectLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "127.0.0.1"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
