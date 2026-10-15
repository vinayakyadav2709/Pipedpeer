package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
	"github.com/pipedpeer/pipedpeer/internal/ping"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
	"github.com/pipedpeer/pipedpeer/internal/userns"
)

func main() {
	// First of all: run from the root-owned key helper (sudo lets the user
	// run that file as root with any arguments), this binary does nothing
	// but add or remove nix keys (ADR-0018).
	if nixtrust.IsHelper() {
		os.Exit(nixtrust.Main(os.Args[1:]))
	}

	// Before anything dials out. The daemon path returns just below, so
	// installing these later left the daemon itself - the process that does
	// nearly all the peer traffic - neither authenticating nor encrypting
	// its own calls, which reads as every peer being unreachable.
	//
	// TLS first, then the token: the token wrapper delegates to whatever it
	// wraps, so this order puts the secret inside the encrypted connection
	// rather than beside it.
	// Before anything else: a userns probe re-executes this binary, and that
	// child must do its one syscall and exit rather than parse arguments.
	userns.Install()

	tlsid.Install()
	authtoken.Install()

	// crun may have been installed into the user's own bin directory, which
	// setup does instead of asking for root. That directory is on PATH for a
	// login shell but not necessarily for a daemon, and a daemon that cannot
	// find crun falls back to running jobs unsandboxed.
	ensureUserBinOnPath()

	if len(os.Args) > 1 && os.Args[1] == "__daemon__" {
		runDaemon(os.Args[2:])
		return
	}
	// Everything below is the CLI: human log lines, not the daemon's JSON.
	logging.SetupCLI()

	rootCmd := &cobra.Command{
		// A command that fails at run time should say why, not print its
		// flags: the flags were fine, the network was not.
		SilenceUsage: true,
		// main prints the error, so a cancel already reported stays one line.
		SilenceErrors: true,
		Use:           "pipedpeer",
		Short:         "Decentralized compute orchestrator",
		Long:          "Pipedpeer — submit, schedule, and execute compute tasks across a peer-to-peer mesh.",
	}

	viper.SetEnvPrefix("PIPEDPEER")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	rootCmd.AddCommand(
		newStartCmd(),
		newStopCmd(),
		newStatusCmd(),
		newSetupCmd(),
		newTrustCmd(),
		newRunCmd(),
		newDashboardCmd(),
		newJobsCmd(),
		newJobCmd(),
		newInitCmd(),
		newRegistryCmd(),
		newNodesCmd(),
		newPingCmd(),
		newNetCheckCmd(),
		newNetPunchCmd(),
		newJoinCmd(),
		newNetJoinCmd(),
		newRelayTestCmd(),
		newRelayConnectCmd(),
		newRendezvousCmd(),
		newTrafficCmd(),
		newTasksCmd(),
		newMapCmd(),
		newAuthCmd(),
		newEventsCmd(),
		newAttachCmd(),
		newStatsCmd(),
		newUICmd(),
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
		if !errors.Is(err, errCancelReported) {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
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

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer cancel()
			go func() { <-ctx.Done(); cancel() }()
			_, stopSuspend := watchSuspend(ctx, false)
			defer stopSuspend()

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
					err := app.RunTask(ctx, env, app.Task{Options: o, Script: scriptPath, Label: fmt.Sprintf("task-%d", task.ShardID)})
					if app.IsPermanent(err) {
						return coordinator.Permanent(err)
					}
					return err
				}
				statusFn := func(msg string) {}
				res, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
				if err != nil {
					return "", err
				}
				return res.NodeID, nil
			}

			return runOutcome(app.RunMap(tasks, base, concurrency, resultsDir, env, run, reduce))
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

			return runDashboard(tea.NewProgram(m, tea.WithAltScreen()))
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
			warned := false
			for _, it := range items {
				mode := "fg"
				status := it.Status
				if it.Detached {
					mode = "bg"
					// History only knows what the submitter last saw; a
					// detached job's daemon knows whether it has finished.
					if status == jobhistory.StateRunning {
						status = detachedJobStatus(it.ID, &warned)
					}
				}
				fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", jobhistory.ShortID(it.ID), status, mode, it.JobName, it.JobSet, it.TargetID, it.Remote, it.DurationMs, it.StartedAt)
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

// detachedJobStatus asks the daemon running a detached job how it is doing.
// On any failure it shows "?" and warns once per listing.
func detachedJobStatus(id string, warned *bool) string {
	r, _, err := jobhistory.ReadRecord(id)
	if err == nil && (r.RemoteJobID == "" || r.DaemonEndpoint == "") {
		err = errors.New("no daemon recorded for it")
	}
	var st daemonctl.JobState
	if err == nil {
		var host, portStr string
		var port int
		if host, portStr, err = net.SplitHostPort(r.DaemonEndpoint); err == nil {
			if port, err = strconv.Atoi(portStr); err == nil {
				st, err = daemonctl.JobStatus(host, port, r.RemoteJobID)
			}
		}
	}
	if err != nil {
		if !*warned {
			*warned = true
			fmt.Fprintf(os.Stderr, "could not ask the daemon about detached job %s: %v\n", jobhistory.ShortID(id), err)
		}
		return "?"
	}
	return st.Status
}

func newAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <id>",
		Short: "Follow a detached job again: replay its output, follow it live, fetch its results",
		Long: "Reattach to a job started with --detach or Ctrl+Z on this machine. <id> is the " +
			"id `pipedpeer jobs` shows. Ctrl+C while attached stops the job.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer stop()
			go exitOnSecondSignal(ctx, stop)
			return runOutcome(app.Attach(ctx, args[0]))
		},
	}
}

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Show details for a specific job",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			withOutput, _ := cmd.Flags().GetBool("output")
			timeline, _ := cmd.Flags().GetBool("timeline")
			explain, _ := cmd.Flags().GetBool("explain")
			showReceipt, _ := cmd.Flags().GetBool("receipt")
			if last, _ := cmd.Flags().GetBool("last"); last && id == "" {
				recent, err := jobhistory.List(1)
				if err != nil {
					return err
				}
				if len(recent) == 0 {
					return fmt.Errorf("no jobs recorded yet")
				}
				id = recent[0].ID
			}
			if id == "" {
				return fmt.Errorf("--id is required (or --last for the most recent job)")
			}
			r, dir, err := jobhistory.Resolve(id)
			if err != nil {
				return err
			}
			if timeline || explain || showReceipt {
				fmt.Printf("job %s  %s  %s\n", r.ID, filepath.Base(r.ScriptPath), r.Status)
				if explain {
					fmt.Println("\n== Placement ==")
					jobhistory.WriteExplanation(os.Stdout, r)
				}
				if timeline {
					fmt.Println("\n== Timeline ==")
					jobhistory.WriteWaterfall(os.Stdout, r.Stages, 40)
				}
				if showReceipt {
					fmt.Println("\n== Receipt ==")
					rc, ok, err := jobhistory.ReadReceipt(dir)
					switch {
					case err != nil:
						return err
					case !ok:
						fmt.Println("No receipt for this run (interception off, or run before receipts were kept).")
					default:
						jobhistory.WriteReceipt(os.Stdout, rc)
					}
				}
				return nil
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
			if len(r.Stages) > 0 {
				fmt.Printf("timeline: %s\n", jobhistory.Summary(r.Stages))
			}
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
	cmd.Flags().Bool("last", false, "Show the most recent job")
	cmd.Flags().Bool("output", false, "Print stdout/stderr")
	cmd.Flags().Bool("timeline", false, "Show where the run's time went, stage by stage")
	cmd.Flags().Bool("explain", false, "Show every node considered for the run and why the chosen one won")
	cmd.Flags().Bool("receipt", false, "Show what the shim sent to other machines, kept local, and why")
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
		Use:   "remove [host|node-id]",
		Short: "Remove a node by host or node ID (short IDs from 'nodes list' work; --all clears everything)",
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
				var body struct {
					Error string `json:"error"`
				}
				json.NewDecoder(resp.Body).Decode(&body)
				if body.Error != "" {
					return fmt.Errorf("%s", body.Error)
				}
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
		printUnreachable(os.Stdout, daemonPort)
		return nil
	}

	printNodeTable(os.Stdout, nodes)
	printUnreachable(os.Stdout, daemonPort)
	return nil
}

// printUnreachable lists peers with no direct path; a fetch error goes to stderr.
func printUnreachable(w io.Writer, daemonPort int) {
	bad, err := fetchUnreachable(daemonPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not list peers with no direct path: %v\n", err)
		return
	}
	if len(bad) == 0 {
		return
	}
	fmt.Fprintln(w, "\nPeers with no direct path (not offered work):")
	for _, p := range bad {
		fmt.Fprintf(w, "  %-10s %s\n", shortID(p.Peer), p.Reason)
	}
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
