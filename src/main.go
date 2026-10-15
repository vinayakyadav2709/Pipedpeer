package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/say"
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
		Version:       version,
		Short:         "Run a Python script on more machines",
		Long: "pipedpeer runs your Python script like python does, and spreads its work over the\n" +
			"machines in your cluster when that is faster.\n\n" +
			"  pipedpeer setup          prepare this machine (once)\n" +
			"  pipedpeer run main.py    run a script\n" +
			"  pipedpeer status         see the machines and what they are doing",
	}
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	// New commands (invite, connect, doctor, uninstall) are added by their
	// own units; join, trust, auth and the network probes are gone.
	rootCmd.AddCommand(
		newStartCmd(),
		newStopCmd(),
		newStatusCmd(),
		newSetupCmd(),
		newRunCmd(),
		newDashboardCmd(),
		newJobsCmd(),
		newMapCmd(),
		newEventsCmd(),
		newAttachCmd(),
		newUICmd(),
	)

	rootCmd.SetArgs(os.Args[1:])

	if err := rootCmd.Execute(); err != nil {
		if !errors.Is(err, errCancelReported) {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		os.Exit(exitCode(err))
	}
}

// version is stamped by GoReleaser (-X main.version=...).
var version = "dev"

// exitCode is what the process exits with for a failed command: the
// script's own code when a remote job ended non-zero, 130 for a cancel
// (like a shell after Ctrl+C), 1 otherwise.
func exitCode(err error) int {
	var re *daemonctl.RemoteExitError
	if errors.As(err, &re) && re.Code > 0 {
		return re.Code
	}
	if errors.Is(err, errCancelReported) || errors.Is(err, context.Canceled) {
		return 130
	}
	return 1
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the machines in the cluster and what they are doing",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			st := daemonctl.Status()
			if !st.Running {
				return errors.New("daemon stopped (start it with: pipedpeer start)")
			}
			fmt.Printf("daemon running (pid=%d node-id=%s port=%d version=%s)\n", st.PID, st.NodeID, localDaemonPort(), version)
			printStatus(os.Stdout, localDaemonPort())
			return nil
		},
	}
}
func newMapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "map",
		Hidden: true,
		Short:  "Scatter one script across many shards/inputs and gather the results",
		Long: "Build a Python environment once, then run the script as many tasks across " +
			"the cluster, one per input file / args-file line / data shard. Each task gets " +
			"its own results directory and PIPEDPEER_SHARD_ID/NUM_SHARDS envs.",
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptPath, _ := cmd.Flags().GetString("script")
			daemonPort := localDaemonPort()
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
			say.Printf("[coordinator] Resource estimate: %s (tier=%s)\n",
				resourceest.FormatBytes(resReq.MemBytes), resReq.Tier)

			rrFn := func(count int) int {
				url := fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", daemonPort, count)
				resp, err := http.Get(url)
				if err != nil {
					say.Warnf("round-robin index unavailable, starting at 0: %v\n", err)
					return 0
				}
				defer resp.Body.Close()
				var r struct{ Index int }
				if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
					say.Warnf("round-robin index unreadable, starting at 0: %v\n", err)
				}
				return r.Index
			}

			coord := coordinator.New(coordinator.Config{
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

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer cancel()
			go func() { <-ctx.Done(); cancel() }()

			absScript, err := filepath.Abs(scriptPath)
			if err != nil {
				return err
			}
			env, err := app.BuildEnvironment(ctx, absScript, app.EnvOptions{
				PythonVersion: pythonVersion,
				Pkgs:          pkgs,
				Verbose:       say.Verbose,
			}, func(step int, title string) {
				say.Printf("[%d/7] %s\n", step, title)
			})
			if err != nil {
				return err
			}
			defer env.Close()
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
	cmd.Flags().Bool("isolate", true, "Run in OCI sandbox (crun)")
	cmd.Flags().Bool("remote", false, "Only scatter tasks to other machines (excludes this one)")
	cmd.Flags().String("gpu", "", "GPU mode: force, prefer, off")
	cmd.Flags().String("gpu-id", "", "Pin GPU device IDs")
	cmd.Flags().String("gpu-mem", "", "Minimum free VRAM on a single GPU")
	cmd.Flags().StringSliceP("env", "e", nil, "Environment variables (e.g., -e API_KEY=123)")
	cmd.Flags().StringSlice("pkg", nil, "Packages or requirements.txt")
	cmd.Flags().String("python", "", "Python version")
	cmd.Flags().String("mem", "", "Memory requirement override")
	cmd.Flags().String("strategy", "smart", "Placement strategy: smart (default) or round-robin")
	_ = cmd.Flags().MarkHidden("strategy")
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
		Short: "Live view of the machines, their work and recent runs (q to exit)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort := localDaemonPort()
			interval, _ := cmd.Flags().GetDuration("interval")

			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}

			if _, err := daemonctl.EnsureStarted(nodeID.NodeID, daemonPort); err != nil {
				say.Warnf("could not start the daemon, showing what is left: %v\n", err)
			}

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
	cmd.Flags().Duration("interval", 2*time.Second, "Refresh interval (e.g. 1s, 2s, 500ms)")
	return cmd
}
func newJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs [id]",
		Short: "List recent runs, or show one run in detail",
		Long: "Without an id: the recent runs started on this machine.\n" +
			"With an id (or \"last\"): that run, plus any of --output, --timeline, --explain,\n" +
			"--receipt, --steps, or --export csv|json for its step record.\n" +
			"--export-all <dir> writes every run's record as CSV and JSON into <dir>.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if dir := mustString(cmd, "export-all"); dir != "" {
				return exportAllRuns(dir, mustString(cmd, "since"))
			}
			if len(args) == 1 {
				return showJob(cmd, args[0])
			}
			for _, f := range []string{"output", "timeline", "explain", "receipt", "steps", "export"} {
				if cmd.Flags().Changed(f) {
					return fmt.Errorf("--%s needs a job id (see: pipedpeer jobs)", f)
				}
			}
			limit, _ := cmd.Flags().GetInt("limit")
			return listJobs(os.Stdout, limit)
		},
	}
	pruneCmd := &cobra.Command{
		Use:   "prune",
		Short: "Delete job history entries not touched within --older-than",
		Args:  cobra.NoArgs,
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
	cmd.Flags().Int("limit", 20, "Max runs to list")
	cmd.Flags().Bool("output", false, "Print the run's output")
	cmd.Flags().Bool("timeline", false, "Show where the run's time went, stage by stage")
	cmd.Flags().Bool("explain", false, "Show every machine considered for the run and why the chosen one won")
	cmd.Flags().Bool("receipt", false, "Show what was sent to other machines, kept local, and why")
	cmd.Flags().Bool("steps", false, "Show every step of every chunk on every machine")
	cmd.Flags().String("export", "", "Print the run's step record as csv or json")
	cmd.Flags().String("export-all", "", "Write every run's record as CSV and JSON into this directory")
	cmd.Flags().String("since", "", "With --export-all: only runs since this date (2006-01-02 or RFC 3339)")
	return cmd
}

// listJobs prints the recent runs as a table a person reads.
func listJobs(w io.Writer, limit int) error {
	items, err := jobhistory.List(limit)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Fprintln(w, "no runs yet")
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSCRIPT\tSTATUS\tWHERE\tTOOK\tSTARTED")
	warned := false
	for _, it := range items {
		status := it.Status
		if it.Detached {
			// History only knows what the submitter last saw; a
			// detached job's daemon knows whether it has finished.
			if status == jobhistory.StateRunning {
				status = detachedJobStatus(it.ID, &warned)
			}
			status += " (background)"
		}
		script := filepath.Base(it.ScriptPath)
		if it.ScriptPath == "" {
			script = it.JobName
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", jobhistory.ShortID(it.ID), truncate(script, 24), status,
			whereRan(it.Remote, ""), humanMs(it.DurationMs), humanStarted(it.StartedAt))
	}
	return tw.Flush()
}

// whereRan names the machine a run went to: its host, else "-".
func whereRan(remote, runHost string) string {
	if runHost != "" {
		return runHost
	}
	if remote == "" {
		return "-"
	}
	return hostFromEndpoint(remote)
}

// humanMs is a duration rounded to what a person cares about.
func humanMs(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return d.Round(100 * time.Millisecond).String()
	default:
		return d.Round(time.Second).String()
	}
}

// humanStarted is "3 minutes ago" for an RFC 3339 time, the raw text otherwise.
func humanStarted(s string) string {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		if s == "" {
			return "-"
		}
		return s
	}
	return humanize.Time(t)
}

// showJob prints one run: a short summary, then each section asked for.
func showJob(cmd *cobra.Command, id string) error {
	if id == "last" {
		recent, err := jobhistory.List(1)
		if err != nil {
			return err
		}
		if len(recent) == 0 {
			return fmt.Errorf("no runs yet")
		}
		id = recent[0].ID
	}
	r, dir, err := jobhistory.Resolve(id)
	if err != nil {
		return err
	}
	flag := func(name string) bool { v, _ := cmd.Flags().GetBool(name); return v }

	if format := mustString(cmd, "export"); format != "" {
		rec, err := runrec.Load(dir)
		if err != nil {
			return fmt.Errorf("no step record for run %s: %w", jobhistory.ShortID(r.ID), err)
		}
		switch format {
		case "csv":
			return runrec.WriteCSV(os.Stdout, rec)
		case "json":
			return runrec.WriteJSON(os.Stdout, rec)
		default:
			return fmt.Errorf("invalid --export %q (want csv or json)", format)
		}
	}

	fmt.Printf("%s  %s  %s\n", jobhistory.ShortID(r.ID), filepath.Base(r.ScriptPath), r.Status)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	row := func(k, v string) {
		if v != "" && v != "-" {
			fmt.Fprintf(tw, "  %s\t%s\n", k, v)
		}
	}
	started := r.StartedAt
	if t, err := time.Parse(time.RFC3339Nano, r.StartedAt); err == nil {
		started = t.Local().Format("2006-01-02 15:04:05") + " (" + humanize.Time(t) + ")"
	}
	row("started", started)
	row("took", humanMs(r.DurationMs))
	where := whereRan(r.Remote, r.RunHost)
	if r.TargetID != "" {
		where += " (" + shortID(r.TargetID) + ")"
	}
	row("ran on", where)
	if r.ExitCode != 0 {
		row("exit code", strconv.Itoa(r.ExitCode))
	}
	row("error", r.Error)
	row("results", r.LocalSyncRoot)
	if len(r.Stages) > 0 {
		row("timeline", jobhistory.Summary(r.Stages))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if flag("explain") {
		fmt.Println("\n== Placement ==")
		jobhistory.WriteExplanation(os.Stdout, r)
	}
	if flag("timeline") {
		fmt.Println("\n== Timeline ==")
		jobhistory.WriteWaterfall(os.Stdout, r.Stages, 40)
	}
	if flag("receipt") {
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
	if flag("steps") {
		fmt.Println("\n== Steps ==")
		rec, err := runrec.Load(dir)
		if err != nil {
			fmt.Printf("No step record for this run: %v\n", err)
		} else {
			runrec.WriteDetailed(os.Stdout, rec)
		}
	}
	if flag("output") {
		for _, part := range []struct{ title, file string }{
			{"output", "stdout.log"}, {"errors", "stderr.log"}, {"remote logs", "remote_logs.txt"},
		} {
			text, err := jobhistory.ReadOptionalText(dir, part.file)
			if err != nil {
				say.Warnf("could not read %s of run %s: %v\n", part.file, jobhistory.ShortID(r.ID), err)
				continue
			}
			if strings.TrimSpace(text) == "" {
				continue
			}
			fmt.Printf("\n--- %s ---\n%s", part.title, text)
			if !strings.HasSuffix(text, "\n") {
				fmt.Println()
			}
		}
	}
	return nil
}

// exportAllRuns is the one demo-day command: every run's record into dir.
func exportAllRuns(dir, sinceText string) error {
	var since time.Time
	if sinceText != "" {
		var err error
		if since, err = time.ParseInLocation("2006-01-02", sinceText, time.Local); err != nil {
			if since, err = time.Parse(time.RFC3339, sinceText); err != nil {
				return fmt.Errorf("invalid --since %q (want 2006-01-02 or RFC 3339)", sinceText)
			}
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	n, err := runrec.ExportAll(dir, since)
	if err != nil {
		return err
	}
	fmt.Printf("exported %d run(s) to %s\n", n, dir)
	return nil
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
