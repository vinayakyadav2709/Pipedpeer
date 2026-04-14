package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/remote"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "__daemon__" {
		runDaemon(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "__sync_job__" {
		runSyncJobWorker(os.Args[2:])
		return
	}

	cmd := "run"
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "start", "stop", "status", "run", "jobs", "job":
			cmd = args[0]
			args = args[1:]
		}
	}

	switch cmd {
	case "start":
		runStart(args)
	case "stop":
		runStop()
	case "status":
		runStatus()
	case "jobs":
		runJobs(args)
	case "job":
		runJobDetails(args)
	default:
		runJob(args)
	}
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("__daemon__", flag.ExitOnError)
	nodeID := fs.String("node-id", "node-local", "Node ID served by this daemon")
	port := fs.Int("port", 38080, "Daemon listen port")
	_ = fs.Parse(args)

	server := daemonapi.New(*nodeID)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		os.Exit(0)
	}()

	if err := server.ListenAndServe(*port); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runSyncJobWorker(args []string) {
	fs := flag.NewFlagSet("__sync_job__", flag.ExitOnError)
	user := fs.String("user", "", "SSH user")
	host := fs.String("host", "", "SSH host")
	port := fs.Int("port", 22, "SSH port")
	jobDir := fs.String("job-dir", "", "Remote job directory")
	localRoot := fs.String("local-root", "", "Local workspace root to sync files into")
	historyDir := fs.String("history-dir", "", "Local job history directory")
	timeoutSec := fs.Int("timeout-sec", 43200, "Wait timeout for detached job completion")
	_ = fs.Parse(args)

	if *user == "" || *host == "" || *jobDir == "" || *localRoot == "" || *historyDir == "" {
		fmt.Fprintln(os.Stderr, "missing required sync worker arguments")
		os.Exit(2)
	}

	err := app.RunDetachedSyncWorker(app.DetachedSyncWorkerOptions{
		User:       *user,
		Host:       *host,
		Port:       *port,
		JobDir:     *jobDir,
		LocalRoot:  *localRoot,
		HistoryDir: *historyDir,
		TimeoutSec: *timeoutSec,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	nodeID := fs.String("node-id", "node-local", "Local node ID")
	port := fs.Int("daemon-port", 38080, "Local daemon port")
	_ = fs.Parse(args)

	wasStarted, err := daemonctl.EnsureStarted(*nodeID, *port)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if wasStarted {
		fmt.Printf("started daemon (node-id=%s, port=%d)\n", *nodeID, *port)
	} else {
		fmt.Printf("daemon already running\n")
	}
}

func runStop() {
	if err := daemonctl.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("stopped daemon")
}

func runStatus() {
	st := daemonctl.Status()
	if !st.Running {
		fmt.Println("daemon stopped")
		return
	}
	fmt.Printf("daemon running (pid=%d node-id=%s port=%d)\n", st.PID, st.NodeID, st.Port)
}

func runJob(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	scriptPath := fs.String("script", "", "Path to Python script")
	remoteAddr := fs.String("remote", "", "Remote SSH destination (e.g., root@localhost:2221)")
	targetID := fs.String("target-id", "", "Required remote node ID for daemon acceptance")
	daemonPort := fs.Int("daemon-port", 38080, "Daemon API port on nodes")
	localNodeID := fs.String("local-node-id", "node-local", "Local node ID used when auto-starting daemon")
	detach := fs.Bool("detach", false, "Submit job and return immediately instead of waiting for completion")
	jobName := fs.String("job-name", "", "Optional job name used for remote job directory")
	isolate := fs.Bool("isolate", true, "Run job in an isolated bubblewrap sandbox on the remote node")
	checkOnly := fs.Bool("check-only", false, "Only perform daemon start + target acceptance checks")
	_ = fs.Parse(args)

	if *scriptPath == "" || *remoteAddr == "" || *targetID == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s run --script <script.py> --remote <user@host:port> --target-id <node-id>\n", os.Args[0])
		fs.PrintDefaults()
		os.Exit(1)
	}

	wasStarted, err := daemonctl.EnsureStarted(*localNodeID, *daemonPort)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if wasStarted {
		fmt.Println("started daemon")
	}

	sshCfg, err := remote.Parse(*remoteAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := daemonctl.CheckRemoteAcceptance(sshCfg.Host, *daemonPort, *targetID, *jobName); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("remote daemon accepted job for target-id=%s\n", *targetID)

	if *checkOnly {
		fmt.Println("checks complete")
		return
	}

	if err := app.Run(app.Options{
		ScriptPath: *scriptPath,
		Remote:     *remoteAddr,
		TargetID:   *targetID,
		Detach:     *detach,
		JobName:    *jobName,
		Isolate:    *isolate,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runJobs(args []string) {
	fs := flag.NewFlagSet("jobs", flag.ExitOnError)
	limit := fs.Int("limit", 20, "Max jobs to show")
	_ = fs.Parse(args)

	historyPath := jobhistory.BaseDir()
	if err := os.MkdirAll(historyPath, 0755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	items, err := jobhistory.List(*limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(items) == 0 {
		fmt.Println("no jobs found yet (run a non-check-only job first)")
		fmt.Printf("history path: %s\n", historyPath)
		return
	}

	fmt.Printf("history path: %s\n", historyPath)
	fmt.Println("ID\tSTATUS\tMODE\tTARGET\tREMOTE\tDURATION_MS\tSTARTED")
	for _, it := range items {
		mode := "fg"
		if it.Detached {
			mode = "bg"
		}
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%d\t%s\n", it.ID, it.Status, mode, it.TargetID, it.Remote, it.DurationMs, it.StartedAt)
	}
}

func runJobDetails(args []string) {
	fs := flag.NewFlagSet("job", flag.ExitOnError)
	id := fs.String("id", "", "Job ID from `pipedpeer jobs`")
	withOutput := fs.Bool("output", false, "Print saved stdout/stderr for foreground jobs")
	_ = fs.Parse(args)

	if *id == "" {
		fmt.Fprintln(os.Stderr, "--id is required")
		os.Exit(1)
	}

	r, dir, err := jobhistory.ReadRecord(*id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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

	if *withOutput {
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
}
