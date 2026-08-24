package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

type Options struct {
	ScriptPath    string
	DaemonHost    string
	DaemonPort    int
	TargetID      string
	JobName       string
	Isolate       bool
	GPU           bool
	GPUDevices    string
	Mode          string
	PythonVersion string
	Envs          []string
	// Submitter is the submitting machine's daemon endpoint ("host:port").
	// When set it travels to the executing node as PIPEDPEER_SUBMITTER so
	// spill ranking sinks the orchestrator behind real workers.
	Submitter  string
	Pkgs       []string
	ScriptArgs []string
	// ResultsDir is where files the job produced are written. Defaults to the
	// project root, which is what a single interactive run wants; a fan-out
	// gives each task its own directory so tasks cannot overwrite each other.
	ResultsDir string
	// JobSet groups this run's jobhistory record under a fan-out, so the jobs
	// view can show all tasks of one map run together.
	JobSet string
	// Intercept embeds the sitecustomize shim and routes parallel primitives
	// (multiprocessing.Pool, ProcessPoolExecutor, joblib) through the cluster.
	Intercept bool
	// SkipBroadcast tells the target daemon not to fan the closure out to the
	// other healthy peers. DDP ranks upload directly to their own node and the
	// other ranks already get their copy the same way, so broadcasting would
	// push the closure to nodes that don't execute it.
	SkipBroadcast bool
	// Coordinator placement diagnostics
	PlacementSource string
	DegradedMode    bool
	CandidateCount  int
	PlacementReason string
	// Resource estimation
	EstimatedMemBytes int64
	EstimationTier    string
}

func Run(opts Options) (runErr error) {
	if opts.DaemonHost == "" {
		return fmt.Errorf("daemon host is required")
	}
	if opts.DaemonPort <= 0 {
		opts.DaemonPort = 38080
	}

	if _, err := os.Stat(opts.ScriptPath); os.IsNotExist(err) {
		return fmt.Errorf("file not found: %s", opts.ScriptPath)
	}

	absScriptPath, err := filepath.Abs(opts.ScriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", absScriptPath)
	fmt.Printf("Daemon: %s:%d\n\n", opts.DaemonHost, opts.DaemonPort)

	env, err := BuildEnvironment(absScriptPath, EnvOptions{
		PythonVersion: opts.PythonVersion,
		Pkgs:          opts.Pkgs,
		Intercept:     opts.Intercept,
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

	task := Task{
		Options:  opts,
		Script:   absScriptPath,
		StageFmt: "[%d/7] %s\n",
	}
	return RunTask(env, task)
}

// Task is one execution of a script in an already-built environment.
type Task struct {
	Options Options
	Script  string
	// Label distinguishes tasks in a fan-out; empty for a single run.
	Label string
	// StageFmt formats the upload/execute headings. Empty means quiet, which
	// is what a fan-out wants since many tasks share the terminal.
	StageFmt string
}

// RunTask uploads a prepared environment to a node, runs the script there, and
// brings the results back. It is the per-node half of a run: everything it
// needs that is node-independent already lives in env.
func RunTask(env *Environment, task Task) (runErr error) {
	opts := task.Options

	resolvedJobName := opts.JobName
	if resolvedJobName == "" {
		resolvedJobName = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	historyRecord, historyDir, err := jobhistory.NewRecord(task.Script, "", opts.TargetID, false, opts.Isolate)
	if err != nil {
		return fmt.Errorf("failed to initialize job history: %v", err)
	}
	historyRecord.JobName = resolvedJobName
	historyRecord.JobSet = opts.JobSet
	historyRecord.RunHost = opts.DaemonHost
	historyRecord.PlacementSource = opts.PlacementSource
	historyRecord.DegradedMode = opts.DegradedMode
	historyRecord.CandidateCount = opts.CandidateCount
	historyRecord.PlacementReason = opts.PlacementReason
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	historyRecord.StorePath = env.StorePath
	_ = jobhistory.SaveRecord(historyDir, historyRecord)

	defer func() {
		_ = jobhistory.Finalize(historyDir, historyRecord, runErr)
	}()
	_ = jobhistory.CopyFileTo(historyDir, task.Script, "script.py")
	_ = jobhistory.SaveText(historyDir, "flake.nix", env.FlakeContent)

	stage := func(step int, title string) {
		if task.StageFmt != "" {
			fmt.Printf("\n"+task.StageFmt, step, title)
		}
	}
	note := func(format string, args ...any) {
		if task.StageFmt != "" {
			fmt.Printf(format, args...)
		}
	}

	stage(6, "Uploading to daemon...")
	jobhistory.UpdateStage(historyDir, "uploading")
	note("      Project root: %s\n", env.ProjectRoot)
	note("      Script: %s\n", env.ScriptRel)

	uploadResp, err := daemonctl.UploadJob(opts.DaemonHost, opts.DaemonPort,
		env.WorkspaceTar, env.NarPath, env.StorePath, env.ScriptRel, opts.SkipBroadcast)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	note("      Uploaded job %s\n", uploadResp.JobID)

	if opts.Submitter != "" {
		opts.Envs = append(opts.Envs, "PIPEDPEER_SUBMITTER="+opts.Submitter)
	}

	stage(7, "Executing on remote...")
	jobhistory.UpdateStage(historyDir, "executing")
	execCfg := daemonctl.ExecConfig{
		ScriptPath: env.ScriptRel,
		Args:       opts.ScriptArgs,
		Envs:       opts.Envs,
		Isolate:    opts.Isolate,
		StorePath:  env.StorePath,
		GPU:        opts.GPU,
		GPUDevices: opts.GPUDevices,
		Intercept:  opts.Intercept,
	}

	peakBytes, stdoutText, stderrText, err := daemonctl.StreamExecute(context.Background(), opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, execCfg)
	if err != nil {
		return fmt.Errorf("execution failed: %v", err)
	}
	historyRecord.PeakMemBytes = peakBytes
	_ = jobhistory.SaveText(historyDir, "stdout.log", stdoutText)
	_ = jobhistory.SaveText(historyDir, "stderr.log", stderrText)

	resultsDir := opts.ResultsDir
	if resultsDir == "" {
		resultsDir = env.ProjectRoot
	}
	manifest, err := daemonctl.DownloadResults(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, resultsDir)
	if err != nil {
		note("      Warning: results download failed: %v\n", err)
	} else {
		manifest.JobID = uploadResp.JobID
		manifestPath, writeErr := jobhistory.WriteManifest(historyDir, manifest)
		if writeErr != nil {
			note("      Warning: could not record manifest: %v\n", writeErr)
		}
		// Set on the record rather than on disk: the deferred Finalize writes
		// this same value out at the end of the run.
		historyRecord.LocalSyncRoot = resultsDir
		historyRecord.ReceivedFiles = manifest.Count()
		historyRecord.NewFiles = len(manifest.New)
		historyRecord.UpdatedFiles = len(manifest.Updated)
		historyRecord.DeletedFiles = len(manifest.Deleted)
		historyRecord.ManifestPath = manifestPath
		if manifest.Count() == 0 && len(manifest.Deleted) == 0 {
			note("      No files changed on the remote\n")
		} else {
			note("      Synced %d file(s) to %s (%d new, %d updated, %d deleted)\n",
				manifest.Count(), resultsDir, len(manifest.New), len(manifest.Updated), len(manifest.Deleted))
		}
	}

	if task.StageFmt != "" {
		fmt.Printf("\n=== Done ===\n")
		fmt.Printf("History: %s\n", historyDir)
	}
	return nil
}
