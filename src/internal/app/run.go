package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

type Options struct {
	ScriptPath string
	DaemonHost string
	DaemonPort int
	TargetID   string
	JobName    string
	Isolate    bool
	// GPU means this job wants a device: it drives placement and selects the
	// CUDA build of torch. Off, the closure is several gigabytes smaller —
	// importing torch is not the same as wanting a GPU.
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
	// Cold sends the whole closure even if the node has it, to time a first run.
	Cold bool
	// Detach returns as soon as the job is running; DetachCh does the same
	// when it closes (Ctrl+Z). The job keeps running on the node either way.
	Detach   bool
	DetachCh <-chan struct{}
	// LeaseID binds the node's lease to the job, so it lives as long as the
	// job does even with nobody renewing it.
	LeaseID string
}

func Run(ctx context.Context, opts Options) (runErr error) {
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

	started := time.Now()
	env, err := BuildEnvironment(absScriptPath, EnvOptions{
		PythonVersion: opts.PythonVersion,
		Pkgs:          opts.Pkgs,
		Intercept:     opts.Intercept,
		GPU:           opts.GPU,
	}, func(step int, title string) {
		if step == 1 {
			fmt.Printf("[%d/7] %s\n", step, title)
		} else {
			fmt.Printf("\n[%d/7] %s\n", step, title)
		}
	})
	if err != nil {
		// Ctrl+C reaches nix too, so a stopped build fails rather than
		// finishing; say it was the cancel, not a broken build.
		if ctx.Err() != nil {
			return fmt.Errorf("environment build stopped (%v): %w", err, ctx.Err())
		}
		return err
	}
	defer env.Close()
	if ctx.Err() != nil {
		return fmt.Errorf("cancelled before upload: %w", ctx.Err())
	}

	task := Task{
		Options:   opts,
		Script:    absScriptPath,
		StageFmt:  "[%d/7] %s\n",
		started:   started,
		buildTime: time.Since(started),
	}
	return RunTask(ctx, env, task)
}

// Task is one execution of a script in an already-built environment.
type Task struct {
	Options Options
	Script  string
	// Label distinguishes tasks in a fan-out; empty for a single run.
	Label string
	// StageFmt formats the upload/execute headings. Empty means quiet, which
	// is what a fan-out wants since many tasks share the terminal. A run
	// with headings also gets the summary at the end.
	StageFmt string

	// Set by Run so the summary covers the build too.
	started   time.Time
	buildTime time.Duration
}

// RunTask uploads a prepared environment to a node, runs the script there, and
// brings the results back. It is the per-node half of a run: everything it
// needs that is node-independent already lives in env.
//
// A cancelled ctx stops the job on the node; the returned error then wraps
// context.Canceled. A detach returns an error wrapping daemonctl.ErrDetached
// with the job still running and its history record left open for Attach.
func RunTask(ctx context.Context, env *Environment, task Task) (runErr error) {
	opts := task.Options
	started := task.started
	if started.IsZero() {
		started = time.Now()
	}
	sum := summary{Host: opts.DaemonHost, Port: opts.DaemonPort, BuildTime: task.buildTime}
	detached := false

	resolvedJobName := opts.JobName
	if resolvedJobName == "" {
		resolvedJobName = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	historyRecord, historyDir, err := jobhistory.NewRecord(task.Script, "", opts.TargetID, opts.Detach, opts.Isolate)
	if err != nil {
		return fmt.Errorf("failed to initialize job history: %v", err)
	}
	historyRecord.JobName = resolvedJobName
	historyRecord.JobSet = opts.JobSet
	historyRecord.RunHost = opts.DaemonHost
	historyRecord.DaemonEndpoint = net.JoinHostPort(opts.DaemonHost, strconv.Itoa(opts.DaemonPort))
	historyRecord.PlacementSource = opts.PlacementSource
	historyRecord.DegradedMode = opts.DegradedMode
	historyRecord.CandidateCount = opts.CandidateCount
	historyRecord.PlacementReason = opts.PlacementReason
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	historyRecord.StorePath = env.StorePath
	_ = jobhistory.SaveRecord(historyDir, historyRecord)

	defer func() {
		if detached {
			return // Attach finalizes it
		}
		if err := jobhistory.Finalize(historyDir, historyRecord, runErr); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not finalize job history %s: %v\n", historyDir, err)
		}
		if task.StageFmt != "" {
			if runErr == nil {
				fmt.Printf("\n=== Done ===\n")
			}
			fmt.Printf("History: %s\n", historyDir)
			sum.Total = time.Since(started)
			sum.Err = runErr
			printSummary(os.Stdout, sum)
		}
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

	uploadStart := time.Now()
	uploadResp, err := daemonctl.UploadJob(opts.DaemonHost, opts.DaemonPort,
		env.WorkspaceTar, env.NarPath, env.StorePath, env.ScriptRel,
		daemonctl.UploadOptions{SkipBroadcast: opts.SkipBroadcast, Cold: opts.Cold})
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	sum.Closure = closureLine(uploadResp, time.Since(uploadStart), opts.Cold)
	note("      Uploaded job %s\n", uploadResp.JobID)
	if ctx.Err() != nil {
		return fmt.Errorf("cancelled before the job started: %w", ctx.Err())
	}

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
		// Headroom over the estimate: the estimator is deliberately rough,
		// and a cap that trips on a job that would have fitted is worse than
		// no cap. This only has to catch runaway growth.
		MemLimitBytes: memLimitFor(opts.EstimatedMemBytes),
		LeaseID:       opts.LeaseID,
	}

	detach := opts.DetachCh
	if opts.Detach {
		// Closed up front: StreamExecute detaches once the job is started.
		ch := make(chan struct{})
		close(ch)
		detach = ch
	}

	resultsDir := opts.ResultsDir
	if resultsDir == "" {
		resultsDir = env.ProjectRoot
	}

	execStart := time.Now()
	peakBytes, stdoutText, stderrText, err := daemonctl.StreamExecute(ctx, opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, execCfg, detach)
	sum.ScriptRan, sum.ScriptTime, sum.Peak, sum.Stderr = true, time.Since(execStart), peakBytes, stderrText
	historyRecord.PeakMemBytes = peakBytes
	_ = jobhistory.SaveText(historyDir, "stdout.log", stdoutText)
	_ = jobhistory.SaveText(historyDir, "stderr.log", stderrText)

	if errors.Is(err, daemonctl.ErrDetached) {
		detached = true
		historyRecord.Detached = true
		historyRecord.RemoteJobID = uploadResp.JobID
		historyRecord.LocalSyncRoot = resultsDir
		historyRecord.Stage = "executing"
		if saveErr := jobhistory.SaveRecord(historyDir, historyRecord); saveErr != nil {
			// Without the record attach cannot find the job; say where it is.
			fmt.Fprintf(os.Stderr, "warning: could not save the detached job's record in %s: %v (job %s on %s)\n",
				historyDir, saveErr, uploadResp.JobID, historyRecord.DaemonEndpoint)
		}
		short := jobhistory.ShortID(historyRecord.ID)
		fmt.Printf("\ndetached — job %s keeps running\n", short)
		fmt.Printf("  reattach: pipedpeer attach %s\n", short)
		return fmt.Errorf("job %s detached: %w", short, err)
	}
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	sum.Files = syncResults(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, resultsDir, historyDir, &historyRecord, note)
	return nil
}

// syncResults downloads the files a job produced into resultsDir and records
// them on rec. It returns nil when the download failed (and says so).
func syncResults(host string, port int, jobID, resultsDir, historyDir string, rec *jobhistory.Record, note func(string, ...any)) *daemonctl.ResultManifest {
	manifest, err := daemonctl.DownloadResults(host, port, jobID, resultsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "      Warning: results download for job %s failed: %v\n", jobID, err)
		return nil
	}
	manifest.JobID = jobID
	manifestPath, writeErr := jobhistory.WriteManifest(historyDir, manifest)
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "      Warning: could not record manifest: %v\n", writeErr)
	}
	// Set on the record rather than on disk: the deferred Finalize writes
	// this same value out at the end of the run.
	rec.LocalSyncRoot = resultsDir
	rec.ReceivedFiles = manifest.Count()
	rec.NewFiles = len(manifest.New)
	rec.UpdatedFiles = len(manifest.Updated)
	rec.DeletedFiles = len(manifest.Deleted)
	rec.ManifestPath = manifestPath
	if manifest.Count() == 0 && len(manifest.Deleted) == 0 {
		note("      No files changed on the remote\n")
	} else {
		note("      Synced %d file(s) to %s (%d new, %d updated, %d deleted)\n",
			manifest.Count(), resultsDir, len(manifest.New), len(manifest.Updated), len(manifest.Deleted))
	}
	return manifest
}

// memLimitFor turns a memory estimate into a cgroup ceiling.
//
// Doubling it is deliberate. The estimate comes from import baselines or the
// last run's peak, both approximations, and a limit that kills a job which
// would have completed is a worse failure than the one it prevents. The point
// is to stop a runaway allocation taking the machine down with it, not to
// hold jobs to their forecast.
func memLimitFor(estimate int64) int64 {
	if estimate <= 0 {
		return 0 // nothing to go on: leave it uncapped rather than guess
	}
	const floor = 512 << 20
	if limit := estimate * 2; limit > floor {
		return limit
	}
	return floor
}
