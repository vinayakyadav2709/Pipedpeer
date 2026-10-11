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
	Strategy        string
	Candidates      []jobhistory.Candidate
	// Resource estimation
	EstimatedMemBytes int64
	EstimationTier    string
	// QueuedAt is when the coordinator started looking for a node, so the
	// timeline can show placement. Zero when the node was given.
	QueuedAt time.Time
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
		GPU:           opts.GPU,
	}, func(step int, title string) {
		if step == 1 {
			fmt.Printf("[%d/7] %s\n", step, title)
		} else {
			fmt.Printf("\n[%d/7] %s\n", step, title)
		}
	})
	if err != nil {
		return &BuildError{Err: err}
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
	historyRecord.Strategy = opts.Strategy
	historyRecord.Candidates = opts.Candidates
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	historyRecord.StorePath = env.StorePath
	_ = jobhistory.SaveRecord(historyDir, historyRecord)

	// The timeline starts with how long the coordinator spent finding a node
	// (when it did), then the environment build, then this run's own steps.
	timeline := &jobhistory.Timeline{}
	if !opts.QueuedAt.IsZero() && len(env.Stages) > 0 {
		if built, err := time.Parse(time.RFC3339Nano, env.Stages[0].At); err == nil && built.After(opts.QueuedAt) {
			timeline.Add("placement", "local", opts.QueuedAt, built.Sub(opts.QueuedAt))
		}
	}
	timeline.Stages = append(timeline.Stages, env.Stages...)
	defer func() {
		historyRecord.Stages = timeline.Stages
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

	node := opts.DaemonHost
	if opts.TargetID != "" {
		node = shortNode(opts.TargetID)
	}
	uploadStart := time.Now()
	uploadResp, err := daemonctl.UploadJob(opts.DaemonHost, opts.DaemonPort,
		env.WorkspaceTar, env.NarPath, env.StorePath, env.ScriptRel, opts.SkipBroadcast)
	uploadDur := time.Since(uploadStart)
	if err != nil {
		timeline.Add("upload", node, uploadStart, uploadDur)
		return fmt.Errorf("upload failed: %v", err)
	}
	// The node answers the upload only after handing the closure to its own
	// peers, which on a cold cluster is most of the wait.
	seed := time.Duration(uploadResp.SeedMs) * time.Millisecond
	if seed > uploadDur {
		seed = uploadDur
	}
	timeline.Add("upload", node, uploadStart, uploadDur-seed)
	if seed > 0 {
		timeline.Add("seed peers", node, uploadStart.Add(uploadDur-seed), seed)
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
		// Headroom over the estimate: the estimator is deliberately rough,
		// and a cap that trips on a job that would have fitted is worse than
		// no cap. This only has to catch runaway growth.
		MemLimitBytes: memLimitFor(opts.EstimatedMemBytes),
	}

	execStart := time.Now()
	res, err := daemonctl.StreamExecute(context.Background(), opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, execCfg)
	addExecStages(timeline, node, execStart, time.Since(execStart), res.Timing)
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}
	historyRecord.PeakMemBytes = res.PeakBytes
	_ = jobhistory.SaveText(historyDir, "stdout.log", res.Stdout)
	_ = jobhistory.SaveText(historyDir, "stderr.log", res.Stderr)
	var receiptLine string
	if len(res.Receipt) > 0 {
		_ = jobhistory.SaveReceipt(historyDir, res.Receipt)
		if rc, ok, _ := jobhistory.ReadReceipt(historyDir); ok {
			receiptLine = rc.Summary()
		}
	}

	resultsDir := opts.ResultsDir
	if resultsDir == "" {
		resultsDir = env.ProjectRoot
	}
	endSync := timeline.Begin("sync results", "local")
	manifest, err := daemonctl.DownloadResults(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, resultsDir)
	endSync()
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
		fmt.Printf("Timeline: %s (total %s)\n", jobhistory.Summary(timeline.Stages),
			jobhistory.Total(timeline.Stages).Round(100*time.Millisecond))
		if receiptLine != "" {
			fmt.Printf("Shim: %s  (--receipt for detail)\n", receiptLine)
		}
		fmt.Printf("History: %s  (pipedpeer job --id %s --timeline)\n", historyDir, historyRecord.ID)
	}
	return nil
}

// addExecStages splits the execution into the node's own account of it - the
// closure import, the sandbox setup, the script itself - and whatever is left
// over, which is the round trip and output streaming. A node too old to
// report timing gets a single "execute" stage.
func addExecStages(t *jobhistory.Timeline, node string, start time.Time, total time.Duration, tm *daemonctl.ExecTiming) {
	if tm == nil {
		t.Add("execute", node, start, total)
		return
	}
	ms := func(v int64) time.Duration { return time.Duration(v) * time.Millisecond }
	at := start
	if tm.ImportMs > 0 {
		t.Add("closure import", node, at, ms(tm.ImportMs))
		at = at.Add(ms(tm.ImportMs))
	}
	t.Add("sandbox setup", node, at, ms(tm.SetupMs))
	at = at.Add(ms(tm.SetupMs))
	t.Add("script", node, at, ms(tm.RunMs))
	if rest := total - ms(tm.ImportMs+tm.SetupMs+tm.RunMs); rest >= 50*time.Millisecond {
		t.Add("stream + wrap-up", node, at.Add(ms(tm.RunMs)), rest)
	}
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
