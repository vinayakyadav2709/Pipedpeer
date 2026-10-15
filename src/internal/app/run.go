package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

type Options struct {
	ScriptPath string
	DaemonHost string
	DaemonPort int
	// SelfDaemonPort is this machine's daemon, asked for its peers when the
	// connection to the node breaks; 0 means the default port.
	SelfDaemonPort int
	TargetID       string
	JobName        string
	Isolate        bool
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
	// Cold sends the whole closure even if the node has it, to time a first run.
	Cold bool
	// Detach returns as soon as the job is running; DetachCh does the same
	// when it closes (Ctrl+Z). The job keeps running on the node either way.
	Detach   bool
	DetachCh <-chan struct{}
	// LeaseID binds the node's lease to the job, so it lives as long as the
	// job does even with nobody renewing it.
	LeaseID string
	// QueuedAt is when the coordinator started looking for a node, so the
	// timeline can show placement. Zero when the node was given.
	QueuedAt time.Time
}

// releaseCancelledLease frees the node's lease when a run is cancelled before
// its job started (build, closure seed, upload). Nothing on the node is bound
// to the lease yet, so without this it held the slot until the running-lease
// TTL, and a GPU node refused other runs for 5 min.
func releaseCancelledLease(opts Options) {
	if opts.LeaseID == "" {
		return
	}
	if err := daemonctl.ReleaseLease(opts.DaemonHost, opts.DaemonPort, opts.LeaseID, "cancelled"); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: could not release lease %s on %s:%d (%v); the node frees it once renewals stop\n",
			opts.LeaseID, opts.DaemonHost, opts.DaemonPort, err)
	}
}

func Run(ctx context.Context, opts Options) (runErr error) {
	if opts.DaemonHost == "" {
		return fmt.Errorf("daemon host is required")
	}
	if opts.DaemonPort <= 0 {
		opts.DaemonPort = daemonctl.DefaultPort
	}

	if _, err := os.Stat(opts.ScriptPath); os.IsNotExist(err) {
		return fmt.Errorf("file not found: %s", opts.ScriptPath)
	}

	absScriptPath, err := filepath.Abs(opts.ScriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	say.Printf("=== Pipedpeer CLI ===\n")
	say.Printf("Script: %s\n", absScriptPath)
	say.Printf("Daemon: %s:%d\n\n", opts.DaemonHost, opts.DaemonPort)

	started := time.Now()
	env, err := BuildEnvironment(ctx, absScriptPath, EnvOptions{
		PythonVersion: opts.PythonVersion,
		Pkgs:          opts.Pkgs,
		Intercept:     opts.Intercept,
		GPU:           opts.GPU,
		Verbose:       say.Verbose,
	}, func(step int, title string) {
		if step == 1 {
			say.Printf("[%d/7] %s\n", step, title)
		} else {
			say.Printf("\n[%d/7] %s\n", step, title)
		}
	})
	if err != nil {
		// Ctrl+C reaches nix too, so a stopped build fails rather than
		// finishing; say it was the cancel, not a broken build.
		if ctx.Err() != nil {
			releaseCancelledLease(opts)
			return fmt.Errorf("environment build stopped (%v): %w", err, ctx.Err())
		}
		return &BuildError{Err: err}
	}
	defer env.Close()
	if ctx.Err() != nil {
		releaseCancelledLease(opts)
		return fmt.Errorf("cancelled before upload: %w", ctx.Err())
	}

	task := Task{
		Options:   opts,
		Script:    absScriptPath,
		started:   started,
		buildTime: time.Since(started),
		single:    true,
	}
	if say.Verbose {
		task.StageFmt = "[%d/7] %s\n"
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
	// single marks a run of its own (not a fan-out task or a DDP rank): it
	// saves the run record and prints its simple view.
	single bool
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
	historyRecord.Strategy = opts.Strategy
	historyRecord.Candidates = opts.Candidates
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	historyRecord.Args = opts.ScriptArgs
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
	// The script's own run, as the node timed it; zero when it never ran.
	var scriptStart time.Time
	var scriptDur time.Duration
	defer func() {
		if detached {
			return // Attach finalizes it
		}
		historyRecord.Stages = timeline.Stages
		if err := jobhistory.Finalize(historyDir, historyRecord, runErr); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not finalize job history %s: %v\n", historyDir, err)
		}
		if task.StageFmt != "" {
			if runErr == nil {
				fmt.Printf("\n=== Done ===\n")
			}
			fmt.Printf("History: %s  (pipedpeer jobs %s --timeline)\n", historyDir, jobhistory.ShortID(historyRecord.ID))
			sum.Total = time.Since(started)
			sum.Err = runErr
			printSummary(os.Stdout, sum)
		}
		if task.single {
			saveRunRecord(historyDir, historyRecord.ID, opts, started, scriptStart, scriptDur, sum.Receipt)
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

	node := opts.DaemonHost
	if opts.TargetID != "" {
		node = shortNode(opts.TargetID)
	}
	uploadStart := time.Now()
	uploadResp, err := daemonctl.UploadJob(ctx, opts.DaemonHost, opts.DaemonPort,
		env.WorkspaceTar, env.NarPath, env.StorePath, env.ScriptRel,
		daemonctl.UploadOptions{SkipBroadcast: opts.SkipBroadcast, Cold: opts.Cold, LeaseID: opts.LeaseID})
	uploadDur := time.Since(uploadStart)
	if err != nil {
		timeline.Add("upload", node, uploadStart, uploadDur)
		if ctx.Err() != nil {
			releaseCancelledLease(opts)
		}
		return fmt.Errorf("upload failed: %w", err)
	}
	sum.Closure = closureLine(uploadResp, uploadDur, opts.Cold)
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
	if ctx.Err() != nil {
		releaseCancelledLease(opts)
		return fmt.Errorf("cancelled before the job started: %w", ctx.Err())
	}

	if opts.Submitter != "" {
		opts.Envs = append(opts.Envs, "PIPEDPEER_SUBMITTER="+opts.Submitter)
	}

	stage(7, "Executing on remote...")
	jobhistory.UpdateStage(historyDir, "executing")
	if opts.EstimatedMemBytes <= 0 {
		say.Printf("      Note: no memory estimate for %s; capping the job at %s (raise with --mem)\n",
			env.ScriptRel, humanize.IBytes(noEstimateMemLimit))
	}
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
		MemLimitBytes: jobMemLimit(opts.EstimatedMemBytes, opts.EstimationTier),
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
	// Only a run that prints its summary samples; a DDP rank's caller
	// samples every rank's node itself.
	var usage *Usage
	if task.StageFmt != "" {
		usage = StartUsage([]UsageNode{{Host: opts.DaemonHost, Port: opts.DaemonPort}})
	}
	res, err := daemonctl.StreamExecute(ctx, opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, execCfg, detach)
	execDur := time.Since(execStart)
	if usage != nil {
		var nodes []nodeUse
		sum.SelfUse, nodes = usage.finish()
		if len(nodes) > 0 {
			sum.NodeUse = nodes[0].Line
		}
	}
	addExecStages(timeline, node, execStart, execDur, res.Timing)
	if tm := res.Timing; tm != nil {
		scriptStart = execStart.Add(time.Duration(tm.ImportMs+tm.SetupMs) * time.Millisecond)
		scriptDur = time.Duration(tm.RunMs) * time.Millisecond
	}
	sum.ScriptRan, sum.ScriptTime, sum.Peak, sum.Receipt = true, execDur, res.PeakBytes, res.Receipt
	historyRecord.PeakMemBytes = res.PeakBytes
	_ = jobhistory.SaveText(historyDir, "stdout.log", res.Stdout)
	_ = jobhistory.SaveText(historyDir, "stderr.log", res.Stderr)
	if len(res.Receipt) > 0 {
		if err := jobhistory.SaveReceipt(historyDir, res.Receipt); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not save the shim receipt in %s: %v\n", historyDir, err)
		}
	}

	// A lost connection is ambiguous: the node died (run the job elsewhere,
	// the failover) or this machine went offline (the job runs on: detach,
	// and this machine's daemon collects it). Only where detaching is offered
	// (not DDP/map).
	endedOnNode := false
	if detach != nil && errors.Is(err, daemonctl.ErrConnectionLost) {
		fmt.Fprintf(os.Stderr, "\nlost the connection to %s (%v); checking whether the node or this machine went away...\n", historyRecord.DaemonEndpoint, err)
		verdict, why := judgeLostConnection(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, opts.SelfDaemonPort)
		switch verdict {
		case lostRerun:
			fmt.Fprintf(os.Stderr, "the node is gone: %s; running the job elsewhere\n", why)
		case lostEnded:
			fmt.Fprintf(os.Stderr, "%s; collecting it\n", why)
			endedOnNode = true
			err = fmt.Errorf("%v: %w", err, daemonctl.ErrDetached)
		default:
			fmt.Fprintf(os.Stderr, "%s; the job keeps running there\n", why)
			err = fmt.Errorf("%v: %w", err, daemonctl.ErrDetached)
		}
	}
	if errors.Is(err, daemonctl.ErrDetached) {
		detached = true
		historyRecord.Detached = true
		historyRecord.RemoteJobID = uploadResp.JobID
		historyRecord.Stages = timeline.Stages
		historyRecord.LocalSyncRoot = resultsDir
		historyRecord.Stage = "executing"
		if saveErr := jobhistory.SaveRecord(historyDir, historyRecord); saveErr != nil {
			// Without the record attach cannot find the job; say where it is.
			fmt.Fprintf(os.Stderr, "warning: could not save the detached job's record in %s: %v (job %s on %s)\n",
				historyDir, saveErr, uploadResp.JobID, historyRecord.DaemonEndpoint)
		}
		short := jobhistory.ShortID(historyRecord.ID)
		if endedOnNode {
			got, _, done, jobErr, cerr := collect(context.WithoutCancel(ctx), historyDir, note)
			if cerr == nil && done {
				if rerr := replayRest(historyDir, res.Stdout, res.Stderr); rerr != nil {
					fmt.Fprintf(os.Stderr, "warning: job %s: the output printed after the connection was lost is missing here: %v; pipedpeer attach %s shows what was saved\n", short, rerr, short)
				}
				if say.Verbose {
					printSummary(os.Stdout, recordSummary(historyDir, got, opts.DaemonHost, opts.DaemonPort))
				}
				return jobErr
			}
			fmt.Fprintf(os.Stderr, "warning: could not collect job %s now (%v); this machine's daemon retries\n", short, cerr)
		}
		fmt.Fprintf(os.Stderr, "\ndetached — job %s keeps running\n", short)
		fmt.Fprintf(os.Stderr, "  reattach: pipedpeer attach %s\n", short)
		return fmt.Errorf("job %s detached: %w", short, err)
	}
	if err != nil {
		if sandboxFailed(err, res.Stderr) {
			err = &SandboxError{Err: err}
		}
		return fmt.Errorf("execution failed: %w", err)
	}

	endSync := timeline.Begin("sync results", "local")
	sum.Files, err = syncResults(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, resultsDir, historyDir, &historyRecord, note)
	endSync()
	return err
}

// ResultsError is a run whose script finished but whose output files did not
// come back. The run fails (and is recorded failed), but running the script
// again elsewhere would not help, so the coordinator must not retry it.
type ResultsError struct{ Err error }

func (e *ResultsError) Error() string { return e.Err.Error() }
func (e *ResultsError) Unwrap() error { return e.Err }

// syncResults downloads the files a job produced into resultsDir and records
// them on rec. A failed download returns a *ResultsError.
func syncResults(host string, port int, jobID, resultsDir, historyDir string, rec *jobhistory.Record, note func(string, ...any)) (*daemonctl.ResultManifest, error) {
	manifest, err := daemonctl.DownloadResults(host, port, jobID, resultsDir)
	if err != nil {
		return nil, &ResultsError{Err: fmt.Errorf("the script finished but its result files did not come back from job %s: %w", jobID, err)}
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
	return manifest, nil
}

// saveRunRecord merges this process's run steps with the shim receipt's
// "steps", adds the script's own run time, saves the record beside the job's
// history and prints its simple view (stderr) when work left this machine,
// plus the detailed view under -v.
func saveRunRecord(historyDir, runID string, opts Options, started, scriptStart time.Time, scriptDur time.Duration, receipt json.RawMessage) {
	steps := runrec.Steps()
	if len(receipt) > 0 {
		var r struct {
			Steps []runrec.Step `json:"steps"`
		}
		if err := json.Unmarshal(receipt, &r); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not read the steps in job %s's receipt: %v\n", jobhistory.ShortID(runID), err)
		}
		steps = append(steps, r.Steps...)
	}
	// Machine ids are node ids (key fingerprints), never addresses.
	me, err := identity.GetOrCreate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not read this node's identity for the run record: %v\n", err)
	}
	ranHere := opts.PlacementSource == "self"
	rec := runrec.Record{RunID: runID, TotalNS: time.Since(started).Nanoseconds(), Steps: steps,
		Names: map[string]string{}}
	if me.NodeID != "" {
		rec.Names[me.NodeID] = me.Hostname
	}
	if !scriptStart.IsZero() {
		machine := me.NodeID
		if !ranHere {
			machine = opts.TargetID
		}
		rec.ScriptNS = scriptDur.Nanoseconds()
		rec.Steps = append(rec.Steps, runrec.Step{Machine: machine, Node: opts.TargetID, Chunk: -1, Name: "script",
			StartNS: scriptStart.UnixNano(), DurNS: rec.ScriptNS})
	}
	left := !ranHere
	var known map[string]string
	for _, s := range rec.Steps {
		if s.Machine == "" || s.Machine == me.NodeID {
			continue
		}
		left = true
		if _, ok := rec.Names[s.Machine]; !ok {
			if known == nil {
				known = nodeNames(opts.DaemonHost, opts.DaemonPort)
			}
			if name, ok := known[s.Machine]; ok {
				rec.Names[s.Machine] = name
			}
		}
	}
	if err := runrec.Save(historyDir, rec); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not save the run record in %s: %v\n", historyDir, err)
	}
	if left {
		runrec.WriteSimple(os.Stderr, runrec.Simple(rec))
	}
	if say.Verbose {
		runrec.WriteDetailed(os.Stderr, rec)
	}
}

// nodeNames maps node id to hostname for every node the daemon at host:port
// knows, itself included.
func nodeNames(host string, port int) map[string]string {
	names := map[string]string{}
	var nodes []struct {
		NodeID       string            `json:"node_id"`
		Capabilities map[string]string `json:"capabilities"`
	}
	url := fmt.Sprintf("http://%s/v1/nodes", net.JoinHostPort(host, strconv.Itoa(port)))
	if err := getJSON(url, &nodes); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not list %s's nodes for the run record's names: %v\n", host, err)
		return names
	}
	for _, n := range nodes {
		if name := n.Capabilities["hostname"]; n.NodeID != "" && name != "" {
			names[n.NodeID] = name
		}
	}
	return names
}

// noEstimateMemLimit caps a job that came with no memory estimate. Generous
// on purpose (the estimator's own fallback is 512 MiB): it only has to stop a
// runaway, and --mem raises it.
const noEstimateMemLimit = 4 << 30

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

// jobMemLimit is the cgroup ceiling a job runs under.
//
// Only --mem (tier "user") is a statement about what the script needs. The
// other tiers are guesses: "import" sums library baselines and knows nothing
// of the data (02_numpy_heavy.py: 300 MiB for numpy, so a 600 MiB cap, while
// its two 134 MB matrices and their product need over 1 GB), and "historical"
// can come from a run that was itself killed at the cap. A guess may raise
// the ceiling but never take it below the no-estimate one; the node still
// lowers it to what it can spare (daemonapi grantableMemory).
func jobMemLimit(estimate int64, tier string) int64 {
	limit := memLimitFor(estimate)
	if tier != "user" && limit < noEstimateMemLimit {
		return noEstimateMemLimit
	}
	return limit
}

// memLimitFor turns a memory estimate into a cgroup ceiling.
//
// Doubling it is deliberate. The estimate comes from import baselines or the
// last run's peak, both approximations, and a limit that kills a job which
// would have completed is a worse failure than the one it prevents. The point
// is to stop a runaway allocation taking the machine down with it, not to
// hold jobs to their forecast.
//
// No estimate still gets a cap: an uncapped sandbox is exactly the runaway
// this exists to stop. The node lowers any cap to what it can spare.
func memLimitFor(estimate int64) int64 {
	if estimate <= 0 {
		return noEstimateMemLimit
	}
	const floor = 512 << 20
	if limit := estimate * 2; limit > floor {
		return limit
	}
	return floor
}
