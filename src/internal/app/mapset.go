package app

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// MapTask is one unit of work in a fan-out. It differs from a single Task only
// in that it carries its own arguments and shard identity.
type MapTask struct {
	Args    []string
	Envs    []string
	ShardID int
}

// MapSpec describes how to turn a script + data source into a list of tasks.
type MapSpec struct {
	ScriptPath string
	// Inputs are glob patterns; each matched file becomes one task whose first
	// argument is the file path.
	Inputs []string
	// ArgsFile holds one task per non-empty line; each line is the task's args.
	ArgsFile string
	// SplitSource is a record-oriented file to split into shard files.
	SplitSource string
	// Split is "rows:N" (chunk a record file into N-row shards) or "parts:N"
	// (split into N equal shards).
	Split string
	// NodeCount is used by "parts:auto" (≈ 2–3× eligible nodes) and defaults
	// to 1 when unknown.
	NodeCount int
	// ShardDir is where split shard files are written (a temp dir otherwise).
	ShardDir string
}

// ResolveMapTasks expands a MapSpec into a concrete list of tasks. It is the
// only place that reads the data source, so a fan-out can't guess wrong: it
// either produces tasks or errors before anything is placed.
func ResolveMapTasks(spec MapSpec) ([]MapTask, string, error) {
	explicit := 0
	if len(spec.Inputs) > 0 {
		explicit++
	}
	if spec.ArgsFile != "" {
		explicit++
	}
	if spec.SplitSource != "" {
		explicit++
	}
	if explicit > 1 {
		return nil, "", fmt.Errorf("specify only one of --inputs, --args-file, or --input/--split")
	}

	switch {
	case len(spec.Inputs) > 0:
		return tasksFromInputs(spec.ScriptPath, spec.Inputs)

	case spec.ArgsFile != "":
		return tasksFromArgsFile(spec.ArgsFile)

	case spec.SplitSource != "":
		return tasksFromSplit(spec)

	default:
		// No data source: a single task with no args — a degenerate but valid map.
		return []MapTask{{}}, "", nil
	}
}

func tasksFromInputs(scriptPath string, patterns []string) ([]MapTask, string, error) {
	var tasks []MapTask
	var matched []string
	for _, p := range patterns {
		files, err := filepath.Glob(p)
		if err != nil {
			return nil, "", fmt.Errorf("bad --inputs pattern %q: %v", p, err)
		}
		matched = append(matched, files...)
	}
	if len(matched) == 0 {
		return nil, "", fmt.Errorf("--inputs matched no files")
	}
	for i, f := range matched {
		abs, err := filepath.Abs(f)
		if err != nil {
			abs = f
		}
		tasks = append(tasks, MapTask{Args: []string{abs}, ShardID: i})
	}
	return tasks, "", nil
}

func tasksFromArgsFile(path string) ([]MapTask, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("open args file: %v", err)
	}
	defer f.Close()

	var tasks []MapTask
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tasks = append(tasks, MapTask{Args: []string{line}, ShardID: len(tasks)})
	}
	if err := sc.Err(); err != nil {
		return nil, "", err
	}
	if len(tasks) == 0 {
		return nil, "", fmt.Errorf("args file %q has no task lines", path)
	}
	return tasks, "", nil
}

func tasksFromSplit(spec MapSpec) ([]MapTask, string, error) {
	mode, n, err := parseSplit(spec.Split, spec.NodeCount)
	if err != nil {
		return nil, "", err
	}

	dir := spec.ShardDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "pipedpeer-shards-*")
		if err != nil {
			return nil, "", err
		}
	}

	switch ext := strings.ToLower(filepath.Ext(spec.SplitSource)); ext {
	case ".csv":
		return splitCSV(spec.SplitSource, dir, mode, n)
	case ".jsonl", ".ndjson", ".txt":
		return splitLines(spec.SplitSource, dir, mode, n)
	default:
		return nil, "", fmt.Errorf("cannot safely split %q (%s): only csv, jsonl, ndjson, txt are supported", spec.SplitSource, ext)
	}
}

// splitMode says whether the split value is a per-shard row count ("rows") or
// a total shard count ("parts").
type splitMode int

const (
	splitByRows splitMode = iota
	splitByParts
)

// parseSplit turns "rows:N", "parts:N", or "parts:auto" into a split mode and
// its value. Defaults to a single part. Auto sizes to ~2× the node count so
// fan-out has work to pull without over-splitting.
func parseSplit(spec string, nodeCount int) (splitMode, int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return splitByParts, 1, nil
	}
	kind, val, ok := strings.Cut(spec, ":")
	if !ok {
		return 0, 0, fmt.Errorf("--split must be rows:N or parts:N, got %q", spec)
	}
	switch kind {
	case "rows":
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("invalid --split rows value %q", val)
		}
		return splitByRows, n, nil
	case "parts":
		if val == "auto" {
			n := 2 * nodeCount
			if n < 1 {
				n = 1
			}
			if n > 512 {
				n = 512
			}
			return splitByParts, n, nil
		}
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("invalid --split parts value %q", val)
		}
		if n > 512 {
			return 0, 0, fmt.Errorf("refusing >512 shards (%d)", n)
		}
		return splitByParts, n, nil
	default:
		return 0, 0, fmt.Errorf("--split must be rows:N or parts:N, got %q", spec)
	}
}

// splitCSV shards a CSV, preserving the header on every shard so each shard is
// independently consumable.
func splitCSV(path, dir string, mode splitMode, n int) ([]MapTask, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // tolerate ragged rows; we only re-emit what we read

	header, err := r.Read()
	if err == io.EOF {
		return nil, "", fmt.Errorf("%s is empty", path)
	}
	if err != nil {
		return nil, "", err
	}

	// Count records to divide evenly without buffering everything in memory.
	recordCount := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
		recordCount++
	}
	shardSize := shardRows(mode, n, recordCount)
	if shardSize == 0 {
		shardSize = 1
	}

	f.Seek(0, io.SeekStart)
	r = csv.NewReader(f)
	r.FieldsPerRecord = -1
	if _, err := r.Read(); err != nil { // re-consume header
		return nil, "", err
	}

	var tasks []MapTask
	for part := 0; ; part++ {
		out, err := os.Create(filepath.Join(dir, fmt.Sprintf("shard-%03d.csv", part)))
		if err != nil {
			return nil, "", err
		}
		w := csv.NewWriter(out)
		_ = w.Write(header)
		rows := 0
		for rows < shardSize {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				out.Close()
				return nil, "", err
			}
			_ = w.Write(rec)
			rows++
		}
		w.Flush()
		if err := w.Error(); err != nil {
			out.Close()
			return nil, "", err
		}
		out.Close()
		if rows == 0 {
			os.Remove(out.Name())
			break // no more records
		}
		tasks = append(tasks, MapTask{Args: []string{out.Name()}, ShardID: part})
	}
	return tasks, dir, nil
}

// splitLines shards a line-delimited file (JSONL, txt).
func splitLines(path, dir string, mode splitMode, n int) ([]MapTask, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	lineCount := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lineCount++
	}
	if err := sc.Err(); err != nil {
		return nil, "", err
	}
	shardSize := shardRows(mode, n, lineCount)
	if shardSize == 0 {
		shardSize = 1
	}

	f.Seek(0, io.SeekStart)
	sc = bufio.NewScanner(f)

	var tasks []MapTask
	for part := 0; ; part++ {
		out, err := os.Create(filepath.Join(dir, fmt.Sprintf("shard-%03d.txt", part)))
		if err != nil {
			return nil, "", err
		}
		bw := bufio.NewWriter(out)
		rows := 0
		for rows < shardSize {
			if !sc.Scan() {
				break
			}
			fmt.Fprintln(bw, sc.Text())
			rows++
		}
		if err := sc.Err(); err != nil {
			out.Close()
			return nil, "", err
		}
		bw.Flush()
		out.Close()
		if rows == 0 {
			os.Remove(out.Name())
			break
		}
		tasks = append(tasks, MapTask{Args: []string{out.Name()}, ShardID: part})
	}
	return tasks, dir, nil
}

// shardRows computes rows-per-shard: rows mode uses N directly; parts mode
// divides the total count into N equal shards.
func shardRows(mode splitMode, n, total int) int {
	if mode == splitByRows {
		return n
	}
	return (total + n - 1) / n
}

// ShardEnvs returns the PIPEDPEER_SHARD_ID/NUM_SHARDS envs for a task.
func ShardEnvs(shardID, numShards int) []string {
	return []string{
		"PIPEDPEER_SHARD_ID=" + strconv.Itoa(shardID),
		"PIPEDPEER_NUM_SHARDS=" + strconv.Itoa(numShards),
	}
}

// MapRunner places and executes one task on the cluster. It is supplied by the
// caller so coordinator placement (which lives in main) stays out of app.
type MapRunner func(task MapTask, base Options) (nodeID string, err error)

// RunMap builds nothing itself — the environment is built once by the caller —
// and fans the task list out across the cluster at most concurrency at a time.
// Each task gets its own results directory under resultsDir/<task-N>/ and the
// PIPEDPEER_SHARD_* envs. On completion it prints one summary line per task.
//
// reduce, if set, is a script run locally over the gathered result dirs after
// all tasks finish.
func RunMap(tasks []MapTask, base Options, concurrency int, resultsDir string, env *Environment, run MapRunner, reduce string) error {
	if concurrency <= 0 {
		concurrency = len(tasks)
		if concurrency == 0 {
			concurrency = 1
		}
	}
	numShards := len(tasks)

	sem := make(chan struct{}, concurrency)
	errs := make([]error, len(tasks))
	var wg sync.WaitGroup

	for i := range tasks {
		task := tasks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			base.ResultsDir = filepath.Join(resultsDir, fmt.Sprintf("task-%d", task.ShardID))
			base.JobSet = filepath.Base(resultsDir)
			base.ScriptArgs = task.Args
			base.Envs = append(append([]string{}, base.Envs...), ShardEnvs(task.ShardID, numShards)...)
			if len(task.Envs) > 0 {
				base.Envs = append(base.Envs, task.Envs...)
			}

			nodeID, err := run(task, base)
			if err != nil {
				errs[task.ShardID] = err
				fmt.Printf("[task %d] failed on %s: %v\n", task.ShardID, shortNode(nodeID), err)
				return
			}
			fmt.Printf("[task %d] ok on %s → %s\n", task.ShardID, shortNode(nodeID), base.ResultsDir)
		}()
	}
	wg.Wait()

	var failed int
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	fmt.Printf("\nmap complete: %d/%d tasks succeeded\n", len(tasks)-failed, len(tasks))

	if reduce != "" {
		return runReduce(reduce, resultsDir)
	}
	if failed > 0 {
		return fmt.Errorf("%d task(s) failed", failed)
	}
	return nil
}

func runReduce(script, resultsDir string) error {
	cmd := exec.Command("python", script)
	cmd.Dir = resultsDir
	cmd.Env = append(os.Environ(), "PIPEDPEER_RESULTS="+resultsDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("reduce failed: %v", err)
	}
	return nil
}

func shortNode(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
