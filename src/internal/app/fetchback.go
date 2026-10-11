package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/rs/zerolog"
)

// collectEvery is how often the daemon looks for detached jobs that ended.
const collectEvery = 30 * time.Second

// collectTimeout bounds one job's output fetch and results download.
const collectTimeout = 10 * time.Minute

// WatchDetached runs in this machine's daemon until ctx ends. Every
// collectEvery it finishes each detached job started from this machine whose
// node reports it ended, exactly as a foreground run finishes (see collect),
// so detaching only changes whether the output is watched live. A node that
// cannot be reached is retried on the next tick; each job warns once.
func WatchDetached(ctx context.Context, log zerolog.Logger) {
	warned := map[string]bool{}
	t := time.NewTicker(collectEvery)
	defer t.Stop()
	for {
		collectDetached(ctx, log, warned)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// collectDetached is one pass of WatchDetached over the job history.
func collectDetached(ctx context.Context, log zerolog.Logger, warned map[string]bool) {
	base := jobhistory.BaseDir()
	entries, err := os.ReadDir(base)
	if err != nil {
		if !os.IsNotExist(err) && !warned[base] {
			warned[base] = true
			log.Warn().Err(err).Str("dir", base).Msg("detached jobs: could not list the job history")
		}
		return
	}
	for _, e := range entries {
		dir := filepath.Join(base, e.Name())
		if !e.IsDir() || !jobhistory.IsRecordDir(dir) {
			continue
		}
		rec, err := jobhistory.ReadRecordAt(dir)
		if err != nil {
			if !warned[dir] {
				warned[dir] = true
				log.Warn().Err(err).Str("dir", dir).Msg("detached jobs: unreadable job record; skipping it")
			}
			continue
		}
		if !pendingDetached(rec) {
			continue
		}
		note := func(format string, args ...any) {
			log.Info().Str("job", rec.ID).Msg(strings.TrimSpace(fmt.Sprintf(format, args...)))
		}
		cctx, cancel := context.WithTimeout(ctx, collectTimeout)
		got, n, done, err := collect(cctx, dir, note)
		cancel()
		if err != nil {
			// ponytail: a job its node no longer knows is retried every tick
			// for the daemon's lifetime; cheap, and the warning says why.
			if !warned[rec.ID] {
				warned[rec.ID] = true
				log.Warn().Err(err).Str("job", rec.ID).Str("from", rec.DaemonEndpoint).
					Msgf("could not collect a detached job; retrying every %s", collectEvery)
			}
			continue
		}
		if done && n > 0 {
			delete(warned, rec.ID)
			log.Info().Str("job", got.ID).Str("from", got.DaemonEndpoint).Int64("bytes", n).
				Str("status", got.Status).Int("files", got.ReceivedFiles).Msg("job collected")
		}
	}
}

// pendingDetached: a detached job this machine has not finished yet.
func pendingDetached(r jobhistory.Record) bool {
	return r.Detached && r.RemoteJobID != "" && r.DaemonEndpoint != "" &&
		!r.Collected && r.Status == jobhistory.StateRunning
}

func jobEnded(status string) bool {
	return status == "done" || status == "failed" || status == "cancelled"
}

func splitEndpoint(ep string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(ep)
	if err != nil {
		return "", 0, fmt.Errorf("unreadable daemon address %q: %v", ep, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("unreadable daemon port %q: %v", ep, err)
	}
	return host, port, nil
}

// collect finishes a detached job that ended on its node the way a
// foreground run finishes: its whole output saved as output.jsonl (for
// attach) plus stdout.log, stderr.log and the receipt, peak memory and the
// node's timing on the record, the results synced into LocalSyncRoot with
// the foreground's code and rules (only when the script succeeded), and the
// record finalized and marked Collected. done is false, with no error, while
// the job still runs. Idempotent: under the record's lock, a job already
// collected is returned as it is, so the watcher and attach never sync twice.
// n is the output bytes fetched, 0 when nothing was done.
func collect(ctx context.Context, dir string, note func(string, ...any)) (rec jobhistory.Record, n int64, done bool, err error) {
	unlock, err := jobhistory.Lock(dir)
	if err != nil {
		return rec, 0, false, err
	}
	defer unlock()
	rec, err = jobhistory.ReadRecordAt(dir)
	if err != nil {
		return rec, 0, false, err
	}
	if rec.Collected {
		return rec, 0, true, nil
	}
	host, port, err := splitEndpoint(rec.DaemonEndpoint)
	if err != nil {
		return rec, 0, false, err
	}
	st, err := daemonctl.JobStatus(host, port, rec.RemoteJobID)
	if err != nil {
		return rec, 0, false, err
	}
	if !jobEnded(st.Status) {
		return rec, 0, false, nil
	}

	outPath := filepath.Join(dir, jobhistory.CollectedOutput)
	n, err = fetchOutputFile(ctx, host, port, rec.RemoteJobID, outPath)
	if err != nil {
		return rec, n, false, err
	}
	res, execErr, err := replayToLogs(dir, outPath)
	if err != nil {
		return rec, n, false, err
	}
	if res.PeakBytes > 0 {
		rec.PeakMemBytes = res.PeakBytes
	}
	if len(res.Receipt) > 0 {
		if err := jobhistory.SaveReceipt(dir, res.Receipt); err != nil {
			note("      Warning: could not save the shim receipt in %s: %v\n", dir, err)
		}
	}
	finished := time.Now()
	if t, perr := time.Parse(time.RFC3339, st.FinishedAt); perr == nil {
		finished = t
	} else {
		note("      Warning: node gave no usable finish time for job %s (%q); using now\n", rec.RemoteJobID, st.FinishedAt)
	}
	if res.Timing != nil {
		node := host
		if rec.TargetID != "" {
			node = shortNode(rec.TargetID)
		}
		tm := res.Timing
		total := time.Duration(tm.ImportMs+tm.SetupMs+tm.RunMs) * time.Millisecond
		tl := &jobhistory.Timeline{Stages: rec.Stages}
		addExecStages(tl, node, finished.Add(-total), total, tm)
		rec.Stages = tl.Stages
	}
	rec.ExitCode = st.ExitCode

	var runErr error
	if execErr != nil {
		runErr = fmt.Errorf("execution failed: %w", execErr)
	} else {
		resultsDir := rec.LocalSyncRoot
		if resultsDir == "" {
			resultsDir = filepath.Dir(rec.ScriptPath)
			note("      Warning: job %s recorded no results directory; using %s\n", jobhistory.ShortID(rec.ID), resultsDir)
		}
		syncResults(host, port, rec.RemoteJobID, resultsDir, dir, &rec, note)
	}
	rec.Collected = true
	if err := jobhistory.FinalizeAt(dir, rec, runErr, finished); err != nil {
		return rec, n, false, fmt.Errorf("finalize job history %s: %w", dir, err)
	}
	rec, err = jobhistory.ReadRecordAt(dir)
	return rec, n, err == nil, err
}

// fetchOutputFile saves a job's whole output at path, replacing it only once
// the copy is complete.
func fetchOutputFile(ctx context.Context, host string, port int, jobID, path string) (int64, error) {
	tmp := path + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	n, err := daemonctl.FetchOutput(ctx, host, port, jobID, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		if rerr := os.Remove(tmp); rerr != nil && !os.IsNotExist(rerr) {
			fmt.Fprintf(os.Stderr, "warning: could not remove %s: %v\n", tmp, rerr)
		}
		return n, fmt.Errorf("fetch the output of job %s: %w", jobID, err)
	}
	return n, nil
}

// replayToLogs writes the saved output into stdout.log and stderr.log, as a
// foreground run saves them, and returns the job's result. err is a local
// file problem; execErr is the job's own outcome.
func replayToLogs(dir, outPath string) (res daemonctl.ExecResult, execErr, err error) {
	in, err := os.Open(outPath)
	if err != nil {
		return res, nil, err
	}
	defer in.Close()
	stdout, err := os.Create(filepath.Join(dir, "stdout.log"))
	if err != nil {
		return res, nil, err
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(dir, "stderr.log"))
	if err != nil {
		return res, nil, err
	}
	defer stderr.Close()
	return daemonctl.ReplayOutput(in, stdout, stderr)
}

// recordErr is a finalized record's outcome as an error: nil on success, and
// wrapping context.Canceled for a cancelled job, as the live run returned.
func recordErr(r jobhistory.Record) error {
	switch r.Status {
	case jobhistory.StateSucceeded:
		return nil
	case jobhistory.StateCancelled:
		return fmt.Errorf("%s: %w", r.Error, context.Canceled)
	}
	if r.Error == "" {
		return errors.New("job " + r.Status)
	}
	return errors.New(r.Error)
}
