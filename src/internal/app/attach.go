package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

// Attach reconnects to a job this machine started and then detached: it
// replays the job's output so far, follows it live, brings the results back
// into the directory the run would have used, and prints the summary.
// id is a history id, a prefix of one, or the short id detach printed.
// Cancelling ctx stops the job on the node.
func Attach(ctx context.Context, id string) (runErr error) {
	rec, dir, err := jobhistory.Resolve(id)
	if err != nil {
		return err
	}
	short := jobhistory.ShortID(rec.ID)
	if rec.RemoteJobID == "" || rec.DaemonEndpoint == "" {
		return fmt.Errorf("job %s was not started in the background, so there is nothing to attach to; its output: pipedpeer job --id %s --output", short, rec.ID)
	}
	host, portStr, err := net.SplitHostPort(rec.DaemonEndpoint)
	if err != nil {
		return fmt.Errorf("job %s has an unreadable daemon address %q: %v", short, rec.DaemonEndpoint, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("job %s has an unreadable daemon port %q: %v", short, rec.DaemonEndpoint, err)
	}
	resultsDir := rec.LocalSyncRoot
	if resultsDir == "" {
		resultsDir = filepath.Dir(rec.ScriptPath)
		fmt.Fprintf(os.Stderr, "warning: job %s recorded no results directory; using %s\n", short, resultsDir)
	}

	fmt.Printf("attaching to job %s (%s) on %s\n\n", short, filepath.Base(rec.ScriptPath), rec.DaemonEndpoint)

	sum := summary{Host: host, Port: port, ScriptRan: true}
	defer func() {
		if err := jobhistory.Finalize(dir, rec, runErr); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not finalize job history %s: %v\n", dir, err)
		}
		// The whole job, not just the attached part.
		if t, err := time.Parse(time.RFC3339Nano, rec.StartedAt); err == nil {
			sum.Total = time.Since(t)
		}
		sum.Err = runErr
		printSummary(os.Stdout, sum)
	}()

	res, err := daemonctl.AttachStream(ctx, host, port, rec.RemoteJobID)
	sum.Peak, sum.Receipt = res.PeakBytes, res.Receipt
	if res.PeakBytes > 0 {
		rec.PeakMemBytes = res.PeakBytes
	}
	_ = jobhistory.SaveText(dir, "stdout.log", res.Stdout)
	_ = jobhistory.SaveText(dir, "stderr.log", res.Stderr)
	if len(res.Receipt) > 0 {
		if err := jobhistory.SaveReceipt(dir, res.Receipt); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not save the shim receipt in %s: %v\n", dir, err)
		}
	}
	if err != nil {
		return fmt.Errorf("job %s: %w", short, err)
	}

	sum.Files = syncResults(host, port, rec.RemoteJobID, resultsDir, dir, &rec,
		func(format string, args ...any) { fmt.Printf(format, args...) })
	return nil
}
