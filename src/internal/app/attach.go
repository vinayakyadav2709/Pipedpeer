package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

// Attach shows a job this machine started and then detached. Detaching only
// stops the live view: the job's end (results into the project folder, the
// finalized record) happens anyway, through collect, whether the daemon's
// watcher or this attach sees it first. So attach replays the output (from
// this machine once collected, else live from the node, then collects) and
// prints the summary; it never applies results twice.
// id is a history id, a prefix of one, or the short id detach printed.
// Cancelling ctx while following live stops the job on the node.
func Attach(ctx context.Context, id string) error {
	rec, dir, err := jobhistory.Resolve(id)
	if err != nil {
		return err
	}
	short := jobhistory.ShortID(rec.ID)
	if rec.RemoteJobID == "" || rec.DaemonEndpoint == "" {
		return fmt.Errorf("job %s was not started in the background, so there is nothing to attach to; its output: pipedpeer job --id %s --output", short, rec.ID)
	}
	host, port, err := splitEndpoint(rec.DaemonEndpoint)
	if err != nil {
		return fmt.Errorf("job %s: %w", short, err)
	}

	if rec.Collected {
		fmt.Printf("job %s (%s) finished on %s; its output, saved on this machine:\n\n", short, filepath.Base(rec.ScriptPath), rec.DaemonEndpoint)
		if err := replaySaved(dir); err != nil {
			fmt.Fprintf(os.Stderr, "warning: job %s: %v\n", short, err)
		}
	} else {
		fmt.Printf("attaching to job %s (%s) on %s\n\n", short, filepath.Base(rec.ScriptPath), rec.DaemonEndpoint)
		_, streamErr := daemonctl.AttachStream(ctx, host, port, rec.RemoteJobID)
		// The job's end is collected even after Ctrl+C: the cancel is the
		// job's outcome, and its record must say so.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), collectTimeout)
		note := func(format string, args ...any) { fmt.Printf(format, args...) }
		got, _, done, err := collect(cctx, dir, note)
		cancel()
		switch {
		case err != nil && streamErr != nil:
			return fmt.Errorf("job %s: %w (and collecting its end failed: %v; this machine's daemon retries)", short, streamErr, err)
		case err != nil:
			return fmt.Errorf("job %s ended, but collecting its results failed: %w; this machine's daemon retries every %s", short, err, collectEvery)
		case !done:
			if streamErr == nil {
				streamErr = fmt.Errorf("the output ended but the node says the job still runs")
			}
			return fmt.Errorf("job %s: %w; it keeps running, and this machine's daemon collects it when it ends", short, streamErr)
		}
		rec = got
	}

	printSummary(os.Stdout, recordSummary(dir, rec, host, port))
	return recordErr(rec)
}

// replaySaved prints a collected job's saved output as it was printed live.
func replaySaved(dir string) error {
	f, err := os.Open(filepath.Join(dir, jobhistory.CollectedOutput))
	if err != nil {
		return fmt.Errorf("saved output unreadable: %w", err)
	}
	defer f.Close()
	// The job's own outcome is in the record and the summary.
	_, _, err = daemonctl.ReplayOutput(f, os.Stdout, os.Stderr)
	return err
}

// recordSummary is the run summary of a finalized record.
func recordSummary(dir string, rec jobhistory.Record, host string, port int) summary {
	sum := summary{Host: host, Port: port, ScriptRan: true, Peak: rec.PeakMemBytes,
		Total: time.Duration(rec.DurationMs) * time.Millisecond, Err: recordErr(rec)}
	if r, ok, err := jobhistory.ReadReceipt(dir); err != nil {
		fmt.Fprintf(os.Stderr, "warning: the saved shim receipt in %s is unreadable: %v\n", dir, err)
	} else if ok {
		if raw, err := json.Marshal(r); err == nil {
			sum.Receipt = raw
		} else {
			fmt.Fprintf(os.Stderr, "warning: could not re-encode the shim receipt in %s: %v\n", dir, err)
		}
	}
	if rec.ManifestPath != "" {
		var m daemonctl.ResultManifest
		b, err := os.ReadFile(rec.ManifestPath)
		if err == nil {
			err = json.Unmarshal(b, &m)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: results list %s unreadable: %v\n", rec.ManifestPath, err)
		} else {
			sum.Files = &m
		}
	}
	return sum
}
