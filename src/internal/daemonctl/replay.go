package daemonctl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"
)

// ErrConnectionLost: the connection to a running job's daemon broke without
// a done frame, cancel or detach. The node keeps the job running.
var ErrConnectionLost = errors.New("connection lost")

// FetchOutput copies a job's whole recorded output from its node into w, one
// frame per line exactly as the daemon sent it, up to and including the done
// frame. It never sends a cancel frame. Returns the bytes written.
func FetchOutput(ctx context.Context, host string, port int, jobID string, w io.Writer) (int64, error) {
	conn, err := dialJobSocket(ctx, host, port, "/v1/jobs/"+url.PathEscape(jobID)+"/attach")
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()
	var n int64
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return n, fmt.Errorf("output of job %s ended before its done frame: %w", jobID, err)
		}
		var f struct {
			Done bool `json:"done"`
		}
		if err := json.Unmarshal(msg, &f); err != nil {
			return n, fmt.Errorf("unreadable frame in the output of job %s: %w", jobID, err)
		}
		k, err := w.Write(append(msg, '\n'))
		n += int64(k)
		if err != nil {
			return n, err
		}
		if f.Done {
			return n, nil
		}
	}
}

// ReplayOutput writes output saved by FetchOutput to stdout and stderr the
// way a live attach prints it. jobErr is the job's own outcome, as the live
// stream returned it; err is a problem with the saved file itself. The
// result's Stdout and Stderr stay empty: nothing is buffered.
func ReplayOutput(r io.Reader, stdout, stderr io.Writer) (res ExecResult, jobErr, err error) {
	br := bufio.NewReader(r)
	for {
		line, readErr := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var out outputMessage
			if err := json.Unmarshal(line, &out); err != nil {
				fmt.Fprintf(os.Stderr, "warning: skipping an unreadable saved frame: %v\n", err)
			} else if done, jobErr := applyFrame(out, &res, stdout, stderr, false); done {
				return res, jobErr, nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return res, nil, errors.New("the saved output ends without the job's result")
		}
		if readErr != nil {
			return res, nil, fmt.Errorf("read saved output: %w", readErr)
		}
	}
}

// applyFrame prints one output frame and, for an error or done frame,
// reports done and the job's outcome. cancelSent: this client asked the
// daemon to stop the job.
func applyFrame(out outputMessage, res *ExecResult, stdout, stderr io.Writer, cancelSent bool) (bool, error) {
	if out.Error != "" {
		return true, fmt.Errorf("remote: %s", out.Error)
	}
	// Raw chunks, printed as they come: \r redraws stay in place.
	if _, err := io.WriteString(stdout, out.O); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not write the job's output: %v\n", err)
	}
	if _, err := io.WriteString(stderr, out.E); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not write the job's error output: %v\n", err)
	}
	if !out.Done {
		return false, nil
	}
	if out.PeakMemBytes > 0 {
		res.PeakBytes = out.PeakMemBytes
	}
	res.Timing, res.Receipt = out.Timing, out.Receipt
	// A job that finished cleanly just before the cancel landed did
	// finish: report it as such.
	if out.Cancelled || (cancelSent && out.ExitCode != 0) {
		return true, fmt.Errorf("job cancelled (exit code %d): %w", out.ExitCode, context.Canceled)
	}
	if out.OOMKilled {
		// Without this the user sees "exited with code 137" and has
		// no way to know the node protected itself on purpose.
		return true, fmt.Errorf(
			"job ran out of memory: the kernel killed it for exceeding its %s limit. "+
				"Raise it with --mem, or reduce what the script holds in memory at once",
			humanBytes(out.MemLimitBytes))
	}
	if out.ExitCode != 0 {
		return true, &RemoteExitError{Code: out.ExitCode}
	}
	return true, nil
}
