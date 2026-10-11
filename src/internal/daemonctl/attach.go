package daemonctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ErrDetached means the client stopped watching a job on purpose; the job
// keeps running on its node and can be reattached.
var ErrDetached = errors.New("detached")

// AttachStream replays a job's output so far, then follows it live until the
// done frame. Printing, the receipt filter and cancel-on-ctx are the same as
// StreamExecute.
func AttachStream(ctx context.Context, host string, port int, jobID string) (peak int64, stdout, stderr string, err error) {
	conn, err := dialJobSocket(ctx, host, port, "/v1/jobs/"+url.PathEscape(jobID)+"/attach")
	if err != nil {
		return 0, "", "", err
	}
	defer conn.Close()
	return streamJob(ctx, conn, nil)
}

// JobState is a job's state as its daemon reports it.
type JobState struct {
	Status     string `json:"status"` // uploaded | running | done | failed | cancelled
	ExitCode   int    `json:"exit_code"`
	Detached   bool   `json:"detached"`
	FinishedAt string `json:"finished_at"` // RFC3339, empty while running
}

// JobStatus asks a daemon for one job's state.
func JobStatus(host string, port int, jobID string) (JobState, error) {
	var st JobState
	u := fmt.Sprintf("http://%s:%d/v1/jobs/%s/status", host, port, url.PathEscape(jobID))
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		return st, fmt.Errorf("job status: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return st, fmt.Errorf("job status: %s (HTTP %d)", e.Error, resp.StatusCode)
		}
		return st, fmt.Errorf("job status: HTTP %d: %s", resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, fmt.Errorf("job status: unreadable answer: %w", err)
	}
	return st, nil
}
