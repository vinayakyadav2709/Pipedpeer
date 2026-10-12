package daemonapi

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// lostDoneGrace is how long attach waits, once the job record says the job
// ended, for its done frame to reach the output log before it reports the
// output as cut short (the daemon restarted while the job ran).
const lostDoneGrace = 2 * time.Second

// JobStatusResponse is GET /v1/jobs/{id}/status.
type JobStatusResponse struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	ExitCode   int    `json:"exit_code"`
	Detached   bool   `json:"detached"`
	FinishedAt string `json:"finished_at"`
}

func jobEnded(status string) bool {
	return status == "done" || status == "failed" || status == "cancelled"
}

// jobStatus reads the job's status under the lock; ok is false for an
// unknown id.
func (s *Server) jobStatus(jobID string) (status string, detached bool, finishedAt time.Time, ok bool) {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return "", false, time.Time{}, false
	}
	return job.Status, job.Detached, job.FinishedAt, true
}

// handleJobStatus handles GET /v1/jobs/{id}/status.
func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	status, detached, finishedAt, ok := s.jobStatus(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if status == "importing" {
		status = "running"
	}
	resp := JobStatusResponse{JobID: jobID, Status: status, Detached: detached}
	if !finishedAt.IsZero() {
		resp.FinishedAt = finishedAt.UTC().Format(time.RFC3339)
	}
	if final := s.jobFinalFrame(jobID); final != nil {
		resp.ExitCode = final.ExitCode
	}
	writeJSON(w, http.StatusOK, resp)
}

// jobFinalFrame is the job's recorded done frame, nil if it has none yet. It
// does not open a log for a job that never ran.
func (s *Server) jobFinalFrame(jobID string) *OutputMessage {
	path := filepath.Join(s.jobDir, jobID, jobOutputFile)
	if l, ok := jobLogs.Load(path); ok {
		return l.(*jobOutputLog).finalFrame()
	}
	recs := readStoredOutput(path, 0)
	if n := len(recs); n > 0 && recs[n-1].Done {
		final := recs[n-1].OutputMessage
		return &final
	}
	return nil
}

// handleJobAttach handles WebSocket GET /v1/jobs/{id}/attach: every frame
// recorded so far, then live ones, ending with the done frame. A
// {"cancel":true} frame from the client stops the job, as on exec.
func (s *Server) handleJobAttach(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	if _, _, _, ok := s.jobStatus(jobID); !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warn().Err(err).Str("job", jobID).Msg("attach: websocket upgrade failed")
		return
	}
	defer conn.Close()
	l := s.jobOutput(jobID)

	// The reader notices the client leaving and handles cancel frames.
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f struct {
				Cancel bool `json:"cancel"`
			}
			if json.Unmarshal(msg, &f) != nil || !f.Cancel {
				continue
			}
			if l.cancel() {
				log.Info().Str("job", jobID).Msg("attach: client cancelled the job")
			} else {
				log.Warn().Str("job", jobID).Msg("attach: cancel asked, but the job has no running process here")
			}
		}
	}()

	send := func(m OutputMessage) bool {
		data, _ := json.Marshal(m)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Warn().Err(err).Str("job", jobID).Msg("attach: client went away")
			return false
		}
		return true
	}
	finish := func() {
		msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done")
		if err := conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second)); err != nil {
			log.Warn().Err(err).Str("job", jobID).Msg("attach: closing the connection")
		}
	}

	last := 0
	tick := time.NewTicker(lostDoneGrace)
	defer tick.Stop()
	var endedSince time.Time
	lost := false
	for {
		past, ch := l.follow(last)
		for _, m := range past {
			if !send(m.OutputMessage) {
				if ch != nil {
					l.unsubscribe(ch)
				}
				return
			}
			last = m.Seq
			if m.Done {
				if ch != nil {
					l.unsubscribe(ch)
				}
				finish()
				return
			}
		}
		if ch == nil {
			break // finished, yet the file holds no done frame
		}
	live:
		for {
			select {
			case m, open := <-ch:
				if !open {
					break live // finished or fell behind: catch up from the file
				}
				if m.Seq <= last {
					continue
				}
				if !send(m.OutputMessage) {
					l.unsubscribe(ch)
					return
				}
				last = m.Seq
				if m.Done {
					l.unsubscribe(ch)
					finish()
					return
				}
			case <-gone:
				l.unsubscribe(ch)
				return
			case <-tick.C:
				// A job that ended without its done frame being recorded
				// (daemon restarted mid-job) would otherwise hang the client.
				status, _, _, _ := s.jobStatus(jobID)
				if !jobEnded(status) {
					continue
				}
				if endedSince.IsZero() {
					endedSince = time.Now()
					continue
				}
				l.unsubscribe(ch)
				lost = true
				break live
			}
		}
		if lost {
			break
		}
	}
	log.Warn().Str("job", jobID).Msg("attach: job output has no done frame; reporting it as cut short")
	if send(OutputMessage{Done: true, ExitCode: -1, Error: "job output ended without a result (the daemon restarted while the job ran?)"}) {
		finish()
	}
}
