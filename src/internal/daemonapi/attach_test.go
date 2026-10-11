package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// attachFixture is a daemon with one job record and the attach/status routes.
func attachFixture(t *testing.T, status string) (*Server, string, *httptest.Server) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("a", 8))
	t.Cleanup(s.StopWarmWorkers)
	id := "job-attach"
	if err := os.MkdirAll(filepath.Join(s.jobDir, id, "work"), 0o755); err != nil {
		t.Fatal(err)
	}
	s.jobsMu.Lock()
	if s.jobs == nil {
		s.jobs = map[string]*JobRecord{}
	}
	s.jobs[id] = &JobRecord{JobID: id, WorkDir: filepath.Join(s.jobDir, id, "work"), Status: status}
	s.jobsMu.Unlock()
	r := chi.NewRouter()
	r.Get("/v1/jobs/{id}/attach", s.handleJobAttach)
	r.Get("/v1/jobs/{id}/status", s.handleJobStatus)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return s, id, srv
}

func dialAttach(t *testing.T, srv *httptest.Server, id string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/jobs/" + id + "/attach"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readUntilDone reads frames until the done frame and checks the server then
// closes normally.
func readUntilDone(t *testing.T, conn *websocket.Conn) []OutputMessage {
	t.Helper()
	var got []OutputMessage
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		var m OutputMessage
		if err := conn.ReadJSON(&m); err != nil {
			t.Fatalf("read frame %d: %v", len(got), err)
		}
		got = append(got, m)
		if m.Done {
			break
		}
	}
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("after done frame: want normal close, got %v", err)
	}
	return got
}

func outputs(ms []OutputMessage) string {
	var b strings.Builder
	for _, m := range ms {
		b.WriteString(m.O)
	}
	return b.String()
}

func TestAttachReplaysFinishedJob(t *testing.T) {
	s, id, srv := attachFixture(t, "done")
	l := s.jobOutput(id)
	l.Append(OutputMessage{O: "a\rb\n"})
	l.Append(OutputMessage{E: "warn\n"})
	l.Finish(OutputMessage{ExitCode: 3})

	got := readUntilDone(t, dialAttach(t, srv, id))
	if len(got) != 3 || got[0].O != "a\rb\n" || got[1].E != "warn\n" || got[2].ExitCode != 3 {
		t.Fatalf("replay = %+v", got)
	}
	// The stored seq never reaches the wire.
	data, _ := json.Marshal(got[0])
	if strings.Contains(string(data), "seq") {
		t.Fatalf("seq leaked onto the wire: %s", data)
	}
}

func TestAttachRunningJobReplayThenLiveNoDuplicates(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	l := s.jobOutput(id)
	l.Append(OutputMessage{O: "1"})
	l.Append(OutputMessage{O: "2"})

	conn := dialAttach(t, srv, id)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for _, want := range []string{"1", "2"} {
		var m OutputMessage
		if err := conn.ReadJSON(&m); err != nil || m.O != want {
			t.Fatalf("replay: got %+v, %v; want %q", m, err, want)
		}
	}
	l.Append(OutputMessage{O: "3"})
	l.Finish(OutputMessage{ExitCode: 0})

	got := readUntilDone(t, conn)
	if outputs(got) != "3" || !got[len(got)-1].Done {
		t.Fatalf("live part = %+v", got)
	}
}

func TestAttachSlowClientCatchesUpWithoutGaps(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	l := s.jobOutput(id)
	conn := dialAttach(t, srv, id)
	time.Sleep(100 * time.Millisecond) // let the handler subscribe

	chunk := strings.Repeat("x", 1024)
	var want strings.Builder
	for i := 0; i < 3000; i++ {
		o := fmt.Sprintf("%d:%s\n", i, chunk)
		want.WriteString(o)
		l.Append(OutputMessage{O: o})
	}
	l.Finish(OutputMessage{})

	if got := outputs(readUntilDone(t, conn)); got != want.String() {
		t.Fatalf("slow attacher got %d bytes, want %d", len(got), want.Len())
	}
}

func TestAttachCancelFrameStopsTheJob(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	l := s.jobOutput(id)
	cancelled := make(chan struct{})
	l.SetCancel(func() { close(cancelled) })

	conn := dialAttach(t, srv, id)
	if err := conn.WriteJSON(map[string]bool{"cancel": true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel frame on attach did not reach the job")
	}
	l.Finish(OutputMessage{ExitCode: 143})
	readUntilDone(t, conn)
}

func TestAttachUnknownJobIs404(t *testing.T) {
	_, _, srv := attachFixture(t, "done")
	for _, path := range []string{"/v1/jobs/nope/attach", "/v1/jobs/nope/status"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]string
		json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound || body["error"] == "" {
			t.Fatalf("%s: status %d body %v", path, resp.StatusCode, body)
		}
	}
}

func TestJobStatusReportsExitAndDetach(t *testing.T) {
	s, id, srv := attachFixture(t, "importing")
	get := func() JobStatusResponse {
		t.Helper()
		resp, err := http.Get(srv.URL + "/v1/jobs/" + id + "/status")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var st JobStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	if st := get(); st.Status != "running" || st.FinishedAt != "" || st.Detached {
		t.Fatalf("running job status = %+v", st)
	}

	s.jobOutput(id).Finish(OutputMessage{ExitCode: 2})
	done := time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)
	s.jobsMu.Lock()
	s.jobs[id].Status = "failed"
	s.jobs[id].Detached = true
	s.jobs[id].FinishedAt = done
	s.jobsMu.Unlock()

	st := get()
	if st.Status != "failed" || st.ExitCode != 2 || !st.Detached || st.FinishedAt != "2026-10-12T09:30:00Z" {
		t.Fatalf("finished job status = %+v", st)
	}
}

func TestJobOutputReopenContinuesTheLog(t *testing.T) {
	s, id, _ := attachFixture(t, "done")
	l := s.jobOutput(id)
	l.Append(OutputMessage{O: "before restart"})
	l.Finish(OutputMessage{ExitCode: 1})
	jobLogs.Delete(l.path) // what a daemon restart forgets

	again := s.jobOutput(id)
	if again == l || again.seq != 2 || again.finalFrame() == nil || again.finalFrame().ExitCode != 1 {
		t.Fatalf("reopened log: seq=%d final=%+v", again.seq, again.finalFrame())
	}
}

func TestAttachJobEndedWithoutDoneFrameIsCutShort(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	s.jobOutput(id).Append(OutputMessage{O: "partial"})
	conn := dialAttach(t, srv, id)
	s.jobsMu.Lock()
	s.jobs[id].Status = "done" // the daemon restarted; nobody will call Finish
	s.jobsMu.Unlock()

	got := readUntilDone(t, conn)
	last := got[len(got)-1]
	if outputs(got) != "partial" || last.ExitCode != -1 || last.Error == "" {
		t.Fatalf("cut-short attach = %+v", got)
	}
}

func TestJobOutputLogIsCappedKeepingTheTail(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	old := jobOutputCap
	jobOutputCap = 8 << 10
	t.Cleanup(func() { jobOutputCap = old })

	l := s.jobOutput(id)
	for i := 0; i < 200; i++ {
		l.Append(OutputMessage{O: fmt.Sprintf("line %03d %s\n", i, strings.Repeat("y", 100))})
	}
	l.Finish(OutputMessage{ExitCode: 0})

	info, err := os.Stat(l.path)
	if err != nil || info.Size() > jobOutputCap {
		t.Fatalf("output log size %v (err %v), cap %d", info.Size(), err, jobOutputCap)
	}
	got := readUntilDone(t, dialAttach(t, srv, id))
	if !strings.Contains(got[0].E, "output trimmed") {
		t.Fatalf("first replayed frame is not the trim note: %+v", got[0])
	}
	if !strings.HasPrefix(got[len(got)-2].O, "line 199 ") || !got[len(got)-1].Done {
		t.Fatalf("tail lost: %+v", got[len(got)-2:])
	}
}

func TestDaemonStartFailsOrphanedJobs(t *testing.T) {
	s, id, srv := attachFixture(t, "running")
	s.jobOutput(id).Append(OutputMessage{O: "half done"})
	jobLogs.Delete(filepath.Join(s.jobDir, id, jobOutputFile)) // the old daemon is gone
	s.mu.Lock()
	s.leases["lease-orphan"] = &Lease{LeaseID: "lease-orphan", JobID: id}
	s.mu.Unlock()

	s.failOrphanedJobs()

	s.jobsMu.Lock()
	status, finished := s.jobs[id].Status, s.jobs[id].FinishedAt
	s.jobsMu.Unlock()
	if status != "failed" || finished.IsZero() {
		t.Fatalf("orphan status %q finished %v", status, finished)
	}
	s.mu.Lock()
	_, leaseLeft := s.leases["lease-orphan"]
	s.mu.Unlock()
	if leaseLeft {
		t.Fatal("orphaned job's lease was not released")
	}
	got := readUntilDone(t, dialAttach(t, srv, id))
	last := got[len(got)-1]
	if outputs(got) != "half done" || last.ExitCode != -1 || !strings.Contains(last.Error, "daemon restarted") {
		t.Fatalf("attach to orphan = %+v", got)
	}
}
