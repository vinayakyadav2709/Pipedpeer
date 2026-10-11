package daemonapi

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startExecJob registers an uploaded job whose closure's bin/run just runs
// the script with sh, and opens its exec websocket with the config sent.
func startExecJob(t *testing.T, script string) (*Server, string, *websocket.Conn) {
	t.Helper()
	s := NewWithConfig("test-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	s.jobDir = t.TempDir()
	dir := t.TempDir()
	store := filepath.Join(dir, "store")
	// Same layout as handleJobUpload: <jobDir>/<id>/work, so the output log
	// lands in <jobDir>/<id>/output.jsonl next to it.
	jobID := strings.ReplaceAll(t.Name(), "/", "-")
	work := filepath.Join(s.jobDir, jobID, "work")
	for _, d := range []string{filepath.Join(store, "bin"), work} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(store, "bin", "run"), []byte("#!/bin/sh\nexec sh \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "job.sh"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	s.jobsMu.Lock()
	s.jobs[jobID] = &JobRecord{JobID: jobID, WorkDir: work, StorePath: store, ScriptPath: "job.sh", Status: "uploaded"}
	s.jobsMu.Unlock()

	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/jobs/"+jobID+"/exec", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.WriteJSON(ExecConfig{ScriptPath: "job.sh", StorePath: store}); err != nil {
		t.Fatal(err)
	}
	return s, jobID, conn
}

// readUntilDone collects stdout and returns it with the done frame.
func readExecUntilDone(t *testing.T, conn *websocket.Conn, within time.Duration) (string, OutputMessage) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(within))
	var out strings.Builder
	for {
		var m OutputMessage
		if err := conn.ReadJSON(&m); err != nil {
			t.Fatalf("reading output: %v (so far %q)", err, out.String())
		}
		if m.Error != "" {
			t.Fatalf("job error: %s", m.Error)
		}
		out.WriteString(m.O)
		if m.Done {
			return out.String(), m
		}
	}
}

func execJobState(s *Server, jobID string) JobRecord {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	return *s.jobs[jobID]
}

func TestExecForwardsCarriageReturnsUnchanged(t *testing.T) {
	_, _, conn := startExecJob(t, `printf 'a\rb\rc\n'`)
	out, done := readExecUntilDone(t, conn, 10*time.Second)
	if out != "a\rb\rc\n" {
		t.Fatalf("stdout = %q, want %q", out, "a\rb\rc\n")
	}
	if done.ExitCode != 0 || done.Cancelled {
		t.Fatalf("done = %+v", done)
	}
}

func TestExecCancelFrameStopsJob(t *testing.T) {
	s, jobID, conn := startExecJob(t, "echo started\nsleep 60\n")
	deadline := time.Now().Add(5 * time.Second)
	for execJobState(s, jobID).Status != "running" {
		if time.Now().After(deadline) {
			t.Fatal("job never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let sh start sleep
	sent := time.Now()
	if err := conn.WriteJSON(map[string]bool{"cancel": true}); err != nil {
		t.Fatal(err)
	}
	_, done := readExecUntilDone(t, conn, 5*time.Second)
	if took := time.Since(sent); took > 3*time.Second {
		t.Fatalf("cancel took %s", took)
	}
	if !done.Cancelled {
		t.Fatalf("done frame not marked cancelled: %+v", done)
	}
	deadline = time.Now().Add(2 * time.Second)
	for execJobState(s, jobID).Status != "cancelled" {
		if time.Now().After(deadline) {
			t.Fatalf("status = %q, want cancelled", execJobState(s, jobID).Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExecDetachLeavesJobRunning(t *testing.T) {
	s, jobID, conn := startExecJob(t, "sleep 0.5\necho finished-line\n")
	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4001, "detach")); err != nil {
		t.Fatal(err)
	}
	conn.Close()

	deadline := time.Now().Add(10 * time.Second)
	for execJobState(s, jobID).Status != "done" {
		if time.Now().After(deadline) {
			t.Fatalf("status = %q, want done", execJobState(s, jobID).Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := execJobState(s, jobID)
	if !st.Detached || st.Cancelled || st.FinishedAt.IsZero() {
		t.Fatalf("job = %+v, want detached, not cancelled, finished", st)
	}

	f, err := os.Open(filepath.Join(s.jobDir, jobID, jobOutputFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out strings.Builder
	var sawDone bool
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m storedOutput
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		out.WriteString(m.O)
		sawDone = sawDone || m.Done
	}
	if out.String() != "finished-line\n" || !sawDone {
		t.Fatalf("output.jsonl stdout = %q, done = %v", out.String(), sawDone)
	}
}

func TestCompleteUTF8(t *testing.T) {
	e := []byte("é")    // 2 bytes
	euro := []byte("€") // 3 bytes
	cases := []struct {
		in   []byte
		want int
	}{
		{[]byte("abc"), 3},
		{append([]byte("a"), e[0]), 1},
		{append([]byte("a"), e...), 3},
		{append([]byte("ab"), euro[:2]...), 2},
		{[]byte{'a', 0xff}, 2},
		{nil, 0},
	}
	for _, c := range cases {
		if got := completeUTF8(c.in); got != c.want {
			t.Errorf("completeUTF8(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
