package app

import (
	"archive/tar"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/rs/zerolog"
)

// fakeNode is a daemon holding one detached job, reporting status, its
// recorded output over the attach socket, and a results tar with one file.
func fakeNode(t *testing.T, jobID string, status *atomic.Value) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs/"+jobID+"/status", func(w http.ResponseWriter, r *http.Request) {
		st := status.Load().(string)
		fin := ""
		if st != "running" {
			fin = "2026-10-12T10:00:00Z"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"job_id": jobID, "status": st, "exit_code": 0, "detached": true, "finished_at": fin})
	})
	mux.HandleFunc("/v1/jobs/"+jobID+"/attach", func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for _, f := range []string{`{"o":"tick 0\n","exit_code":0}`, `{"e":"warn\n","exit_code":0}`,
			`{"done":true,"exit_code":0,"peak_mem_bytes":1234,"timing":{"import_ms":1,"setup_ms":2,"run_ms":30}}`} {
			if err := c.WriteMessage(websocket.TextMessage, []byte(f)); err != nil {
				t.Errorf("write frame: %v", err)
				return
			}
		}
		_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"))
	})
	mux.HandleFunc("/v1/jobs/"+jobID+"/results", func(w http.ResponseWriter, r *http.Request) {
		tw := tar.NewWriter(w)
		body := []byte("result\n")
		if err := tw.WriteHeader(&tar.Header{Name: "out/result.txt", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Errorf("tar header: %v", err)
		}
		_, _ = tw.Write(body)
		_ = tw.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// detachedRecord writes a history record for a detached job on endpoint and
// returns its dir and the project folder results go to.
func detachedRecord(t *testing.T, jobID, endpoint string) (string, string) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	project := t.TempDir()
	rec, dir, err := jobhistory.NewRecord(filepath.Join(project, "main.py"), "", "", true, false)
	if err != nil {
		t.Fatal(err)
	}
	rec.RemoteJobID, rec.DaemonEndpoint, rec.LocalSyncRoot = jobID, endpoint, project
	if err := jobhistory.SaveRecord(dir, rec); err != nil {
		t.Fatal(err)
	}
	return dir, project
}

func TestWatcherCollectsFinishedDetachedJob(t *testing.T) {
	var status atomic.Value
	status.Store("running")
	srv := fakeNode(t, "j1", &status)
	dir, project := detachedRecord(t, "j1", strings.TrimPrefix(srv.URL, "http://"))
	log := zerolog.Nop()
	warned := map[string]bool{}

	collectDetached(context.Background(), log, warned)
	if r, _ := jobhistory.ReadRecordAt(dir); r.Collected || r.Status != jobhistory.StateRunning {
		t.Fatalf("running job collected early: %+v", r)
	}

	status.Store("done")
	collectDetached(context.Background(), log, warned)
	r, err := jobhistory.ReadRecordAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Collected || r.Status != jobhistory.StateSucceeded || r.PeakMemBytes != 1234 || r.ReceivedFiles != 1 {
		t.Fatalf("record after collection: %+v", r)
	}
	if b, err := os.ReadFile(filepath.Join(project, "out", "result.txt")); err != nil || string(b) != "result\n" {
		t.Fatalf("results not in the project folder: %q %v", b, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "stdout.log")); string(b) != "tick 0\n" {
		t.Fatalf("stdout.log = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, jobhistory.CollectedOutput)); strings.Count(string(b), "\n") != 3 {
		t.Fatalf("output.jsonl = %q", b)
	}

	// Idempotent: a second pass and attach never sync again.
	if err := os.Remove(filepath.Join(project, "out", "result.txt")); err != nil {
		t.Fatal(err)
	}
	collectDetached(context.Background(), log, warned)

	// Attach replays from disk with the node gone.
	srv.Close()
	if err := Attach(context.Background(), r.ID); err != nil {
		t.Fatalf("attach of a collected job: %v", err)
	}
	if _, err := os.Stat(filepath.Join(project, "out", "result.txt")); !os.IsNotExist(err) {
		t.Fatalf("results applied a second time (stat err %v)", err)
	}
}

func TestWatcherRetriesUnreachableNode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	dir, _ := detachedRecord(t, "j2", addr)
	warned := map[string]bool{}
	for i := 0; i < 2; i++ {
		collectDetached(context.Background(), zerolog.Nop(), warned)
	}
	r, err := jobhistory.ReadRecordAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.Collected || r.Status != jobhistory.StateRunning {
		t.Fatalf("unreachable job marked: %+v", r)
	}
	if !warned[r.ID] {
		t.Fatal("no warning recorded for the unreachable node")
	}
}
