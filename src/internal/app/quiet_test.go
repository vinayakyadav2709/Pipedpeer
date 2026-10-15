package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

// captureStdout runs f with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		var b bytes.Buffer
		if _, err := io.Copy(&b, r); err != nil {
			t.Errorf("reading captured stdout: %v", err)
		}
		done <- b.String()
	}()
	f()
	os.Stdout = saved
	w.Close()
	return <-done
}

// A plain run prints nothing of pipedpeer's own on stdout: stdout is the
// script's. Driven up to a failed upload, the part of RunTask that printed
// headings and notes before -v existed.
func TestRunTaskQuietWithoutVerbose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	dir := t.TempDir()
	ws := filepath.Join(dir, "ws.tar")
	script := filepath.Join(dir, "job.py")
	for _, f := range []string{ws, script} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	env := &Environment{WorkspaceTar: ws, StorePath: "/nix/store/x-run", ScriptRel: "job.py"}
	say.Verbose = false
	var err error
	out := captureStdout(t, func() {
		err = RunTask(context.Background(), env, Task{Script: script,
			Options: Options{DaemonHost: u.Hostname(), DaemonPort: port}})
	})
	if err == nil {
		t.Fatal("upload to a failing daemon succeeded")
	}
	if out != "" {
		t.Fatalf("quiet run wrote to stdout:\n%s", out)
	}
}

func TestResultsErrorUnwraps(t *testing.T) {
	inner := errors.New("connection reset")
	var err error = &ResultsError{Err: inner}
	var re *ResultsError
	if !errors.Is(err, inner) || !errors.As(err, &re) {
		t.Fatal("ResultsError must unwrap to its cause and be found by errors.As")
	}
}

// A DDP rank (not a single run) saves its record too, with its sync rounds
// from the receipt, the script step and the rank node's hostname, and prints
// nothing: the DDP caller owns the terminal.
func TestDDPRankSavesRunRecord(t *testing.T) {
	home := t.TempDir()
	for _, v := range []string{"XDG_STATE_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME"} {
		t.Setenv(v, filepath.Join(home, v))
	}
	t.Setenv("HOME", home)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"node_id":"rank1node","capabilities":{"hostname":"laptop2"}}]`))
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	dir := t.TempDir()
	receipt := []byte(`{"steps":[{"machine":"rank1node","node":"rank1node","chunk":-1,"step":"ddp_sync","start_ns":1,"dur_ns":5000000}]}`)
	opts := Options{DaemonHost: u.Hostname(), DaemonPort: port, TargetID: "rank1node"}
	start := time.Now()
	out := captureStdout(t, func() {
		saveRunRecord(dir, "run-1", opts, start, start, 2*time.Second, receipt, false)
	})
	if out != "" {
		t.Fatalf("a rank's record printed to stdout:\n%s", out)
	}
	rec, err := runrec.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sync, script bool
	for _, s := range rec.Steps {
		sync = sync || s.Name == "ddp_sync"
		script = script || (s.Name == "script" && s.Machine == "rank1node")
	}
	if !sync || !script || rec.ScriptNS != (2*time.Second).Nanoseconds() {
		t.Fatalf("record = %+v, want the ddp_sync round and a 2 s script step on rank1node", rec)
	}
	if rec.Names["rank1node"] != "laptop2" {
		t.Fatalf("names = %v, want rank1node -> laptop2", rec.Names)
	}
}
