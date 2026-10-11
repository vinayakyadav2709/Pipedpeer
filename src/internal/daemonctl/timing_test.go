package daemonctl

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
)

// A run's Done frame must say how the node spent it - setup and the script
// itself - or the submitter's timeline cannot tell a slow script from a slow
// sandbox. Driven end to end: upload, execute, read the result.
func TestExecutionReportsTiming(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	store := t.TempDir()
	run := filepath.Join(store, "bin", "run")
	if err := os.MkdirAll(filepath.Dir(run), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run, []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755); err != nil {
		t.Fatal(err)
	}

	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "job.py"),
		[]byte("import time\ntime.sleep(0.3)\nprint('hello')\n"), 0644); err != nil {
		t.Fatal(err)
	}
	tarPath := filepath.Join(t.TempDir(), "ws.tar")
	if err := CreateWorkspaceTar(project, tarPath, ""); err != nil {
		t.Fatal(err)
	}

	s := daemonapi.New("node-timing")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	up, err := UploadJob(host, port, tarPath, "", store, "job.py", UploadOptions{SkipBroadcast: true})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	res, err := StreamExecute(context.Background(), host, port, up.JobID,
		ExecConfig{ScriptPath: "job.py", StorePath: store, Isolate: false}, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Stdout != "hello\n" {
		t.Errorf("stdout = %q", res.Stdout)
	}
	if res.Timing == nil {
		t.Fatal("the Done frame carried no timing")
	}
	if res.Timing.RunMs < 300 {
		t.Errorf("run_ms = %d for a script that sleeps 300ms", res.Timing.RunMs)
	}
	if res.Timing.SetupMs < 0 || res.Timing.ImportMs != 0 {
		t.Errorf("timing = %+v; want non-negative setup and no import for a closure already present", *res.Timing)
	}
}

// An intercepted run's receipt comes back with the execution, and stays out
// of the files synced into the user's project.
func TestReceiptReturnsWithTheRunNotTheResults(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	store := t.TempDir()
	run := filepath.Join(store, "bin", "run")
	_ = os.MkdirAll(filepath.Dir(run), 0755)
	if err := os.WriteFile(run, []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	// Stands in for the shim: writes the receipt where the daemon says to.
	job := `import os, json
with open(os.environ["PIPEDPEER_RECEIPT"], "w") as f:
    json.dump({"remote_items": 6, "local_items": 2, "decisions": [{"op": "pool.map", "shipped": True, "why": "w"}]}, f)
open("out.txt", "w").write("result")
`
	if err := os.WriteFile(filepath.Join(project, "job.py"), []byte(job), 0644); err != nil {
		t.Fatal(err)
	}
	tarPath := filepath.Join(t.TempDir(), "ws.tar")
	if err := CreateWorkspaceTar(project, tarPath, "# shim"); err != nil {
		t.Fatal(err)
	}

	s := daemonapi.New("node-receipt")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	up, err := UploadJob(host, port, tarPath, "", store, "job.py", UploadOptions{SkipBroadcast: true})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	res, err := StreamExecute(context.Background(), host, port, up.JobID,
		ExecConfig{ScriptPath: "job.py", StorePath: store, Intercept: true}, nil)
	if err != nil {
		t.Fatalf("execute: %v (stderr %q)", err, res.Stderr)
	}
	var rc struct {
		RemoteItems int `json:"remote_items"`
	}
	if err := json.Unmarshal(res.Receipt, &rc); err != nil || rc.RemoteItems != 6 {
		t.Fatalf("receipt %q did not come back with the run (%v)", res.Receipt, err)
	}

	out := t.TempDir()
	m, err := DownloadResults(host, port, up.JobID, out)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range append(m.New, m.Updated...) {
		if strings.HasPrefix(f, ".pipedpeer/") {
			t.Errorf("pipedpeer's own file %s was synced into the project", f)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(out, "out.txt")); string(b) != "result" {
		t.Errorf("the job's real output did not come back")
	}
}
