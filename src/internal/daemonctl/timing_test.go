package daemonctl

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

	up, err := UploadJob(host, port, tarPath, "", store, "job.py", true)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	res, err := StreamExecute(context.Background(), host, port, up.JobID,
		ExecConfig{ScriptPath: "job.py", StorePath: store, Isolate: false})
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
