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
