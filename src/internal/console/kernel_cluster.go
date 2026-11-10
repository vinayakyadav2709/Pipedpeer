package console

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

const (
	// After an interrupt, how long `pipedpeer run` gets to stop on its own
	// before it is killed.
	clusterStopGrace = 5 * time.Second
	clusterReadChunk = 32 << 10
	clusterScript    = "notebook_cell.py"
)

// runCluster submits a cell as a Pipedpeer job by running this binary's own
// `run` command on a script holding the cell's code, and streams the job's
// output back. The cell shares nothing with the notebook's kernel.
func (s *session) runCluster(ctx context.Context, e *execution) {
	status, failure := s.runClusterJob(ctx, e)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running != e {
		return
	}
	if failure != nil {
		s.output(*failure)
	}
	s.finishLocked(status)
}

func (s *session) runClusterJob(ctx context.Context, e *execution) (string, *errorFrame) {
	fail := func(ename, message string) (string, *errorFrame) {
		return statusError, &errorFrame{Type: "error", CellID: e.cellID, Ename: ename,
			Evalue: message, Traceback: []string{message}}
	}
	cfg := s.mgr.cfg
	if cfg.Executable == "" {
		return fail("ClusterUnavailable", "cluster execution is unavailable: the pipedpeer binary could not be located")
	}

	// A directory of its own: `run` packs the script's whole project
	// directory, and the cell's script must be the only thing in it.
	dir, err := os.MkdirTemp("", "pipedpeer-cell-")
	if err != nil {
		return fail("ClusterError", err.Error())
	}
	defer os.RemoveAll(dir)
	script := filepath.Join(dir, clusterScript)
	if err := os.WriteFile(script, []byte(e.code), 0o600); err != nil {
		return fail("ClusterError", err.Error())
	}

	cmd := termuxexec.Command(cfg.Executable, "run", script)
	cmd.Dir = dir
	cmd.Env = cfg.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fail("ClusterError", err.Error())
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fail("ClusterError", err.Error())
	}
	if err := cmd.Start(); err != nil {
		return fail("ClusterError", "could not start pipedpeer run: "+err.Error())
	}

	var copies sync.WaitGroup
	for name, r := range map[string]io.Reader{"stdout": stdout, "stderr": stderr} {
		copies.Add(1)
		go func() {
			defer copies.Done()
			s.streamCluster(e, name, r)
		}()
	}

	exited := make(chan struct{})
	go func() {
		select {
		case <-exited:
			return
		case <-ctx.Done():
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
		select {
		case <-exited:
		case <-time.After(clusterStopGrace):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()

	copies.Wait()
	waitErr := cmd.Wait()
	close(exited)

	if ctx.Err() != nil {
		return fail("KeyboardInterrupt", "cluster job interrupted")
	}
	var exitErr *exec.ExitError
	switch {
	case waitErr == nil:
		return statusOK, nil
	case errors.As(waitErr, &exitErr):
		return fail("ClusterJobFailed", "pipedpeer run ended: "+exitErr.ProcessState.String())
	default:
		return fail("ClusterError", waitErr.Error())
	}
}

func (s *session) streamCluster(e *execution, name string, r io.Reader) {
	buf := make([]byte, clusterReadChunk)
	var carry []byte
	for {
		n, err := r.Read(buf)
		if n > 0 {
			text, rest := splitValidUTF8(append(carry, buf[:n]...))
			carry = rest
			if text != "" {
				s.mu.Lock()
				if s.running == e {
					s.output(streamFrame{Type: "stream", CellID: e.cellID, Name: name, Text: text})
				}
				s.mu.Unlock()
			}
		}
		if err != nil {
			return
		}
	}
}
