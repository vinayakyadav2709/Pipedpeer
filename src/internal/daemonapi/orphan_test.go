package daemonapi

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// startGroup starts a sleeping process leading its own group, like an
// unisolated job.
func startGroup(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
	})
	return cmd
}

func waitExit(cmd *exec.Cmd, within time.Duration) bool {
	done := make(chan struct{})
	go func() { cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(within):
		return false
	}
}

func TestOrphanedUnisolatedJobIsStopped(t *testing.T) {
	s, id, _ := attachFixture(t, "running")
	cmd := startGroup(t)
	s.recordJobGroup(s.jobs[id], cmd.Process.Pid)
	jobLogs.Delete(filepath.Join(s.jobDir, id, jobOutputFile)) // the old daemon is gone

	s.failOrphanedJobs()

	if !waitExit(cmd, 20*time.Second) {
		t.Fatal("unisolated job kept running after it was marked orphaned")
	}
}

func TestOrphanGroupWithReusedPidIsSpared(t *testing.T) {
	s, id, _ := attachFixture(t, "running")
	cmd := startGroup(t)
	s.recordJobGroup(s.jobs[id], cmd.Process.Pid)
	s.jobs[id].PgidStart -= 5000 // the recorded leader started earlier: a different process

	s.failOrphanedJobs()

	if waitExit(cmd, 500*time.Millisecond) {
		t.Fatal("killed a process group whose leader is not the job's")
	}
}

func TestFinishedJobLogLeavesMap(t *testing.T) {
	s, id, _ := attachFixture(t, "running")
	path := filepath.Join(s.jobDir, id, jobOutputFile)
	l := s.jobOutput(id)
	l.Append(OutputMessage{O: "hi"})
	if _, ok := jobLogs.Load(path); !ok {
		t.Fatal("a running job's log is not shared")
	}
	l.Finish(OutputMessage{ExitCode: 3})
	if _, ok := jobLogs.Load(path); ok {
		t.Fatal("finished job's log still held in memory")
	}
	again := s.jobOutput(id)
	if f := again.finalFrame(); f == nil || f.ExitCode != 3 {
		t.Fatalf("reopened log final frame = %+v", f)
	}
	if _, ok := jobLogs.Load(path); ok {
		t.Fatal("reopening a finished log stored it again")
	}
	if f := s.jobFinalFrame(id); f == nil || f.ExitCode != 3 {
		t.Fatalf("status final frame = %+v", f)
	}
}

func TestPruneDropsJobLog(t *testing.T) {
	s, id, _ := attachFixture(t, "failed")
	path := filepath.Join(s.jobDir, id, jobOutputFile)
	s.jobOutput(id).Append(OutputMessage{O: "stale"})
	old := time.Now().Add(-2 * JobRetention)
	if err := os.Chtimes(filepath.Join(s.jobDir, id), old, old); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PruneCompletedJobDirs(JobRetention); err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	if _, ok := jobLogs.Load(path); ok {
		t.Fatal("pruned job's log still held in memory")
	}
}

func TestDropWorkerKeepsFreshWorker(t *testing.T) {
	pm := newPoolManager()
	failed, fresh := &poolWorker{}, &poolWorker{}
	pm.workers["store"] = fresh
	pm.dropWorker("store", failed)
	if pm.workers["store"] != fresh {
		t.Fatal("dropping a failed worker removed the freshly spawned one")
	}
	pm.dropWorker("store", fresh)
	if _, ok := pm.workers["store"]; ok {
		t.Fatal("failed worker not dropped")
	}
}

func TestRunLocalSpawnFailureReleasesLock(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	pm := newPoolManager()
	missing := filepath.Join(t.TempDir(), "no-such-run")
	pm.runLocal(missing, "store", &chunk{})
	if !pm.mu.TryLock() {
		t.Fatal("a failed warm-worker spawn left the pool lock held")
	}
	pm.mu.Unlock()
}
