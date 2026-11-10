package daemonapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// countingStore is a fake closure whose bin/run logs "<pid> <script>" each
// time it is started, so a test can tell warm workers from cold runs and a
// reused worker from a new process.
type countingStore struct {
	path, runPath, spawnLog string
}

func newCountingStore(t *testing.T) countingStore {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	store := filepath.Join(t.TempDir(), "nix", "store", "fake")
	cs := countingStore{path: store, runPath: filepath.Join(store, "bin", "run"), spawnLog: filepath.Join(store, "spawns")}
	if err := os.MkdirAll(filepath.Dir(cs.runPath), 0755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\necho $$ $(basename \"$1\") >> " + cs.spawnLog + "\nexec " + python + " \"$@\"\n"
	if err := os.WriteFile(cs.runPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return cs
}

// warmPids are the pids of every warm worker started from the store.
func (cs countingStore) warmPids(t *testing.T) []int {
	t.Helper()
	b, err := os.ReadFile(cs.spawnLog)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == "warm_worker.py" {
			pid, _ := strconv.Atoi(f[0])
			pids = append(pids, pid)
		}
	}
	return pids
}

// processState reports what the process table holds for pid: "gone" once it
// has been reaped, "zombie" while it has exited and nobody waited for it, else
// "running".
func processState(t *testing.T, pid int) string {
	t.Helper()
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		if _, perr := os.Stat("/proc/self/stat"); perr != nil {
			t.Skip("no /proc to read the process table from")
		}
		return "gone"
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) > 0 && fields[0] == "Z" {
		return "zombie"
	}
	return "running"
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const identityFunc = "def run(x):\n    return x\n"

func oneItem(funcSrc string) *chunk {
	return &chunk{funcSrc: funcSrc, funcName: "run", items: []json.RawMessage{json.RawMessage("1")}}
}

func (pm *poolManager) workerFor(storePath string) *poolWorker {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.workers[storePath]
}

// A worker that exits is reaped and replaced by the next chunk.
func TestWarmWorkerExitIsReapedAndReplaced(t *testing.T) {
	cs := newCountingStore(t)
	pm := newPoolManager()
	defer pm.stopAll()

	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatal(err)
	}
	first := pm.workerFor(cs.path)
	pid := first.cmd.Process.Pid
	first.cmd.Process.Kill()

	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatalf("chunk after the worker exited: %v", err)
	}
	waitFor(t, "the exited worker to leave the process table", func() bool { return processState(t, pid) == "gone" })

	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatal(err)
	}
	second := pm.workerFor(cs.path)
	if second == nil || second == first || second.dead() {
		t.Fatal("the next chunk did not get a new, live worker")
	}
	if n := len(cs.warmPids(t)); n != 2 {
		t.Fatalf("%d warm workers started, want 2", n)
	}
}

// Many requests failing on one dead worker must leave exactly one replacement:
// none orphaned, none left as a zombie.
func TestConcurrentWorkerFailuresSpawnOneReplacement(t *testing.T) {
	cs := newCountingStore(t)
	marker := filepath.Join(t.TempDir(), "died-once")
	dieOnce := "import os, time\ndef run(x):\n" +
		"    if not os.path.exists(" + strconv.Quote(marker) + "):\n" +
		"        open(" + strconv.Quote(marker) + ", 'w').close()\n" +
		"        time.sleep(0.3)\n" +
		"        os._exit(3)\n" +
		"    return x\n"

	pm := newPoolManager()
	const requests, rounds = 8, 3
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				pm.runLocal(cs.runPath, cs.path, oneItem(dieOnce))
			}
		}()
	}
	wg.Wait()

	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatalf("chunk after the failures: %v", err)
	}
	pids := cs.warmPids(t)
	if len(pids) != 2 {
		t.Fatalf("%d warm workers started, want 2: the one that died and one replacement", len(pids))
	}
	live := pm.workerFor(cs.path)
	waitFor(t, "every dead worker to be reaped", func() bool {
		for _, pid := range pids {
			if processState(t, pid) == "zombie" {
				return false
			}
		}
		return true
	})
	running := 0
	for _, pid := range pids {
		if processState(t, pid) == "running" {
			running++
			if pid != live.cmd.Process.Pid {
				t.Errorf("worker %d is running but no longer owned by the pool", pid)
			}
		}
	}
	if running != 1 {
		t.Fatalf("%d worker processes running, want 1", running)
	}

	pm.stopAll()
	if st := processState(t, live.cmd.Process.Pid); st != "gone" {
		t.Fatalf("after shutdown the worker is %s, want gone", st)
	}
}

// Shutdown kills every worker and waits for it: nothing is left running or
// unreaped when stopAll returns.
func TestStopAllKillsAndReapsWorkers(t *testing.T) {
	cs := newCountingStore(t)
	pm := newPoolManager()
	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatal(err)
	}
	w := pm.workerFor(cs.path)
	pid := w.cmd.Process.Pid
	pm.stopAll()
	if st := processState(t, pid); st != "gone" {
		t.Fatalf("after stopAll the worker is %s, want gone", st)
	}
	if !w.dead() {
		t.Fatal("a stopped worker does not report dead")
	}
}
