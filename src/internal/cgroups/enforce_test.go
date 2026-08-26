package cgroups

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// hogEnv makes a re-exec of this test binary allocate until it dies, so the
// test needs no python or helper binary on the machine under test.
const hogEnv = "PIPEDPEER_CGROUP_HOG_MB"

// scopeEnv marks a re-exec that is already inside a delegated scope, so the
// relaunch below cannot loop.
const scopeEnv = "PIPEDPEER_CGROUP_TEST_SCOPED"

func TestMain(m *testing.M) {
	if mb := os.Getenv(hogEnv); mb != "" {
		n, _ := strconv.Atoi(mb)
		var held [][]byte
		for i := 0; i < n; i++ {
			b := make([]byte, 1<<20)
			// Touch every page: an untouched allocation is never charged to
			// the cgroup, so a limit would appear not to work.
			for j := 0; j < len(b); j += 4096 {
				b[j] = byte(i)
			}
			held = append(held, b)
		}
		os.Exit(len(held) & 0) // 0; reached only if the limit did not hold
	}

	// A test run started from a shell or an ssh session lands in a session
	// scope that systemd owns as root, where nothing can be created - the
	// enforcing path would be unreachable and this file would only ever
	// skip. Relaunch once inside a delegated user scope, which is exactly
	// what `pipedpeer start` does and needs no privilege.
	if usable, _ := Usable(); os.Getenv(scopeEnv) == "" && !usable {
		if prefix := ScopePrefix("pipedpeer-cgroup-test"); prefix != nil {
			argv := append(prefix, append([]string{os.Args[0]}, os.Args[1:]...)...)
			cmd := exec.Command(argv[0], argv[1:]...)
			cmd.Env = append(os.Environ(), scopeEnv+"=1")
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			if err := cmd.Run(); err == nil {
				os.Exit(0)
			} else if ee, ok := err.(*exec.ExitError); ok {
				os.Exit(ee.ExitCode())
			}
			// Falling through on a launch failure is deliberate: the tests
			// below then skip with a reason instead of reporting a failure
			// that is really about this machine's systemd.
		}
	}
	os.Exit(m.Run())
}

// TestPrepareEnforcesMemoryLimit is the only proof that the memory limit is
// real. Every other memory bound in this codebase is a forecast made before
// the job ran; this one asks the kernel to kill a process that exceeds it,
// and fails if the process survives.
//
// It got written because the first version of the limit shipped unverified
// and was wrong twice over: it wrote job cgroups at the hierarchy root, which
// only root may do, and once that was fixed the children still had no
// memory.max because a cgroup holding processes may not delegate controllers
// to its children.
func TestPrepareEnforcesMemoryLimit(t *testing.T) {
	parent, ok, reason := Prepare()
	if !ok {
		// Loud on purpose. A quiet skip here is indistinguishable from a
		// pass, and this test exists because that exact confusion shipped.
		t.Skipf("NOT VERIFIED on this machine: %s\n"+
			"\tThe memory limit is unenforced here. On a systemd machine this test\n"+
			"\trelaunches itself into a delegated scope and does run.", reason)
	}
	t.Logf("job cgroups at %s", parent)

	cg := filepath.Join(mountpoint, parent, "enforce-test")
	if err := os.Mkdir(cg, 0o755); err != nil && !os.IsExist(err) {
		t.Fatalf("create job cgroup: %v", err)
	}
	defer os.Remove(cg)

	const limitMB = 64
	if err := os.WriteFile(filepath.Join(cg, "memory.max"), []byte(strconv.Itoa(limitMB<<20)), 0o644); err != nil {
		t.Fatalf("write memory.max: %v", err)
	}
	// Without this the hog swaps instead of dying, and the test hangs the
	// machine rather than failing - which is also what an unpinned swap does
	// to a real node.
	_ = os.WriteFile(filepath.Join(cg, "memory.swap.max"), []byte("0"), 0o644)

	dir, err := os.Open(cg)
	if err != nil {
		t.Fatalf("open job cgroup: %v", err)
	}
	defer dir.Close()

	// CLONE_INTO_CGROUP: the child starts in the capped cgroup, so there is
	// no window in which it allocates outside the limit.
	cmd := exec.Command(os.Args[0], "-test.run=TestPrepareEnforcesMemoryLimit")
	cmd.Env = append(os.Environ(), hogEnv+"="+strconv.Itoa(limitMB*8), scopeEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(dir.Fd())}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("a process allocating %d MiB under a %d MiB cap ran to completion; "+
			"the limit is not enforced\noutput: %s", limitMB*8, limitMB, out)
	}

	// Distinguish "the kernel killed it" from "it failed for some other
	// reason", which would pass the check above while proving nothing.
	ee, isExit := err.(*exec.ExitError)
	if !isExit {
		t.Fatalf("hog did not run: %v", err)
	}
	ws, isWait := ee.Sys().(syscall.WaitStatus)
	if !isWait || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("hog exited %v, want death by SIGKILL\noutput: %s", ee, out)
	}

	events, _ := os.ReadFile(filepath.Join(cg, "memory.events"))
	if !oomKilled(string(events)) {
		t.Fatalf("no oom_kill recorded in the job cgroup; the SIGKILL came from "+
			"somewhere other than the memory limit\nmemory.events:\n%s", events)
	}
	t.Logf("kernel killed a %d MiB allocation under a %d MiB cap", limitMB*8, limitMB)
}

func oomKilled(events string) bool {
	for _, line := range strings.Split(events, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "oom_kill" && f[1] != "0" {
			return true
		}
	}
	return false
}
