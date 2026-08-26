// Package cgroups prepares a cgroup v2 subtree that an unprivileged daemon
// may create per-job cgroups under, so a memory limit is enforced by the
// kernel rather than merely estimated.
//
// Getting this working without root took two discoveries, both of which the
// code below encodes:
//
//  1. A process started from an ssh session or a plain shell lands in a
//     session scope that systemd owns as root. Nothing can be created there.
//     The user manager (`user@<uid>.service`) does have cpu/io/memory/pids
//     delegated on any current systemd, so the daemon has to be *in* that
//     subtree - which `systemd-run --user --scope` arranges, no privilege
//     required.
//
//  2. Being in a delegated cgroup is still not enough. cgroup v2 forbids a
//     cgroup from holding processes and enabling controllers for its children
//     at the same time, so `echo +memory > cgroup.subtree_control` on the
//     scope we live in fails with EBUSY and every child comes up with no
//     memory.max at all. The fix is to move ourselves down into a leaf first,
//     leaving the scope process-free and free to delegate.
package cgroups

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// mountpoint is the cgroup v2 filesystem root. A variable so tests can point
// it at a fixture tree.
var mountpoint = "/sys/fs/cgroup"

// jobsDir groups every job cgroup under one parent, so the daemon's cgroups
// are distinguishable from anything else sharing the scope.
const jobsDir = "pipedpeer"

// leafDir is where the daemon's own processes are parked so that the scope
// above can delegate controllers.
const leafDir = "daemon"

// Self returns this process's cgroup v2 path (the "0::" line), or "" when
// the machine is on cgroup v1 or the file is unreadable.
func Self() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		// cgroup v2 has exactly one entry, "0::<path>"; a v1 hierarchy lists
		// numbered controllers instead and is not handled.
		if strings.HasPrefix(line, "0::") {
			return strings.TrimPrefix(line, "0::")
		}
	}
	return ""
}

// Usable reports whether this process's cgroup can host job cgroups with an
// enforceable memory limit, and why not when it cannot. Called before
// starting the daemon to decide whether it needs relaunching into a scope of
// its own; nothing here mutates the hierarchy.
//
// Three separate things have to hold, and each of them was found the hard way
// on a real machine:
//
//   - The cgroup must be ours to write in. An ssh session scope is systemd's,
//     owned by root, and mkdir there fails outright.
//   - Children must be able to hold the memory controller. A scope that some
//     other application has made "domain threaded" only ever produces
//     "domain invalid" children, and memory is not a threaded controller, so
//     no limit can exist anywhere below it however the cgroup is arranged.
//   - We must be the only tenant. The leaf move in Prepare relocates every
//     process in the cgroup, and in a shared scope - a desktop app's, say -
//     that would drag dozens of unrelated processes along with it.
func Usable() (bool, string) {
	own := Self()
	if own == "" {
		return false, "no cgroup v2 (v1 hierarchy or unreadable /proc/self/cgroup)"
	}
	root := filepath.Join(mountpoint, own)
	if !strings.Contains(readFile(filepath.Join(root, "cgroup.controllers")), "memory") {
		return false, "memory controller not delegated to " + own
	}

	probe := filepath.Join(root, "pipedpeer-probe")
	if err := os.Mkdir(probe, 0o755); err != nil && !os.IsExist(err) {
		return false, "cgroup " + own + " is not writable: " + err.Error()
	}
	defer os.Remove(probe)
	if t := strings.TrimSpace(readFile(filepath.Join(probe, "cgroup.type"))); t != "domain" {
		return false, "cgroup " + own + " is \"" + strings.TrimSpace(readFile(filepath.Join(root, "cgroup.type"))) +
			"\", so its children are \"" + t + "\" and can never hold a memory limit"
	}
	if n, err := foreignProcs(root); err != nil {
		return false, "cannot read cgroup members: " + err.Error()
	} else if n > 0 {
		return false, "cgroup " + own + " is shared with " + strconv.Itoa(n) +
			" unrelated process(es), which the leaf move must not relocate"
	}
	return true, ""
}

// foreignProcs counts members of cg that are neither this process nor one of
// its descendants.
func foreignProcs(cg string) (int, error) {
	data, err := os.ReadFile(filepath.Join(cg, "cgroup.procs"))
	if err != nil {
		return 0, err
	}
	self := os.Getpid()
	foreign := 0
	for _, field := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			continue
		}
		if !descendsFrom(pid, self) {
			foreign++
		}
	}
	return foreign, nil
}

// descendsFrom walks pid's parent chain looking for want. A process that
// exits mid-walk simply stops the chain, and counts as foreign - the
// conservative answer, since the cost of being wrong is moving somebody
// else's processes.
func descendsFrom(pid, want int) bool {
	for i := 0; pid > 1 && i < 64; i++ {
		if pid == want {
			return true
		}
		ppid, ok := parentOf(pid)
		if !ok {
			return false
		}
		pid = ppid
	}
	return pid == want
}

func parentOf(pid int) (int, bool) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, false
	}
	// The comm field is parenthesised and may contain spaces, so fields are
	// counted from after the closing paren: state, then ppid.
	close := strings.LastIndex(string(data), ")")
	if close < 0 {
		return 0, false
	}
	f := strings.Fields(string(data)[close+1:])
	if len(f) < 2 {
		return 0, false
	}
	ppid, err := strconv.Atoi(f[1])
	return ppid, err == nil
}

// ScopePrefix returns an argv prefix that runs a command inside a transient,
// delegated user scope, or nil when that is neither possible nor needed.
//
// Nothing here needs privilege: `--user` talks to the caller's own systemd
// manager over its session bus. Machines with no systemd (containers, the
// lab workers) get nil and fall back to an unlimited-but-working daemon.
func ScopePrefix(unit string) []string {
	if ok, _ := Usable(); ok {
		return nil
	}
	if _, err := exec.LookPath("systemd-run"); err != nil {
		return nil
	}
	// Without a session bus `systemd-run --user` cannot reach the user
	// manager, and would fail the daemon start rather than merely skipping
	// the limit.
	if os.Getenv("XDG_RUNTIME_DIR") == "" && os.Getenv("DBUS_SESSION_BUS_ADDRESS") == "" {
		return nil
	}
	return []string{
		"systemd-run", "--user", "--quiet", "--collect",
		"--scope", "--unit=" + unit,
		"--property=Delegate=yes",
		"--",
	}
}

var prepareOnce struct {
	sync.Once
	parent string
	ok     bool
	reason string
}

// Prepare readies the subtree that per-job cgroups are created in and returns
// its path relative to the cgroup root, plus whether a memory limit can be
// enforced at all. Probed once; the reason for a "no" is reported to the
// caller so it can be logged with the daemon's own logger.
func Prepare() (parent string, ok bool, reason string) {
	prepareOnce.Do(func() {
		prepareOnce.parent, prepareOnce.ok, prepareOnce.reason = prepare()
	})
	return prepareOnce.parent, prepareOnce.ok, prepareOnce.reason
}

func prepare() (string, bool, string) {
	if ok, why := Usable(); !ok {
		return "", false, why
	}
	own := Self()
	root := filepath.Join(mountpoint, own)

	// Park our processes in a leaf so the cgroup above can delegate. Skipped
	// when it holds nothing, which is the case if a previous run already did
	// this and we are now the leaf's child.
	if err := vacate(root); err != nil {
		return "", false, "cannot move own processes into a leaf cgroup: " + err.Error()
	}

	// EBUSY here means processes remain above us; EACCES means the cgroup is
	// not delegated at all. Already-enabled is not an error.
	if err := enableMemory(root); err != nil {
		return "", false, "memory controller not delegated: " + err.Error()
	}

	jobs := filepath.Join(root, jobsDir)
	if err := os.Mkdir(jobs, 0o755); err != nil && !os.IsExist(err) {
		return "", false, "cannot create job cgroup parent: " + err.Error()
	}
	if err := enableMemory(jobs); err != nil {
		return "", false, "memory controller not delegated to job cgroups: " + err.Error()
	}

	// Creating the cgroup is not evidence that a limit can be set in it:
	// crun's failure for the gap is "open `memory.max` for writing: No such
	// file or directory", and it fails the job outright. Settle it here.
	probe := filepath.Join(jobs, "probe")
	if err := os.Mkdir(probe, 0o755); err != nil && !os.IsExist(err) {
		return "", false, "cannot create a job cgroup: " + err.Error()
	}
	defer os.Remove(probe)
	if _, err := os.Stat(filepath.Join(probe, "memory.max")); err != nil {
		return "", false, "job cgroups have no memory.max"
	}
	return filepath.Join(own, jobsDir), true, ""
}

// vacate moves every process in cg down into a leaf child, leaving cg empty.
// A process that exits mid-move reports ESRCH, which is not a failure: the
// only thing that matters is that cgroup.procs ends up empty.
func vacate(cg string) error {
	procs, err := os.ReadFile(filepath.Join(cg, "cgroup.procs"))
	if err != nil {
		return err
	}
	pids := strings.Fields(string(procs))
	if len(pids) == 0 {
		return nil
	}
	leaf := filepath.Join(cg, leafDir)
	if err := os.Mkdir(leaf, 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	dst := filepath.Join(leaf, "cgroup.procs")
	for _, pid := range pids {
		// One write per pid: cgroup.procs accepts a single value, and a
		// batch write would abort the whole move on the first dead process.
		f, err := os.OpenFile(dst, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, werr := f.WriteString(pid)
		f.Close()
		if werr != nil && processAlive(pid) {
			return werr
		}
	}
	return nil
}

func processAlive(pid string) bool {
	_, err := os.Stat(filepath.Join("/proc", pid))
	return err == nil
}

// enableMemory hands the memory controller down to cg's children. Writing a
// controller that is already enabled is a no-op for the kernel but returns no
// error, so this is safe to repeat.
func enableMemory(cg string) error {
	if _, err := os.Stat(filepath.Join(cg, "cgroup.subtree_control")); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cg, "cgroup.subtree_control"), []byte("+memory"), 0o644); err != nil {
		// Already enabled? Then the write is unnecessary and the failure is
		// not one, which shows up as memory.max existing in a child.
		if strings.Contains(readFile(filepath.Join(cg, "cgroup.subtree_control")), "memory") {
			return nil
		}
		return err
	}
	return nil
}

func readFile(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}
