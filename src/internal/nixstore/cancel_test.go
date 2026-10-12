package nixstore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// alive reports whether pid still exists and is not a zombie.
func alive(pid int) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	f := strings.Fields(string(b))
	return len(f) > 2 && f[2] != "Z"
}

// waitGone polls generously: a loaded CI runner can take a while to reap.
func waitGone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for alive(pid) {
		if time.Now().After(deadline) {
			syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("%s (pid %d) survived the cancel", what, pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Cancelling the ctx kills what the child forked, not just the child: a nix
// started by Ctrl+C'd CLI must not keep exporting gigabytes.
func TestDieWithKillsTheGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "sleep 60 & echo $!; wait")
	dieWith(cmd)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var line [32]byte
	n, _ := out.Read(line[:])
	grandchild, err := strconv.Atoi(strings.TrimSpace(string(line[:n])))
	if err != nil {
		t.Fatalf("no grandchild pid: %q", line[:n])
	}
	cancel()
	if err := cmd.Wait(); err == nil {
		t.Fatal("a cancelled command reported success")
	}
	waitGone(t, grandchild, "the forked sleep")
}

// A private store runs nix inside crun, which setsid()s the container: the
// group kill alone would miss it. crun must forward the SIGTERM.
func TestDieWithKillsTheCrunContainer(t *testing.T) {
	crun, err := exec.LookPath("crun")
	if err != nil {
		t.Skip("no crun")
	}
	bundle, err := userdir.Scratch("canceltest-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(bundle)
	root := bundle + "/nixroot"
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PIPEDPEER_NIX_ROOT", root)
	if err := writeBundle(bundle, []string{"/bin/sh", "-c", "echo $$; exec sleep 60"}, "/"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, crun, "run", "--bundle", bundle, "pipedpeer-canceltest-"+strconv.Itoa(os.Getpid()))
	dieWith(cmd)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var line [32]byte
	n, _ := out.Read(line[:])
	inside, err := strconv.Atoi(strings.TrimSpace(string(line[:n])))
	if err != nil {
		// No pid namespace in the bundle, so $$ is the host pid; anything
		// else means crun could not start the container here.
		cancel()
		cmd.Wait()
		t.Skipf("crun could not run the container here: %q", line[:n])
	}
	cancel()
	cmd.Wait()
	waitGone(t, inside, "the process inside crun")
}
