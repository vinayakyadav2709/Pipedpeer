package daemonctl

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// sessionOf reads the session id from a /proc/<pid>/stat body.
func sessionOf(t *testing.T, stat []byte) int {
	t.Helper()
	// Fields after the ")" that ends the command name: state ppid pgrp session ...
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	sid, err := strconv.Atoi(fields[3])
	if err != nil {
		t.Fatalf("no session id in %q", stat)
	}
	return sid
}

// The daemon must not share the starting command's session: a hangup of
// that session's terminal (an ssh -t that ends) would kill it.
func TestDetachedCommandHasItsOwnSession(t *testing.T) {
	out := filepath.Join(t.TempDir(), "stat")
	log, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := detachedCommand([]string{"cat", "/proc/self/stat"}, log)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	sid := sessionOf(t, stat)
	self, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	mine := sessionOf(t, self)
	if sid == mine {
		t.Fatalf("the daemon would share this session (%d)", sid)
	}
	if sid != cmd.Process.Pid {
		t.Fatalf("session %d is not led by the started process %d", sid, cmd.Process.Pid)
	}
}
