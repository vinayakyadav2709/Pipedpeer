package daemonctl

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

// detachedCommand runs argv in a session of its own, output to log. The
// daemon outlives the command that started it, and that command's terminal:
// started over `ssh -t` (Ansible's raw module, how the lab campaign drives
// every PC), a daemon left in the caller's session got SIGHUP when the
// session's pty closed and died seconds after `pipedpeer start` reported
// it running.
func detachedCommand(argv []string, log *os.File) *exec.Cmd {
	cmd := termuxexec.Command(argv[0], argv[1:]...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}
