package app

import (
	"errors"
	"regexp"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// BuildError is a failure to build the script's environment. The build runs on
// the submitter, so placing the job on another node cannot change the result.
type BuildError struct {
	Err error
}

func (e *BuildError) Error() string { return e.Err.Error() }
func (e *BuildError) Unwrap() error { return e.Err }

// SandboxError is a job the node's sandbox (crun) failed to start: the script
// never ran, so another node may well run it.
type SandboxError struct {
	Err error
}

func (e *SandboxError) Error() string {
	return "the node's sandbox could not start the job: " + e.Err.Error()
}
func (e *SandboxError) Unwrap() error { return e.Err }

// crunFailure matches the line crun prints when it fails before the script
// runs: "<what>: <strerror>", what often quoting paths in backticks
// ("mount `proc` to `/proc`: Operation not permitted", "unshare: Operation not
// permitted"). It starts lower-case, so a Python exception line
// ("PermissionError: [Errno 1] ...") never matches.
var crunFailure = regexp.MustCompile("^[a-z_][^:]*: (Operation not permitted|Permission denied|No such file or directory|" +
	"Invalid argument|Read-only file system|No space left on device|Device or resource busy|" +
	"Function not implemented|Resource temporarily unavailable|Too many open files|Cannot allocate memory|" +
	"File exists|Not a directory|Bad file descriptor|No such process)$")

// sandboxFailed reports whether a job that exited with code 1 failed in crun
// rather than in the script. crun exits 1 for its own setup errors, the same
// code a failing script uses; its error is then the last line of stderr.
// ponytail: a stderr heuristic; a daemon-side failure kind on the done frame
// would be exact.
func sandboxFailed(err error, stderr string) bool {
	var xe *daemonctl.RemoteExitError
	if !errors.As(err, &xe) || xe.Code != 1 {
		return false
	}
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	return crunFailure.MatchString(strings.TrimSpace(lines[len(lines)-1]))
}

// IsPermanent reports whether a failed run would fail the same way wherever it
// was placed: the environment did not build, or the script itself exited with
// an error (1-125). Retrying those only repeats the failure, and the
// coordinator used to do so forever.
//
// Also counted: a node that refused the closure's signatures (permanent for
// that node; the coordinator moves the job to the next one).
//
// Not counted: 126 and 127 (the node could not execute bin/run: a broken or
// missing closure import), a death by signal (above 128: a node that was shut
// down or ran short of memory kills its job that way), and a sandbox that
// failed to start (*SandboxError). Another node may well finish those.
func IsPermanent(err error) bool {
	var be *BuildError
	if errors.As(err, &be) {
		return true
	}
	var se *SandboxError
	if errors.As(err, &se) {
		return false
	}
	// A node that refused the closure's signatures refuses it again on every
	// retry; each one cost a multi-GB resend (V2 45-friend). For that node
	// only: ExecuteWithRetry sees ClosureRefused first and places the job on
	// another node.
	if coordinator.ClosureRefused(err) {
		return true
	}
	var xe *daemonctl.RemoteExitError
	if errors.As(err, &xe) {
		return xe.Code > 0 && xe.Code <= 125
	}
	return false
}
