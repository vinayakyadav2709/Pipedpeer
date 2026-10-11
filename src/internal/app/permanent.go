package app

import (
	"errors"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// BuildError is a failure to build the script's environment. The build runs on
// the submitter, so placing the job on another node cannot change the result.
type BuildError struct {
	Err error
}

func (e *BuildError) Error() string { return e.Err.Error() }
func (e *BuildError) Unwrap() error { return e.Err }

// IsPermanent reports whether a failed run would fail the same way wherever it
// was placed: the environment did not build, or the script itself exited with
// an error. Retrying those only repeats the failure, and the coordinator used
// to do so forever.
//
// A death by signal (exit codes above 128) is not counted: a node that was
// shut down or ran short of memory kills its job that way, and another node
// may well finish it.
func IsPermanent(err error) bool {
	var be *BuildError
	if errors.As(err, &be) {
		return true
	}
	var xe *daemonctl.RemoteExitError
	if errors.As(err, &xe) {
		return xe.Code > 0 && xe.Code <= 128
	}
	return false
}
