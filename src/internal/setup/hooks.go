package setup

import (
	"errors"
	"io"

	"github.com/pipedpeer/pipedpeer/internal/phone"
)

// ErrNeedsSetup means a step needs `pipedpeer setup` (for example root
// access that join must not ask for). TrustAfterJoin ignores it.
var ErrNeedsSetup = errors.New("needs `pipedpeer setup`")

// Seams: tests replace these; the defaults are the real steps.
var (
	termuxPrep = func(w io.Writer, autoYes, noInstall bool) error {
		return phone.TermuxPrep(w, phone.PrepOpts{AutoYes: autoYes, NoInstall: noInstall})
	}
	ensureSubstituter = EnsureSubstituter
)
