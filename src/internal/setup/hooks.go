package setup

import (
	"errors"
	"io"
)

// ErrNeedsSetup means a step needs `pipedpeer setup` (for example root
// access that join must not ask for). TrustAfterJoin ignores it.
var ErrNeedsSetup = errors.New("needs `pipedpeer setup`")

// Seams to code owned by other units. The wiring unit points them at
// phone.TermuxPrep and EnsureSubstituter; until then they do nothing.
var (
	termuxPrep        = func(w io.Writer, autoYes, noInstall bool) error { return nil }
	ensureSubstituter = func(w io.Writer, interactive, autoYes bool) error { return nil }
)
