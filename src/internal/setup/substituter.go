package setup

import (
	"errors"
	"fmt"
	"io"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
)

// Seams for tests: the multi-user check, the substituter state and the
// question.
var (
	multiUser         = nixsign.MultiUser
	substituterListed = nixtrust.SubstituterListed
	helperSync        = nixtrust.Sync
	askYes            = ask
	installHelperFn   = installHelper
)

// EnsureSubstituter makes a multi-user nix-daemon accept the local peer cache
// (http://127.0.0.1:4242/nix-cache) from builds this user starts: the helper
// writes extra-trusted-substituters into its root-owned file. It does nothing
// off multi-user nix or when the line is already there. When root is needed
// and the run is not interactive it returns ErrNeedsSetup instead of asking.
func EnsureSubstituter(w io.Writer, interactive, autoYes bool) error {
	if !multiUser() || substituterListed() {
		return nil
	}
	if helperReady() {
		err := helperSync()
		if err == nil {
			fmt.Fprintln(w, "    → Peers' builds can now fill this machine's nix cache misses.")
			return nil
		}
		if !errors.Is(err, nixtrust.ErrOldHelper) {
			return fmt.Errorf("enabling the peer cache in nix: %w", err)
		}
	}
	if !interactive {
		return ErrNeedsSetup
	}
	if !autoYes && !askYes("  Let nix fetch from the other machines? (reinstalls the nix key helper; asks for sudo once)") {
		fmt.Fprintln(w, "    → Peers' builds stay off on this machine until `pipedpeer setup`.")
		return nil
	}
	if err := installHelperFn(); err != nil {
		return fmt.Errorf("installing the nix key helper for the peer cache: %w", err)
	}
	if err := helperSync(); err != nil {
		return fmt.Errorf("enabling the peer cache in nix: %w", err)
	}
	fmt.Fprintln(w, "    → Peers' builds can now fill this machine's nix cache misses.")
	return nil
}
