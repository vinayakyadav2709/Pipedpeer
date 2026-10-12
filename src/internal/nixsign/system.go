package nixsign

import (
	"os/exec"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// Seams for the machine's nix, replaced in tests.
var (
	privateStore = nixstore.Private

	// multiUserNix reports whether nix builds go through a root nix-daemon
	// (Determinate installer, distro multi-user setups). That daemon ignores
	// extra-trusted-public-keys from a user it does not list in
	// trusted-users, so narpack.Import passing the keys is not enough there.
	multiUserNix = func() bool {
		out, err := exec.Command("systemctl", "is-active", "nix-daemon").Output()
		return err == nil && strings.TrimSpace(string(out)) == "active"
	}

	nixConfig = func(name string) (string, error) {
		nixPath, err := nixstore.SystemNix()
		if err != nil {
			return "", err
		}
		out, err := exec.Command(nixPath, "config", "show", name).Output()
		return string(out), err
	}
)

// SystemUntrusted reports which of keys this machine's nix accepts only from
// nix.conf and does not list there.
//
// applies is false where nix.conf does not decide: a private store and a
// single-user nix take the keys from each import (narpack.Import), and a nix
// with require-sigs off accepts anything. Then missing is nil and there is
// nothing to fix. On a multi-user nix-daemon the keys must be in nix.conf,
// which only root can edit (`pipedpeer trust`).
func SystemUntrusted(keys []string) (missing []string, applies bool, err error) {
	if privateStore() || !multiUserNix() {
		return nil, false, nil
	}
	if v, err := nixConfig("require-sigs"); err == nil && strings.TrimSpace(v) == "false" {
		return nil, false, nil
	}
	have, err := nixConfig("trusted-public-keys")
	if err != nil {
		return nil, true, err
	}
	trusted := map[string]bool{}
	for _, k := range strings.Fields(have) {
		trusted[k] = true
	}
	for _, k := range keys {
		if !trusted[k] {
			missing = append(missing, k)
		}
	}
	return missing, true, nil
}
