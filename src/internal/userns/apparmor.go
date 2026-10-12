package userns

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ProfileFile is where the AppArmor profile that lets pipedpeer create user
// namespaces lives. diagnose() tells the user to write it; `pipedpeer setup`
// writes it itself.
const ProfileFile = "/etc/apparmor.d/pipedpeer"

// Profile is that profile's text for the binary at exe. It names only the
// pipedpeer binary: crun is exec'd by it (the daemon's job path in
// daemonapi/execution.go, the private store's in nixstore.go, the quirks probe
// in daemonapi/sandboxquirks.go), and a process in a profile flagged
// unconfined keeps that profile across exec unless another profile attaches to
// the new binary (the kernel's profile_transition, security/apparmor/domain.c).
// Verified on Ubuntu 26.04 that crun then builds its namespace.
func Profile(exe string) string {
	return fmt.Sprintf("abi <abi/4.0>,\ninclude <tunables/global>\nprofile pipedpeer %s flags=(unconfined) {\n  userns,\n}\n", exe)
}

// ProfileCommands are the root commands that install Profile(exe) by hand,
// one per line, for the messages that tell a user to.
func ProfileCommands(exe string) []string {
	return []string{
		fmt.Sprintf("printf '%s' | sudo tee %s", strings.ReplaceAll(Profile(exe), "\n", `\n`), ProfileFile),
		"sudo apparmor_parser -r " + ProfileFile,
	}
}

// AppArmorRestricted reports kernel.apparmor_restrict_unprivileged_userns = 1
// (Ubuntu 24.04+), the one cause of a refused namespace that a profile fixes.
func AppArmorRestricted() bool {
	return sysctl("/proc/sys/kernel/apparmor_restrict_unprivileged_userns") == "1"
}

// sysctlRoot is "/" except under test. The diagnosis is the whole reason this
// package exists, and it is produced only on a machine that refuses namespaces
// - so on every machine we develop on, the test for it skipped. A test that
// cannot run is not a test, and the AppArmor branch is the one that matters
// most and is hardest to reach: it needs Ubuntu 24.04+.
var sysctlRoot = "/"

func sysctl(path string) string {
	b, err := os.ReadFile(filepath.Join(sysctlRoot, path))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
