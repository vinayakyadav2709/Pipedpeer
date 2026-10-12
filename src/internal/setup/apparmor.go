package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/userns"
)

// Seams for the AppArmor step: the filesystem root the profile is read from,
// the sysctl, and both namespace probes.
var (
	apparmorRoot       = "/"
	apparmorRestricted = userns.AppArmorRestricted
	usernsAvailable    = userns.Available
	usernsProbeFresh   = userns.ProbeFresh
	selfPath           = os.Executable
)

func profilePath() string { return filepath.Join(apparmorRoot, userns.ProfileFile) }

// profileScript writes the profile when it differs from what is there, then
// (re)loads it. A profile it wrote that will not load is removed again, so
// boot does not trip over it.
func profileScript(path, want string, rewrite bool) string {
	lines := []string{"set -eu"}
	if rewrite {
		lines = append(lines,
			"cat > "+shq(path)+" <<'PIPEDPEER_PROFILE'\n"+want+"PIPEDPEER_PROFILE",
			"apparmor_parser -r "+shq(path)+" || { rm -f "+shq(path)+"; exit 1; }")
	} else {
		lines = append(lines, "apparmor_parser -r "+shq(path))
	}
	return strings.Join(lines, "\n") + "\n"
}

// profileRemoveScript unloads the profile and deletes it. A profile that is
// not loaded still has its file deleted, and says so.
func profileRemoveScript(path string) string {
	return strings.Join([]string{
		"set -eu",
		"apparmor_parser -R " + shq(path) + " || echo " + shq("pipedpeer: could not unload "+path+"; deleting it anyway") + " >&2",
		"rm -f " + shq(path),
	}, "\n") + "\n"
}

// allowUserns installs the AppArmor profile that Ubuntu 24.04+
// (kernel.apparmor_restrict_unprivileged_userns = 1) requires before crun may
// build the job sandbox. Never fatal: any failure leaves jobs running
// unisolated with their warning, and prints how to fix it by hand. Reports
// whether the profile was loaded by this run, so a daemon already running,
// which AppArmor attached nothing to, can be told to restart.
func allowUserns(noInstall bool) bool {
	if !apparmorRestricted() {
		return false
	}
	fmt.Println()
	if ok, _ := usernsAvailable(); ok {
		fmt.Println("    sandbox      ✓  AppArmor profile in place; jobs run isolated")
		return false
	}
	exe, err := selfPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup: cannot locate this executable for the AppArmor profile: %v\n", err)
		exe = "/usr/local/bin/pipedpeer"
		profileManual(exe, "cannot locate this executable: "+err.Error())
		return false
	}
	if noInstall {
		profileManual(exe, "--no-install was given, so the AppArmor profile was not installed")
		return false
	}
	if !canBecomeRoot() {
		profileManual(exe, "installing the AppArmor profile needs root, and sudo is not available here")
		return false
	}

	want := userns.Profile(exe)
	have, err := os.ReadFile(profilePath())
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "setup: reading %s: %v; rewriting it\n", profilePath(), err)
	}
	fmt.Println("    sandbox      …  AppArmor blocks user namespaces here; installing a profile for pipedpeer (sudo)")
	if err := runAsRoot(profileScript(profilePath(), want, string(have) != want)); err != nil {
		profileManual(exe, "installing the profile failed: "+err.Error())
		return false
	}
	// This process was exec'd before the profile was loaded, and AppArmor
	// attaches a profile only at exec: only a new process can tell.
	if ok, why := usernsProbeFresh(); !ok {
		profileManual(exe, "the profile loaded, but a new process still cannot create a user namespace:\n"+indent(why))
		return false
	}
	fmt.Println("    sandbox      ✓  AppArmor profile loaded; jobs run isolated")
	return true
}

// profileManual is the failure message: what is wrong, the steps that fix it
// by hand, and how to check.
func profileManual(exe, why string) {
	cmds := userns.ProfileCommands(exe)
	fmt.Printf("    sandbox      ✗  AppArmor blocks user namespaces here (Ubuntu 24.04+): %s\n", why)
	fmt.Println("                    Jobs still run, but unisolated, each with a warning. To fix it, once:")
	fmt.Printf("                    1. %s\n", cmds[0])
	fmt.Printf("                    2. %s\n", cmds[1])
	fmt.Println("                    3. pipedpeer stop   (the daemon picks the profile up only when it restarts)")
	fmt.Println("                    If the profile will not load, instead of 1-2 lift the restriction for every")
	fmt.Println("                    program (weaker: Ubuntu restricts it to shrink the kernel's attack surface):")
	fmt.Println("                       sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0")
	fmt.Println("                       echo 'kernel.apparmor_restrict_unprivileged_userns=0' | sudo tee /etc/sysctl.d/99-pipedpeer.conf")
	fmt.Println("                    Check: run a job; its output has no \"unisolated\" line.")
	fmt.Println("                    Then re-run pipedpeer setup.")
}

func indent(s string) string {
	return "                    " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n                    ")
}

// removeProfile is the AppArmor half of `pipedpeer setup --uninstall`. No
// profile, no sudo prompt.
func removeProfile() error {
	if _, err := os.Stat(profilePath()); err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "setup: checking %s: %v; leaving it\n", profilePath(), err)
		}
		return nil
	}
	fmt.Println("  Removing pipedpeer's AppArmor profile (sudo)...")
	if err := runAsRoot(profileRemoveScript(profilePath())); err != nil {
		return fmt.Errorf("could not remove the AppArmor profile %s: %w", profilePath(), err)
	}
	return nil
}
