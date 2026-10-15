//go:build linux

// Package userns answers one question: may this unprivileged process create a
// user namespace?
//
// Everything rootless depends on it. crun builds the job sandbox out of a user
// namespace, so where namespaces are refused the sandbox cannot be built and
// jobs either run unisolated or not at all. The failure surfaces from crun as
// "unshare: Operation not permitted", which says nothing about the cause and
// nothing about the fix, and the causes are distinct enough to need different
// fixes.
package userns

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

// Available reports whether a user namespace can be created, and when it
// cannot, an explanation naming the exact one-time command that changes the
// answer.
//
// Probed by actually creating one rather than by reading sysctls: the
// restrictions are distribution-specific and stack, and a machine's real
// answer is worth more than a guess assembled from three knobs.
var Available = sync.OnceValues(available)

// probeEnv marks the re-exec that performs the second half of the probe.
const probeEnv = "PIPEDPEER_USERNS_PROBE"

// Install must run at the very start of main, before any flag parsing. When
// the probe re-executes this binary, the child's whole job is to try the
// second unshare and report by exit status; it must not start a daemon or
// print a usage message on the way.
func Install() {
	if os.Getenv(recheckEnv) != "" && os.Getenv(probeEnv) == "" {
		os.Unsetenv(recheckEnv)
		if ok, why := available(); !ok {
			fmt.Fprint(os.Stderr, why)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if os.Getenv(probeEnv) == "" {
		return
	}
	// Already inside a fresh user namespace with its id maps written. This
	// second, separate unshare is the step that a restricted machine denies.
	if err := syscall.Unshare(syscall.CLONE_NEWNS); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

// recheckEnv marks a re-exec whose only job is to run the whole probe and
// report by exit status, the reason on stderr.
const recheckEnv = "PIPEDPEER_USERNS_RECHECK"

// ProbeFresh runs the probe in a new process. Available is computed once per
// process, and AppArmor attaches a profile only at exec: after setup loads the
// pipedpeer profile, only a freshly exec'd binary shows whether it worked.
func ProbeFresh() (bool, string) {
	self, err := termuxexec.Executable()
	if err != nil {
		return false, "cannot locate this executable to probe with: " + err.Error()
	}
	cmd := termuxexec.Command(self)
	cmd.Env = append(os.Environ(), recheckEnv+"=1")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() == 0 {
			return false, "the namespace probe failed: " + err.Error()
		}
		return false, stderr.String()
	}
	return true, ""
}

func available() (bool, string) {
	self, err := termuxexec.Executable()
	if err != nil {
		return false, "cannot locate this executable to probe with: " + err.Error()
	}

	// Two unshares in sequence, not one clone with both flags - and the
	// difference is the entire point. On Ubuntu 24.04+ a single
	// clone(CLONE_NEWUSER|CLONE_NEWNS) succeeds, while creating the user
	// namespace and then unsharing the mount namespace from inside it is
	// refused: AppArmor confines the process at the moment it enters its own
	// user namespace, and the profile it lands in grants nothing. crun does
	// it in two steps, so a probe that does it in one reports "sandbox
	// available" on a machine where every job would fail. Measured on gcp1,
	// where the one-step probe said yes and crun said
	// "unshare `CLONE_NEWNS`: Operation not permitted".
	cmd := termuxexec.Command(self)
	cmd.Env = append(os.Environ(), probeEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	if err := cmd.Run(); err == nil {
		return true, ""
	}
	return false, diagnose()
}

// diagnose turns the refusal into the specific reason, because the three
// causes below need three different fixes and only one of them is common.
func diagnose() string {
	if v := sysctl("/proc/sys/user/max_user_namespaces"); v == "0" {
		return "user namespaces are disabled on this machine " +
			"(user.max_user_namespaces = 0).\n" +
			"    Enable them once, as root:\n" +
			"      sudo sysctl -w user.max_user_namespaces=15000\n" +
			"      echo 'user.max_user_namespaces=15000' | sudo tee /etc/sysctl.d/99-pipedpeer.conf"
	}
	if v := sysctl("/proc/sys/kernel/unprivileged_userns_clone"); v == "0" {
		return "unprivileged user namespaces are disabled " +
			"(kernel.unprivileged_userns_clone = 0).\n" +
			"    Enable them once, as root:\n" +
			"      sudo sysctl -w kernel.unprivileged_userns_clone=1\n" +
			"      echo 'kernel.unprivileged_userns_clone=1' | sudo tee /etc/sysctl.d/99-pipedpeer.conf"
	}
	if AppArmorRestricted() {
		// Reached even though creating the namespace succeeded: what is
		// denied is the privilege inside it.
		//
		// The narrow profile is verified: installed on an Ubuntu 26.04
		// machine that failed exactly here, after which crun built its
		// namespace and a job ran end to end. Note that the profile need only
		// name the pipedpeer binary - crun, which pipedpeer execs, inherits
		// it. The machine-wide sysctl is the documented alternative and has
		// not been tried, because the profile made it unnecessary.
		// Ubuntu 24.04 and later. There is no unprivileged way around this:
		// the kernel asks AppArmor, and AppArmor's answer depends on a
		// profile that only root can install. The narrow profile is offered
		// first because the sysctl turns the protection off machine-wide.
		exe, _ := termuxexec.Executable()
		if exe == "" {
			exe = "/usr/local/bin/pipedpeer"
		}
		cmds := ProfileCommands(exe)
		return fmt.Sprintf("this system restricts unprivileged user namespaces via AppArmor "+
			"(Ubuntu 24.04+; kernel.apparmor_restrict_unprivileged_userns = 1).\n"+
			"    One root action is needed, once. Either grant it to pipedpeer alone:\n"+
			"      %s\n"+
			"      %s\n"+
			"    or lift it machine-wide (weaker; affects every program):\n"+
			"      sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0\n"+
			"    `pipedpeer setup` installs the profile itself.",
			cmds[0], cmds[1])
	}
	return "the kernel refused to create a user namespace, and none of the " +
		"usual switches (user.max_user_namespaces, unprivileged_userns_clone, " +
		"apparmor_restrict_unprivileged_userns) explain it. Check your " +
		"container/seccomp policy."
}
