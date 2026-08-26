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
	"os/exec"
	"strings"
	"sync"
	"syscall"
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

func available() (bool, string) {
	self, err := os.Executable()
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
	cmd := exec.Command(self)
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
	if v := sysctl("/proc/sys/kernel/apparmor_restrict_unprivileged_userns"); v == "1" {
		// Reached even though creating the namespace succeeded: what is
		// denied is the privilege inside it.
		//
		// UNVERIFIED: the diagnosis is measured (gcp1 has the sysctl set and
		// fails exactly here), but neither remedy below has been run on a
		// restricted machine - applying them needs root on a box that is
		// serving other things. Both are the documented Ubuntu remedies;
		// treat the wording as advice, not as a tested procedure.
		// Ubuntu 24.04 and later. There is no unprivileged way around this:
		// the kernel asks AppArmor, and AppArmor's answer depends on a
		// profile that only root can install. The narrow profile is offered
		// first because the sysctl turns the protection off machine-wide.
		exe, _ := os.Executable()
		if exe == "" {
			exe = "/usr/local/bin/pipedpeer"
		}
		return fmt.Sprintf("this system restricts unprivileged user namespaces via AppArmor "+
			"(Ubuntu 24.04+; kernel.apparmor_restrict_unprivileged_userns = 1).\n"+
			"    One root action is needed, once. Either grant it to pipedpeer alone:\n"+
			"      printf 'abi <abi/4.0>,\\ninclude <tunables/global>\\nprofile pipedpeer %s flags=(unconfined) {\\n  userns,\\n}\\n' | sudo tee /etc/apparmor.d/pipedpeer\n"+
			"      sudo apparmor_parser -r /etc/apparmor.d/pipedpeer\n"+
			"    or lift it machine-wide (weaker; affects every program):\n"+
			"      sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0", exe)
	}
	return "the kernel refused to create a user namespace, and none of the " +
		"usual switches (user.max_user_namespaces, unprivileged_userns_clone, " +
		"apparmor_restrict_unprivileged_userns) explain it. Check your " +
		"container/seccomp policy."
}

func sysctl(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
