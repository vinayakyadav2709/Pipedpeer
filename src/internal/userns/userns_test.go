package userns

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The probe re-executes this binary to perform its second unshare, so the
// test binary has to honour the same hook main does.
func TestMain(m *testing.M) {
	Install()
	os.Exit(m.Run())
}

// TestAvailableMatchesTheKernel checks the probe against an independent
// answer from util-linux, so a probe that is wrong in either direction is
// caught rather than believed. A probe that wrongly reports "yes" makes jobs
// fail with crun's unreadable error; one that wrongly reports "no" silently
// drops the sandbox on a machine that could have had it.
func TestAvailableMatchesTheKernel(t *testing.T) {
	unshare, err := exec.LookPath("unshare")
	if err != nil {
		t.Skip("NOT VERIFIED: util-linux `unshare` is not installed, so there is " +
			"no independent answer to check the probe against")
	}
	// --mount matters: a machine can allow the user namespace and refuse the
	// privileges inside it, and only the second one decides whether a
	// sandbox can be built.
	want := exec.Command(unshare, "--user", "--map-root-user", "--mount", "true").Run() == nil

	got, why := Available()
	if got != want {
		t.Fatalf("Available() = %v, but `unshare --user --map-root-user` says %v (reason given: %s)", got, want, why)
	}
	if got {
		t.Log("user namespaces available: jobs run sandboxed")
		return
	}
	t.Logf("user namespaces refused; diagnosis:\n%s", why)
}

// TestDiagnosisNamesAFix keeps the message actionable. crun's own error is
// "unshare: Operation not permitted", which is what this exists to replace;
// a diagnosis that is merely a different way of saying "no" is no better.
func TestDiagnosisNamesAFix(t *testing.T) {
	ok, why := Available()
	if ok {
		t.Skip("NOT VERIFIED: user namespaces work here, so no diagnosis is produced. " +
			"This runs on a machine that restricts them (Ubuntu 24.04+, or a hardened sysctl).")
	}
	if !strings.Contains(why, "sudo") {
		t.Errorf("diagnosis names no command to run:\n%s", why)
	}
	if !strings.Contains(why, "\n") {
		t.Errorf("diagnosis is a single line, so it cannot be naming a fix:\n%s", why)
	}
}
