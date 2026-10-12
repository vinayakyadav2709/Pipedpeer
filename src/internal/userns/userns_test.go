package userns

import (
	"os"
	"os/exec"
	"path/filepath"
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
//
// It used to call Available() and skip whenever namespaces worked, which is
// every machine here, so the assertion never ran anywhere. The reasons are
// read from /proc, so pointing that read at a directory we control reaches
// all four branches - including the AppArmor one, which otherwise needs an
// Ubuntu 24.04+ machine to exercise at all.
func TestDiagnosisNamesAFix(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
		wants []string
	}{
		{
			name:  "namespaces switched off entirely",
			files: map[string]string{"proc/sys/user/max_user_namespaces": "0"},
			wants: []string{"sysctl -w user.max_user_namespaces=15000", "max_user_namespaces = 0"},
		},
		{
			name:  "unprivileged clone denied",
			files: map[string]string{"proc/sys/kernel/unprivileged_userns_clone": "0"},
			wants: []string{"sysctl -w kernel.unprivileged_userns_clone=1"},
		},
		{
			name:  "AppArmor confines the new namespace",
			files: map[string]string{"proc/sys/kernel/apparmor_restrict_unprivileged_userns": "1"},
			// Both routes, because the narrow one is the one to prefer and
			// the machine-wide one is the fallback; naming only the sysctl
			// would be advice to weaken the whole machine.
			wants: []string{"/etc/apparmor.d/pipedpeer", "apparmor_parser -r", "sysctl -w kernel.apparmor_restrict_unprivileged_userns=0"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for rel, body := range tc.files {
				full := filepath.Join(dir, rel)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(body+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			old := sysctlRoot
			sysctlRoot = dir
			defer func() { sysctlRoot = old }()

			why := diagnose()
			if !strings.Contains(why, "sudo") {
				t.Errorf("diagnosis names no command to run:\n%s", why)
			}
			if !strings.Contains(why, "\n") {
				t.Errorf("diagnosis is a single line, so it cannot be naming a fix:\n%s", why)
			}
			for _, want := range tc.wants {
				if !strings.Contains(why, want) {
					t.Errorf("diagnosis does not mention %q:\n%s", want, why)
				}
			}
		})
	}

	// No switch explains it: still has to say so rather than invent a fix.
	t.Run("no known cause", func(t *testing.T) {
		old := sysctlRoot
		sysctlRoot = t.TempDir()
		defer func() { sysctlRoot = old }()
		why := diagnose()
		if strings.Contains(why, "sudo") {
			t.Errorf("offers a fix for a cause it did not find:\n%s", why)
		}
		if !strings.Contains(why, "seccomp") {
			t.Errorf("does not point anywhere to look next:\n%s", why)
		}
	})
}
