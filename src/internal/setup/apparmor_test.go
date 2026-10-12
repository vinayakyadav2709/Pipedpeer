package setup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/userns"
)

// fakeAppArmor points the AppArmor step at a fake root, a fake
// apparmor_parser that logs its arguments, and a root runner that runs the
// script with sh and no sudo. Real AppArmor needs Ubuntu 24.04+; this runs
// the real scripts everywhere else.
type fakeAA struct {
	root, log string
	scripts   []string
}

func newFakeAppArmor(t *testing.T, restricted, available, fresh bool) *fakeAA {
	t.Helper()
	f := &fakeAA{root: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(f.root, "etc/apparmor.d"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	f.log = filepath.Join(bin, "parser.log")
	parser := "#!/bin/sh\necho \"$@\" >> " + shq(f.log) + "\nexit ${FAKE_PARSER_EXIT:-0}\n"
	if err := os.WriteFile(filepath.Join(bin, "apparmor_parser"), []byte(parser), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	ar, ra, ua, pf, sp, cbr, rr := apparmorRoot, apparmorRestricted, usernsAvailable, usernsProbeFresh, selfPath, canBecomeRoot, runAsRoot
	t.Cleanup(func() {
		apparmorRoot, apparmorRestricted, usernsAvailable, usernsProbeFresh, selfPath, canBecomeRoot, runAsRoot = ar, ra, ua, pf, sp, cbr, rr
	})
	apparmorRoot = f.root
	apparmorRestricted = func() bool { return restricted }
	usernsAvailable = func() (bool, string) { return available, "denied" }
	usernsProbeFresh = func() (bool, string) { return fresh, "still denied" }
	selfPath = func() (string, error) { return "/opt/pp/pipedpeer", nil }
	canBecomeRoot = func() bool { return true }
	runAsRoot = func(script string) error {
		f.scripts = append(f.scripts, script)
		out, err := runShell(script)
		if err != nil {
			t.Logf("script failed: %v\n%s", err, out)
		}
		return err
	}
	return f
}

func runShell(script string) (string, error) {
	cmd := exec.Command("sh", "-c", script)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (f *fakeAA) parserCalls(t *testing.T) []string {
	b, err := os.ReadFile(f.log)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func (f *fakeAA) profile() string { return filepath.Join(f.root, userns.ProfileFile) }

func TestAppArmorStepSkipsMachinesThatDoNotNeedIt(t *testing.T) {
	for _, c := range []struct {
		name                  string
		restricted, available bool
	}{
		{"no AppArmor restriction", false, false},
		{"profile already working", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeAppArmor(t, c.restricted, c.available, true)
			if allowUserns(false) {
				t.Error("reported loading a profile")
			}
			if len(f.scripts) != 0 {
				t.Errorf("ran %d root scripts, want none", len(f.scripts))
			}
		})
	}
}

func TestAppArmorStepWritesLoadsAndIsIdempotent(t *testing.T) {
	f := newFakeAppArmor(t, true, false, true)
	want := userns.Profile("/opt/pp/pipedpeer")

	if !allowUserns(false) {
		t.Fatal("did not report the profile loaded")
	}
	got, err := os.ReadFile(f.profile())
	if err != nil || string(got) != want {
		t.Fatalf("profile = %q, %v; want %q", got, err, want)
	}
	if calls := f.parserCalls(t); len(calls) != 1 || calls[0] != "-r "+f.profile() {
		t.Fatalf("parser calls = %q", calls)
	}

	// Second run, identical profile: reloaded, not rewritten.
	if !allowUserns(false) {
		t.Fatal("second run did not report the profile loaded")
	}
	if strings.Contains(f.scripts[1], "cat >") {
		t.Errorf("rewrote an identical profile:\n%s", f.scripts[1])
	}
	if calls := f.parserCalls(t); len(calls) != 2 {
		t.Errorf("parser calls = %q, want a reload", calls)
	}

	// A profile for another path is replaced.
	if err := os.WriteFile(f.profile(), []byte(userns.Profile("/old/pipedpeer")), 0o644); err != nil {
		t.Fatal(err)
	}
	allowUserns(false)
	if got, _ := os.ReadFile(f.profile()); string(got) != want {
		t.Errorf("stale profile not replaced: %q", got)
	}
}

func TestAppArmorStepFailuresAreNotFatal(t *testing.T) {
	t.Run("parser refuses a new profile", func(t *testing.T) {
		f := newFakeAppArmor(t, true, false, true)
		t.Setenv("FAKE_PARSER_EXIT", "1")
		if allowUserns(false) {
			t.Error("reported success")
		}
		if _, err := os.Stat(f.profile()); !os.IsNotExist(err) {
			t.Errorf("a profile that would not load was left behind: %v", err)
		}
	})
	t.Run("fresh probe still denied", func(t *testing.T) {
		newFakeAppArmor(t, true, false, false)
		if allowUserns(false) {
			t.Error("reported success")
		}
	})
	t.Run("no root", func(t *testing.T) {
		f := newFakeAppArmor(t, true, false, true)
		canBecomeRoot = func() bool { return false }
		if allowUserns(false) || len(f.scripts) != 0 {
			t.Error("tried to install without root")
		}
	})
	t.Run("--no-install", func(t *testing.T) {
		f := newFakeAppArmor(t, true, false, true)
		if allowUserns(true) || len(f.scripts) != 0 {
			t.Error("installed under --no-install")
		}
	})
}

func TestUninstallRemovesTheProfile(t *testing.T) {
	f := newFakeAppArmor(t, true, false, true)
	if err := removeProfile(); err != nil || len(f.scripts) != 0 {
		t.Fatalf("no profile: err %v, %d root scripts; want neither", err, len(f.scripts))
	}

	allowUserns(false)
	if err := removeProfile(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.profile()); !os.IsNotExist(err) {
		t.Errorf("profile still there: %v", err)
	}
	calls := f.parserCalls(t)
	if last := calls[len(calls)-1]; last != "-R "+f.profile() {
		t.Errorf("last parser call = %q, want an unload", last)
	}

	// Not loaded: the file still goes.
	allowUserns(false)
	t.Setenv("FAKE_PARSER_EXIT", "1")
	if err := removeProfile(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.profile()); !os.IsNotExist(err) {
		t.Errorf("profile kept after a failed unload: %v", err)
	}
}
