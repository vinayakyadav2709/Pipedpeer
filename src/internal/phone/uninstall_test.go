package phone

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeDpkg makes runCmd answer dpkg-query from *have and record every other
// call in *ran; sim is apt-get -s's output.
func fakeDpkg(have *[]string, ran *[]string, sim string) {
	runCmd = func(name string, args ...string) ([]byte, error) {
		cmd := name + " " + strings.Join(args, " ")
		switch {
		case name == "dpkg-query":
			var b strings.Builder
			for _, p := range *have {
				b.WriteString(p + " installed\n")
			}
			b.WriteString("gone not-installed\n")
			return []byte(b.String()), nil
		case strings.HasPrefix(cmd, "apt-get -s"):
			return []byte(sim), nil
		}
		*ran = append(*ran, cmd)
		return nil, nil
	}
}

func TestTrackedRecordsOnlyWhatSetupAddedAcrossReruns(t *testing.T) {
	fakePhone(t)
	have := []string{"bash", "python", "curl"} // the user's: python stays theirs
	var ran []string
	fakeDpkg(&have, &ran, "Inst python-numpy (2.4 Termux)\nInst curl [8.20] (8.21 Termux)\n")
	pkgs := []string{"python", "python-numpy", "curl"}
	if err := tracked(pkgs, func() error { have = append(have, "python-numpy", "libopenblas"); return nil }); err != nil {
		t.Fatal(err)
	}
	record(installed{Pip: []string{"cloudpickle"}})
	want := []string{"libopenblas", "python-numpy"}
	check := func(when string) {
		got, found := loadRecord()
		slices.Sort(got.Pkgs)
		if !found || !slices.Equal(got.Pkgs, want) || !slices.Equal(got.Pip, []string{"cloudpickle"}) {
			t.Errorf("%s: record = %+v, want packages %q (no upgrade, nothing of the user's) and cloudpickle", when, got, want)
		}
	}
	check("first setup")

	// Setup again (or after an update): everything is present now, apt has
	// nothing to do, and the record must still say these are ours.
	fakeDpkg(&have, &ran, "")
	if err := tracked(pkgs, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	check("second setup")
}

func TestInterruptedSetupRecordsFirst(t *testing.T) {
	fakePhone(t)
	have := []string{"bash"}
	var ran []string
	fakeDpkg(&have, &ran, "Inst proot-distro (5.3)\nInst proot (5.1)\n")
	runStep = func(w io.Writer, name string, args ...string) error {
		if len(args) > 0 && args[0] == "install" {
			return errors.New("killed") // pkg install or proot-distro install, half done
		}
		if name == "proot-distro" {
			return errors.New("not installed")
		}
		return nil
	}
	if err := SetupProot(io.Discard); err == nil {
		t.Fatal("want the failed install reported")
	}
	got, _ := loadRecord()
	slices.Sort(got.Pkgs)
	if !slices.Equal(got.Pkgs, []string{"proot", "proot-distro"}) {
		t.Errorf("record = %+v, want what apt was about to install", got)
	}

	// The package step passes, the container step is killed: the container
	// is recorded before it is created.
	runStep = func(w io.Writer, name string, args ...string) error {
		if name == "proot-distro" && args[0] != "list" {
			return errors.New("killed")
		}
		return nil
	}
	if err := SetupProot(io.Discard); err == nil {
		t.Fatal("want the failed install reported")
	}
	if got, _ := loadRecord(); !slices.Contains(got.Containers, Distro) || !slices.Contains(got.Paths, filepath.Dir(Rootfs())) {
		t.Errorf("record = %+v, want container %s and its dir", got, Distro)
	}
}

func TestUninstallAfterUpdateRemovesOldNames(t *testing.T) {
	fakePhone(t)
	pre := os.Getenv("PREFIX")
	oldRoot := filepath.Join(pre, "var", "lib", "proot-distro", "containers", "pp-old", "rootfs")
	oldData := filepath.Join(t.TempDir(), "old-data")
	for _, d := range []string{oldRoot, oldData} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	record(installed{Containers: []string{"pp-old"}, Paths: []string{oldData, filepath.Join(t.TempDir(), "never-made")}})
	var have, ran []string
	fakeDpkg(&have, &ran, "")
	var whats []string
	for _, s := range UninstallSteps() {
		whats = append(whats, s.What)
		if err := s.Do(); err != nil {
			t.Errorf("%s: %v", s.What, err)
		}
	}
	if !slices.Equal(ran, []string{"proot-distro remove pp-old"}) || exists(oldData) {
		t.Errorf("ran %q, old dir left %v; want the old container and dir removed", ran, exists(oldData))
	}
	if exists(filepath.Dir(recordPath())) {
		t.Errorf("steps %q: want the record deleted last", whats)
	}
}

func TestUninstallWithoutRecordSaysSo(t *testing.T) {
	fakePhone(t)
	have := []string{"proot-distro", "python", "bash"}
	var ran []string
	fakeDpkg(&have, &ran, "")
	var whats []string
	for _, s := range UninstallSteps() {
		whats = append(whats, s.What)
		if err := s.Do(); err != nil {
			t.Errorf("%s: %v", s.What, err)
		}
	}
	all := strings.Join(whats, "\n")
	if !strings.Contains(all, "no install record at "+recordPath()) || !strings.Contains(all, "present: proot-distro python ") || strings.Contains(all, "bash") {
		t.Errorf("steps:\n%s\nwant the missing record named with setup's packages that are present", all)
	}
	if len(ran) != 0 {
		t.Errorf("ran %q, want nothing removed without a record", ran)
	}
}

func TestUninstallStepsRemoveOnlyOurs(t *testing.T) {
	guest := fakePhone(t)
	pre := os.Getenv("PREFIX")
	legacy := filepath.Join(pre, "var", "lib", "proot-distro", "containers", "debian", "rootfs", marker)
	own := filepath.Join(pre, "opt", "pipedpeer-proot")
	ptmp := filepath.Join(pre, "tmp", "proot-1-x")
	for _, f := range []string{guest, legacy, filepath.Join(own, "bin", "proot"), ptmp} {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	record(installed{Pkgs: []string{"proot-distro", "proot", "gone"}})
	have := []string{"proot-distro", "python"}
	var ran []string
	fakeDpkg(&have, &ran, "Purg proot-distro [5.3.0]\n")

	var whats []string
	for _, s := range UninstallSteps() {
		whats = append(whats, s.What)
		if strings.HasPrefix(s.What, "stop") {
			continue // forward needs a real proot-distro
		}
		if err := s.Do(); err != nil {
			t.Errorf("%s: %v", s.What, err)
		}
	}
	all := strings.Join(whats, "\n")
	for _, want := range []string{"stop the daemon in the pipedpeer container", "delete the proot-distro container pipedpeer", "keep the proot-distro container debian", "delete " + own, "Termux packages setup installed: proot-distro proot gone", "proot's 1 temp dirs"} {
		if !strings.Contains(all, want) {
			t.Errorf("steps:\n%s\nwant one with %q", all, want)
		}
	}
	if !slices.Equal(ran, []string{"proot-distro remove pipedpeer", "apt-get purge -y proot-distro"}) {
		t.Errorf("ran %q, want the container and our still-installed package removed, debian and python kept", ran)
	}
	if exists(own) || exists(ptmp) {
		t.Error("own proot dir or proot's temp dir still there")
	}
	if !exists(filepath.Dir(legacy)) {
		t.Error("the user's debian container (and so proot-distro's data) must stay")
	}
}

func TestRemovePkgsKeepsAllWhenUsersWouldGo(t *testing.T) {
	fakePhone(t)
	have := []string{"python", "python-numpy"}
	var ran []string
	fakeDpkg(&have, &ran, "Purg python [3.13]\nPurg python-numpy [2]\nPurg users-tool [1]\n")
	err := removePkgs([]string{"python", "python-numpy"})
	if err == nil || !strings.Contains(err.Error(), "users-tool") || len(ran) != 0 {
		t.Errorf("err %v, ran %q: want nothing purged and the user's package named", err, ran)
	}
}

func TestPhantomNoteOverWifi(t *testing.T) {
	fakePhone(t)
	oldAdb, oldLook := adb, lookAdb
	t.Cleanup(func() { adb, lookAdb = oldAdb, oldLook })
	lookAdb = func() (string, error) { return "/bin/adb", nil }
	var calls []string
	adb = func(_ io.Writer, args ...string) error { calls = append(calls, strings.Join(args, " ")); return nil }

	var out bytes.Buffer
	PhantomNote(&out, bufio.NewReader(strings.NewReader("y\n37001\n123456\n41002\n")), false, true)
	want := []string{
		"pair 127.0.0.1:37001 123456",
		"connect 127.0.0.1:41002",
		"-s 127.0.0.1:41002 shell device_config delete activity_manager max_phantom_processes",
		"-s 127.0.0.1:41002 shell settings put global settings_enable_monitor_phantom_procs true",
		"-s 127.0.0.1:41002 shell device_config set_sync_disabled_for_tests none",
		"disconnect 127.0.0.1:41002",
	}
	if !slices.Equal(calls, want) {
		t.Errorf("adb calls %q\nwant %q", calls, want)
	}
	if !strings.Contains(out.String(), "adb shell settings put global settings_enable_monitor_phantom_procs true") {
		t.Errorf("output %q: want the revert lines printed", out.String())
	}

	calls = nil
	PhantomNote(io.Discard, bufio.NewReader(strings.NewReader("")), false, false)
	if len(calls) != 0 {
		t.Errorf("-y: ran adb %q, want only the lines printed", calls)
	}
}
