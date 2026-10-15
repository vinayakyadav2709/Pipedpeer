package termuxexec

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func fakeLinker(t *testing.T, on bool) {
	old, oldExe := underLinker, realExe
	underLinker = func() bool { return on }
	t.Cleanup(func() { underLinker, realExe = old, oldExe })
}

func TestFixArgsUnderLinker(t *testing.T) {
	fakeLinker(t, true)
	old := os.Args
	t.Cleanup(func() { os.Args = old })
	os.Args = []string{"pipedpeer", "/data/data/com.termux/files/usr/bin/pipedpeer", "run", "x.py"}
	FixArgs()
	if want := []string{"pipedpeer", "run", "x.py"}; !reflect.DeepEqual(os.Args, want) {
		t.Fatalf("os.Args = %q, want %q", os.Args, want)
	}
	if exe, err := Executable(); err != nil || exe != "/data/data/com.termux/files/usr/bin/pipedpeer" {
		t.Fatalf("Executable() = %q, %v", exe, err)
	}
}

func TestNoLinkerLeavesEverything(t *testing.T) {
	fakeLinker(t, false)
	old := os.Args
	t.Cleanup(func() { os.Args = old })
	os.Args = []string{"pipedpeer", "run", "x.py"}
	FixArgs()
	if len(os.Args) != 3 {
		t.Fatalf("os.Args changed without the linker: %q", os.Args)
	}
	c := Command("/data/x", "a")
	if c.Path != "/data/x" || !reflect.DeepEqual(c.Args, []string{"/data/x", "a"}) {
		t.Fatalf("Command rewrote without the linker: %q %q", c.Path, c.Args)
	}
}

func TestViaLinker(t *testing.T) {
	dir := t.TempDir()
	elf := filepath.Join(dir, "bash")
	os.WriteFile(elf, []byte("\x7fELF\x02\x01\x01"), 0o755)
	script := filepath.Join(dir, "proot-distro")
	os.WriteFile(script, []byte("#!"+elf+"\necho hi\n"), 0o755)
	sys := filepath.Join(dir, "sys.sh")
	os.WriteFile(sys, []byte("#!/system/bin/sh -e\n"), 0o755)
	t.Setenv("PREFIX", dir)

	cases := []struct {
		path string
		want []string
	}{
		{elf, []string{linker, elf, "x"}},
		{script, []string{linker, elf, script, "x"}},
		{sys, []string{"/system/bin/sh", "-e", sys, "x"}},
	}
	for _, c := range cases {
		got, err := viaLinker(c.path, []string{"x"}, 0)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("viaLinker(%s) = %q, %v; want %q", filepath.Base(c.path), got, err, c.want)
		}
	}
	if got := termuxPath("/usr/bin/env"); got != filepath.Join(dir, "bin", "env") {
		t.Errorf("termuxPath(/usr/bin/env) = %q", got)
	}
	if _, err := viaLinker(filepath.Join(dir, "missing"), nil, 0); err == nil {
		t.Error("viaLinker on a missing file: no error")
	}
}

// TestNoDirectExec keeps every program start going through this package, so
// Play-Store Termux can run it (see the package doc). A lint, like
// tmpfs_guard_test.go: the failure is "permission denied" on phones only.
func TestNoDirectExec(t *testing.T) {
	banned := regexp.MustCompile(`(^|[^\w.])(exec\.Command(Context)?\(|os\.Executable\(\))`)
	var offenders []string
	err := filepath.Walk("../..", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "vendor" || info.Name() == "termuxexec" || (strings.HasPrefix(info.Name(), ".") && info.Name() != ".." && info.Name() != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(body), "\n") {
			code, _, _ := strings.Cut(line, "//")
			if banned.MatchString(code) {
				offenders = append(offenders, path+": "+strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Errorf("use termuxexec.Command/CommandContext/Executable instead:\n  %s", strings.Join(offenders, "\n  "))
	}
}
