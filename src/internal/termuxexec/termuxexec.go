// Package termuxexec runs programs the way Play-Store Termux needs.
//
// Termux from Google Play targets a recent Android SDK, where an app may not
// execve a file in its own data directory. Its termux-exec preload starts
// every program as `/system/bin/linker64 <path> <args>` instead, which has
// three effects on a Go binary that does not go through libc:
//   - os.Args gets the program path inserted at index 1,
//   - os.Executable() is the linker, not this binary,
//   - its own exec.Command calls are denied (Go does not call the preloaded
//     execve), so they must start the linker too.
//
// Everything here is a no-op when this process was not started by the
// linker: F-Droid/GitHub Termux, inside proot, and every other machine.
// Call FixArgs first thing in main; use Command, CommandContext and
// Executable instead of the os/exec and os versions.
package termuxexec

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const linker = "/system/bin/linker64"

var (
	// underLinker reports whether /proc/self/exe is Android's linker. A var
	// so tests can fake it.
	underLinker = sync.OnceValue(func() bool {
		exe, err := os.Executable()
		return err == nil && strings.HasPrefix(filepath.Base(exe), "linker")
	})
	realExe string
)

// FixArgs removes the program path the linker inserted at os.Args[1] and
// remembers it as this binary's path.
func FixArgs() {
	if len(os.Args) > 1 && underLinker() {
		realExe = os.Args[1]
		os.Args = append(os.Args[:1:1], os.Args[2:]...)
	}
}

// Executable is os.Executable, but this binary rather than the linker when
// Termux started it through the linker.
func Executable() (string, error) {
	if realExe != "" {
		return realExe, nil
	}
	if underLinker() {
		if p := os.Getenv("TERMUX_EXEC__PROC_SELF_EXE"); p != "" {
			return filepath.Abs(p)
		}
		return "", fmt.Errorf("started by %s and neither FixArgs nor TERMUX_EXEC__PROC_SELF_EXE gave this binary's path", linker)
	}
	return os.Executable()
}

// Command is exec.Command, started through the linker when it has to be.
func Command(name string, arg ...string) *exec.Cmd {
	return wrap(exec.Command(name, arg...))
}

// CommandContext is exec.CommandContext, started through the linker when it
// has to be.
func CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	return wrap(exec.CommandContext(ctx, name, arg...))
}

func wrap(c *exec.Cmd) *exec.Cmd {
	if c.Err != nil || !underLinker() || !needsLinker(c.Path) {
		return c
	}
	argv, err := viaLinker(c.Path, c.Args[1:], 0)
	if err != nil {
		log.Printf("termuxexec: cannot start %s through %s, trying it directly: %v", c.Path, linker, err)
		return c
	}
	c.Path, c.Args = argv[0], argv
	return c
}

// viaLinker returns the linker argv that runs path with args: an ELF file
// directly, a script through its #! interpreter (as the kernel would).
func viaLinker(path string, args []string, depth int) ([]string, error) {
	if depth > 4 {
		return nil, fmt.Errorf("%s: too many nested #! interpreters", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	head, _ := bufio.NewReader(f).ReadSlice('\n')
	switch {
	case bytes.HasPrefix(head, []byte("\x7fELF")):
		return append([]string{linker, path}, args...), nil
	case bytes.HasPrefix(head, []byte("#!")):
		line := strings.TrimSpace(string(head[2:]))
		interp, opt, _ := strings.Cut(line, " ")
		if interp == "" {
			return nil, fmt.Errorf("%s: empty #! line", path)
		}
		interp = termuxPath(interp)
		iargs := []string{}
		if opt = strings.TrimSpace(opt); opt != "" {
			iargs = append(iargs, opt)
		}
		iargs = append(append(iargs, path), args...)
		if !needsLinker(interp) {
			return append([]string{interp}, iargs...), nil
		}
		return viaLinker(interp, iargs, depth+1)
	}
	return nil, fmt.Errorf("%s is neither an ELF file nor a #! script", path)
}

// needsLinker: only Android's own system files may be exec'd directly.
func needsLinker(p string) bool {
	for _, dir := range []string{"/system/", "/apex/", "/vendor/", "/product/"} {
		if strings.HasPrefix(p, dir) {
			return false
		}
	}
	return true
}

// termuxPath maps /bin/x and /usr/bin/x in a #! line to Termux's own copy, as
// termux-exec does (Android has no /usr; its /bin/sh is not bash).
func termuxPath(p string) string {
	for _, dir := range []string{"/usr/bin/", "/bin/"} {
		if rest, ok := strings.CutPrefix(p, dir); ok {
			prefix := os.Getenv("PREFIX")
			if prefix == "" {
				prefix = "/data/data/com.termux/files/usr"
			}
			return filepath.Join(prefix, "bin", rest)
		}
	}
	return p
}
