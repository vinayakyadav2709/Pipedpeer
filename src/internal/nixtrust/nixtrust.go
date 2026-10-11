// Package nixtrust is the root-owned helper that lets a multi-user nix trust
// this cluster's signing keys with no password prompt after setup, and the
// client that calls it (ADR-0018).
//
// Setup copies the pipedpeer binary to HelperPath, root-owned, and installs a
// sudoers rule letting the user run that one file as root without a password.
// Run from that file, the binary does nothing but Main: add or remove strictly
// validated pipedpeer keys in KeysFile, which nix.conf includes. Its arguments
// come from an unprivileged user and are treated as hostile.
package nixtrust

import (
	"bufio"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
)

const (
	// HelperPath is where setup installs the helper. Every parent must be
	// root-owned and writable by root alone (CheckRootOwned).
	HelperPath = "/usr/local/libexec/pipedpeer-nix-trust"
	// KeysFile is the only file the helper writes.
	KeysFile = "/etc/nix/pipedpeer-trusted-keys.conf"
	// SudoersFile holds the one rule that lets the user run the helper.
	SudoersFile = "/etc/sudoers.d/pipedpeer"
	// IncludeLine is what setup adds once to nix.conf. `!include` ignores a
	// missing file, so nix works before the first key is added.
	IncludeLine = "!include " + KeysFile

	keysSetting = "extra-trusted-public-keys"
	maxKeys     = 256
)

// keyRE is the exact shape nixsign.PublicKeyFor produces: a pipedpeer name
// over a 16-hex fingerprint, and 32 bytes of base64 (43 chars and one pad).
// Anything else - spaces, newlines, a second option, another name - is
// refused before the bytes are even decoded.
var keyRE = regexp.MustCompile(`^pipedpeer-[0-9a-f]{16}:[A-Za-z0-9+/]{43}=$`)

// ValidKey reports whether k is a pipedpeer nix public key whose name belongs
// to its key bytes.
func ValidKey(k string) error {
	if !keyRE.MatchString(k) {
		return fmt.Errorf("not a pipedpeer nix public key: %q", k)
	}
	name, b64, _ := strings.Cut(k, ":")
	raw, err := base64.StdEncoding.Strict().DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return fmt.Errorf("key %s: not 32 bytes of base64", name)
	}
	return nixsign.Verify(k, identity.Fingerprint(ed25519.PublicKey(raw)))
}

// FileContent is the whole of KeysFile for keys.
func FileContent(keys []string) string {
	s := "# Written by " + HelperPath + " (pipedpeer); do not edit.\n" +
		"# Signing keys of this machine's pipedpeer cluster.\n"
	if len(keys) > 0 {
		s += keysSetting + " = " + strings.Join(keys, " ") + "\n"
	}
	return s
}

// parseKeys reads the keys back from a KeysFile. The file is the helper's
// own, so a line or key it would not have written is dropped, with a note.
func parseKeys(body string) []string {
	var keys []string
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) != keysSetting {
			fmt.Fprintf(os.Stderr, "pipedpeer-nix-trust: dropping unexpected line in %s: %q\n", KeysFile, line)
			continue
		}
		for _, k := range strings.Fields(val) {
			if err := ValidKey(k); err != nil {
				fmt.Fprintf(os.Stderr, "pipedpeer-nix-trust: dropping from %s: %v\n", KeysFile, err)
				continue
			}
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// apply returns the key set after verb, deduplicated, in first-seen order.
func apply(have []string, verb string, keys []string) []string {
	next := slices.Clone(have)
	for _, k := range keys {
		i := slices.Index(next, k)
		switch {
		case verb == "add" && i < 0:
			next = append(next, k)
		case verb == "remove" && i >= 0:
			next = slices.Delete(next, i, i+1)
		}
	}
	return next
}

// Main is the helper: `add|remove <key>...`. Nothing else is accepted.
func Main(args []string) int {
	// sudo resets the environment, but a root daemon calls this directly.
	os.Setenv("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
	err := run(args, KeysFile, func() error {
		cmd := exec.Command("systemctl", "restart", "nix-daemon")
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		return cmd.Run()
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipedpeer-nix-trust:", err)
		return 1
	}
	return 0
}

func run(args []string, file string, restart func() error) error {
	if len(args) < 2 || (args[0] != "add" && args[0] != "remove") {
		return errors.New("usage: pipedpeer-nix-trust add|remove <key> [key ...]")
	}
	if len(args)-1 > maxKeys {
		return fmt.Errorf("at most %d keys at once", maxKeys)
	}
	for _, k := range args[1:] {
		if err := ValidKey(k); err != nil {
			return err
		}
	}
	body, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	have := parseKeys(string(body))
	next := apply(have, args[0], args[1:])
	if slices.Equal(have, next) && FileContent(next) == string(body) {
		return nil
	}
	if err := writeAtomic(file, FileContent(next)); err != nil {
		return err
	}
	if slices.Equal(have, next) {
		return nil // only the file's form changed; nix sees the same keys
	}
	if err := restart(); err != nil {
		return fmt.Errorf("keys written, but restarting nix-daemon failed: %w", err)
	}
	return nil
}

func writeAtomic(file, content string) error {
	tmp, err := os.CreateTemp(filepath.Dir(file), ".pipedpeer-trusted-keys-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // a no-op after the rename
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), file)
}

// IsHelper reports whether this process runs from the installed helper file,
// in which case main must call Main and nothing else: the sudoers rule lets
// the user run that file as root with any arguments.
func IsHelper() bool {
	self, err := os.Executable()
	if err != nil {
		return false
	}
	a, err := os.Stat(self)
	if err != nil {
		return false
	}
	b, err := os.Stat(HelperPath)
	return err == nil && os.SameFile(a, b)
}

// rootOnly fails unless fi is owned by root and writable by nobody else.
func rootOnly(path string, fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 {
		return fmt.Errorf("%s is not owned by root", path)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s is writable by a non-root user (%v)", path, fi.Mode().Perm())
	}
	return nil
}

// CheckRootOwned fails unless every existing directory above path, along the
// path as written and along its resolved form, is root-owned and writable by
// root alone, so no user can swap what sudo runs.
func CheckRootOwned(path string) error {
	dirs := []string{filepath.Dir(path)}
	if real, err := filepath.EvalSymlinks(filepath.Dir(path)); err == nil {
		dirs = append(dirs, real)
	}
	for _, d := range dirs {
		for ; ; d = filepath.Dir(d) {
			fi, err := os.Stat(d)
			if err == nil {
				if err := rootOnly(d, fi); err != nil {
					return err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if d == "/" {
				break
			}
		}
	}
	return nil
}

// Installed reports whether the helper file is in place and safe to point
// sudo at. It does not say whether sudo lets this user run it (Ready does).
func Installed() bool {
	fi, err := os.Lstat(HelperPath)
	if err != nil || !fi.Mode().IsRegular() {
		return false
	}
	if err := rootOnly(HelperPath, fi); err != nil {
		fmt.Fprintf(os.Stderr, "ignoring the nix key helper: %v\n", err)
		return false
	}
	if err := CheckRootOwned(HelperPath); err != nil {
		fmt.Fprintf(os.Stderr, "ignoring the nix key helper: %v\n", err)
		return false
	}
	return true
}

// Seams for tests.
var (
	geteuid = os.Geteuid
	execute = func(argv []string) ([]byte, error) {
		return exec.Command(argv[0], argv[1:]...).CombinedOutput()
	}
)

func argv(args ...string) []string {
	a := append([]string{HelperPath}, args...)
	if geteuid() != 0 {
		a = append([]string{"sudo", "-n"}, a...)
	}
	return a
}

// Ready reports whether the helper is installed and this user may run it
// without a password (`sudo -n -l` asks sudo without running anything).
func Ready() bool {
	if !Installed() {
		return false
	}
	if geteuid() == 0 {
		return true
	}
	_, err := execute([]string{"sudo", "-n", "-l", HelperPath, "add"})
	return err == nil
}

// Add has the helper trust keys, without a prompt. nix-daemon restarts if
// the set changed.
func Add(keys []string) error { return call("add", keys) }

// Remove has the helper stop trusting keys.
func Remove(keys []string) error { return call("remove", keys) }

func call(verb string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	a := argv(append([]string{verb}, keys...)...)
	if out, err := execute(a); err != nil {
		return fmt.Errorf("%s %s: %w: %s", HelperPath, verb, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// SudoersRule is the whole sudoers file for user.
func SudoersRule(user string) string {
	return user + " ALL=(root) NOPASSWD: " + HelperPath + " *\n"
}

// userRE limits what goes into the sudoers file to a plain login name.
var userRE = regexp.MustCompile(`^[a-z_][a-z0-9_.-]{0,31}$`)

// ValidUser refuses a login name that sudoers would read as more than a name.
func ValidUser(u string) error {
	if !userRE.MatchString(u) {
		return fmt.Errorf("user name %q cannot go into a sudoers rule", u)
	}
	return nil
}
