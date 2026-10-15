// Package service runs the daemon as a systemd user unit, so it comes back
// after a reboot (Finding 14) and `start`/`stop` go through systemd instead
// of a pid file.
//
// DECIDED: hand-rolled unit file + `systemctl --user`; coreos/go-systemd
// covers D-Bus and socket activation, not writing unit files.
package service

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
)

// Name is the unit's name.
const Name = "pipedpeer.service"

// systemctl runs `systemctl --user args...`; a variable so tests run without systemd.
var systemctl = func(args ...string) (string, error) {
	out, err := exec.Command("systemctl", append([]string{"--user"}, args...)...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// loginctl runs `loginctl args...`; a variable for tests.
var loginctl = func(args ...string) (string, error) {
	out, err := exec.Command("loginctl", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// UnitPath is ~/.config/systemd/user/pipedpeer.service.
func UnitPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "pipedpeer: no home directory (%v); unit path relative to the current directory\n", err)
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "systemd", "user", Name)
}

// Unit is the unit file text. path is the PATH the daemon gets: systemd's
// user manager starts with a bare one, and the daemon needs nix and crun.
// Delegate=yes gives the daemon its own cgroup subtree so job memory limits
// are enforced (what the systemd-run scope did before).
func Unit(binary, path string) string {
	return "[Unit]\n" +
		"Description=pipedpeer daemon\n" +
		"After=network-online.target\n" +
		"Wants=network-online.target\n" +
		"\n" +
		"[Service]\n" +
		"ExecStart=" + binary + " __daemon__\n" +
		"Environment=PATH=" + path + "\n" +
		"Restart=on-failure\n" +
		"RestartSec=5\n" +
		"Delegate=yes\n" +
		"\n" +
		"[Install]\n" +
		"WantedBy=default.target\n"
}

// Supported reports whether a systemd user manager answers here.
func Supported() bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	_, err := systemctl("show-environment")
	return err == nil
}

// Installed reports whether the unit file exists.
func Installed() bool {
	_, err := os.Stat(UnitPath())
	return err == nil
}

// Install writes the unit (pointing at this binary) and reloads systemd;
// enable also turns on start at boot.
func Install(enable bool) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding this binary for the service: %w", err)
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}
	p := UnitPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(Unit(exe, os.Getenv("PATH"))), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", p, err)
	}
	if out, err := systemctl("daemon-reload"); err != nil {
		return fmt.Errorf("systemctl --user daemon-reload: %v (%s)", err, out)
	}
	if enable {
		return Enable(true)
	}
	return nil
}

// Enable turns start at boot on or off. On also asks for lingering, without
// which a user unit starts only at login; a refusal is a warning, not an error.
func Enable(on bool) error {
	verb := "disable"
	if on {
		verb = "enable"
	}
	if out, err := systemctl(verb, Name); err != nil {
		return fmt.Errorf("systemctl --user %s %s: %v (%s)", verb, Name, err, out)
	}
	if !on {
		return nil
	}
	login := os.Getenv("USER")
	if u, err := user.Current(); err == nil {
		login = u.Username
	}
	if out, err := loginctl("enable-linger", login); err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: could not enable lingering for %s (%v: %s); "+
			"the daemon will start at login, not at boot. Fix: sudo loginctl enable-linger %s\n", login, err, out, login)
	}
	return nil
}

func run(verb string) error {
	if out, err := systemctl(verb, Name); err != nil {
		return fmt.Errorf("systemctl --user %s %s: %v (%s)", verb, Name, err, out)
	}
	return nil
}

// Start starts the unit.
func Start() error { return run("start") }

// Stop stops the unit.
func Stop() error { return run("stop") }

// Restart restarts the unit.
func Restart() error { return run("restart") }

// Active reports whether the unit is running and its main pid (0 when not).
func Active() (bool, int) {
	out, err := systemctl("show", "-p", "MainPID", "--value", Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: systemctl --user show %s: %v (%s)\n", Name, err, out)
		return false, 0
	}
	var pid int
	if _, err := fmt.Sscan(out, &pid); err != nil || pid <= 0 {
		return false, 0
	}
	return true, pid
}

// Remove stops and disables the unit and deletes its file.
func Remove() error {
	if !Installed() {
		return nil
	}
	if out, err := systemctl("disable", "--now", Name); err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: systemctl --user disable --now %s: %v (%s); removing the file anyway\n", Name, err, out)
	}
	if err := os.Remove(UnitPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %w", UnitPath(), err)
	}
	if out, err := systemctl("daemon-reload"); err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: systemctl --user daemon-reload: %v (%s)\n", err, out)
	}
	return nil
}
