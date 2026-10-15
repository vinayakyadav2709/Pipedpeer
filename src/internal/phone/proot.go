package phone

// The full-node route: Debian in proot-distro with single-user Nix, so the
// phone is an ordinary aarch64-linux node (whole jobs, pool chunks in a job's
// own closure, the public aarch64 binary cache) without root.
//
// Termux runs the pipedpeer binary as a launcher: every command is forwarded
// into the Debian (Launch), where /nix exists. proot-distro kills a session's
// processes when its command exits, so a command inside cannot leave the
// daemon behind; it asks the launcher, over an inherited pipe, to start it in
// a detached session instead (Detach).

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/term"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Distro is the proot-distro container the full-node route runs in.
const Distro = "debian"

// marker exists inside the Debian once setup finished there; its presence is
// what makes the Termux binary forward and what InProot looks for. A
// variable so tests can place it.
var marker = "/etc/pipedpeer-phone"

// ctlEnv names the fd (in the forwarded command) of the pipe to the launcher.
const ctlEnv = "PIPEDPEER_PHONE_CTL_FD"

// guestPath is PATH inside the Debian: Nix's single-user profile first.
const guestPath = "/root/.nix-profile/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// InProot reports whether this process runs inside the phone's proot Debian.
// Android has no /etc of its own to hold the marker, so Termux never matches.
func InProot() bool { return exists(marker) }

// prefix is Termux's $PREFIX.
func prefix() string {
	if p := os.Getenv("PREFIX"); p != "" {
		return p
	}
	return "/data/data/com.termux/files/usr"
}

// Rootfs is the Debian's root directory as Termux sees it: proot-distro 5
// keeps it under containers/, 4 under installed-rootfs/.
func Rootfs() string {
	base := filepath.Join(prefix(), "var", "lib", "proot-distro")
	v4 := filepath.Join(base, "installed-rootfs", Distro)
	if exists(v4) {
		return v4
	}
	return filepath.Join(base, "containers", Distro, "rootfs")
}

// ProotReady reports whether this Termux has the full-node route set up.
func ProotReady() bool { return IsTermux() && exists(filepath.Join(Rootfs(), marker)) }

// Launch runs this command inside the phone's Debian and exits with its code
// when the full-node route is set up; otherwise it returns and the command
// runs here. Called first thing in main.
func Launch(args []string) {
	if !ProotReady() {
		return
	}
	os.Exit(forward(args))
}

// loginArgs starts `proot-distro login` for the full node. --minimal, because
// the default mode binds the host's /proc/self/fd onto /dev/fd, which proot
// resolves once to proot-distro's own fds, so every <(...) in a Nix builder
// fails ("/dev/fd/63: No such file or directory", proot-distro 5.3) and no
// local Nix build works. --minimal keeps --link2symlink (Nix needs it) and
// leaves /dev as the host's, whose /dev/fd -> /proc/self/fd proot resolves per
// process. The binds below are the default mode's, minus /dev/fd and storage:
// /data/data/com.termux keeps this binary and the user's working directory at
// their paths, /apex /system /vendor /linkerconfig let Android's linker start
// this binary (a GOOS=android build), and the fake /proc files stand in for
// the ones Android hides. Lost: proot's SysV IPC emulation (POSIX shm via
// /dev/shm still works) and the fake kernel release. --minimal also sets no
// HOME or USER (Go then takes /sdcard as home on Android), so they are set.
func loginArgs() []string {
	args := []string{"login", "--minimal", "-e", "HOME=/root", "-e", "USER=root"}
	bind := func(host, guest string) {
		if exists(host) {
			args = append(args, "--bind="+host+":"+guest)
		}
	}
	for _, p := range []string{"/apex", "/odm", "/product", "/system", "/vendor", "/linkerconfig/ld.config.txt", "/data/data/com.termux"} {
		bind(p, p)
	}
	bind("/dev/urandom", "/dev/random")
	bind(filepath.Join(Rootfs(), "tmp"), "/dev/shm")
	sysdata := filepath.Join(filepath.Dir(Rootfs()), "sysdata")
	for name, guest := range map[string]string{
		"loadavg": "/proc/loadavg", "stat": "/proc/stat", "uptime": "/proc/uptime",
		"version": "/proc/version", "vmstat": "/proc/vmstat",
		"sysctl_entry_cap_last_cap":       "/proc/sys/kernel/cap_last_cap",
		"sysctl_inotify_max_user_watches": "/proc/sys/fs/inotify/max_user_watches",
		"sysctl_kernel_overflowuid":       "/proc/sys/kernel/overflowuid",
		"sysctl_kernel_overflowgid":       "/proc/sys/kernel/overflowgid",
	} {
		bind(filepath.Join(sysdata, name), guest)
	}
	return args
}

// forward runs `pipedpeer args` in the Debian, where this binary and the
// user's working directory keep their paths (loginArgs).
func forward(args []string) int {
	exe, err := termuxexec.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: cannot find this binary to run it in the phone's Debian: %v\n", err)
		return 1
	}
	r, w, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: %v\n", err)
		return 1
	}
	login := append(loginArgs(), "-e", ctlEnv+"=3", "-e", "PATH="+guestPath)
	// proot-distro starts from a clean environment; pipedpeer's own knobs
	// are meant for the command, so they go along.
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "PIPEDPEER_") && !strings.HasPrefix(kv, ctlEnv+"=") {
			login = append(login, "-e", kv)
		}
	}
	if wd, err := os.Getwd(); err == nil {
		login = append(login, "-w", wd)
	}
	login = append(login, Distro, "--", exe)
	cmd := termuxexec.Command("proot-distro", append(login, args...)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.ExtraFiles = []*os.File{w}
	// Ctrl-C reaches the whole foreground group, the command inside
	// included; the launcher stays to report that command's exit code.
	signal.Notify(make(chan os.Signal, 1), os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: cannot enter the phone's Debian (proot-distro login %s): %v\n", Distro, err)
		return 1
	}
	w.Close()
	var served sync.WaitGroup
	served.Add(1)
	go func() {
		defer served.Done()
		serveDetach(r)
	}()
	err = cmd.Wait()
	served.Wait()
	if err != nil && cmd.ProcessState == nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: %v\n", err)
		return 1
	}
	return cmd.ProcessState.ExitCode()
}

// serveDetach starts each argv the command inside sends (one JSON array per
// line) in its own detached session, which outlives that command.
func serveDetach(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var argv []string
		if err := json.Unmarshal(sc.Bytes(), &argv); err != nil || len(argv) == 0 {
			fmt.Fprintf(os.Stderr, "pipedpeer: bad detach request %q: %v\n", sc.Text(), err)
			continue
		}
		// proot-distro 5 has no --detach: --no-kill-on-exit keeps the session
		// up while the command runs, and its own session keeps it from dying
		// with this launcher. Its output goes to launcher.log.
		login := append(append(loginArgs(), "--no-kill-on-exit", "-e", "PATH="+guestPath, Distro, "--"), argv...)
		cmd := termuxexec.Command("proot-distro", login...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		logPath := filepath.Join(userdir.State(), "launcher.log")
		_ = os.MkdirAll(filepath.Dir(logPath), 0o700) // OpenFile reports the failure
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err == nil {
			cmd.Stdout, cmd.Stderr = f, f
		} else {
			fmt.Fprintf(os.Stderr, "pipedpeer: %s's output is lost: %v\n", argv[0], err)
		}
		err = cmd.Start()
		if f != nil {
			f.Close()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "pipedpeer: could not start %s in the phone's Debian: %v\n", argv[0], err)
			continue
		}
		cmd.Process.Release()
	}
}

// Detach asks the Termux launcher to run argv in a detached Debian session.
// Only a command forwarded by Launch has the pipe for that.
func Detach(argv []string) error {
	fd, err := strconv.Atoi(os.Getenv(ctlEnv))
	if err != nil {
		return errors.New("this command was not started from Termux, so it cannot start the daemon; run `pipedpeer start` in Termux")
	}
	b, err := json.Marshal(argv)
	if err != nil {
		return err
	}
	_, err = os.NewFile(uintptr(fd), "phone-launcher").Write(append(b, '\n'))
	return err
}

// Route says how this phone takes work, for setup and doctor; "" off a phone.
func Route() string {
	switch {
	case InProot():
		return "full node (Debian in proot with Nix)"
	case IsTermux():
		return "pool-only worker (Termux python)"
	}
	return ""
}

// ChunksOnlyNote is the doctor and daemon-start line for a full-node phone.
// Tried on an S20 FE (Play-Store Termux, proot 5.1.107.80): Termux's proot
// started inside the proot-distro session cannot start any program (its
// loader is embedded and started from memory, which the outer proot cannot
// follow), so the whole-job sandbox (SandboxShell) never runs there.
const ChunksOnlyNote = "phone: takes pool and torch chunks only; whole jobs and DDP ranks need a nested proot sandbox, which this Termux's proot can't start"

// SandboxNote is the doctor and log line for the phone sandbox.
const SandboxNote = "phone sandbox: proot filesystem view (job dir + /nix only), memory rlimit, low CPU priority; weaker than crun (no namespaces or cgroups without root)"

// sandboxProot is Termux's proot; a variable for tests.
var sandboxProot = func() string { return filepath.Join(prefix(), "bin", "proot") }

// skel is the empty root a sandboxed job's view starts from.
func skel() string { return filepath.Join(userdir.Data(), "phone-sandbox-root") }

// SandboxShell is the shell fragment that runs a job command (appended after
// it) in the phone sandbox: an address-space rlimit, nice 10 and a proot view
// that holds only the store, the job dir and what a process needs to start.
// quote is the caller's shell quoting.
//
// The proot is Termux's own (visible here under /data/data/com.termux):
// Debian's proot 5.1.0 segfaults under an outer proot, Termux's fork nests.
func SandboxShell(workDir string, memBytes int64, quote func(string) string) (string, error) {
	pr := sandboxProot()
	if _, err := os.Stat(pr); err != nil {
		return "", fmt.Errorf("no Termux proot at %s (in Termux: pkg install proot): %w", pr, err)
	}
	if err := os.MkdirAll(skel(), 0o755); err != nil {
		return "", err
	}
	argv := []string{"nice", "-n", "10", pr, "-r", skel(), "-w", workDir, "-b", workDir}
	for _, p := range []string{"/nix", "/dev", "/proc", "/sys", "/etc/resolv.conf", "/etc/hosts", "/etc/ssl"} {
		if exists(p) {
			argv = append(argv, "-b", p)
		}
	}
	q := make([]string, len(argv))
	for i, a := range argv {
		q[i] = quote(a)
	}
	s := "exec " + strings.Join(q, " ") + " "
	if memBytes > 0 {
		// ponytail: RLIMIT_AS counts address space, not memory. torch and
		// OpenBLAS reserve far more than they touch, so the limit is the
		// job's memory plus 2 GiB; a cgroup would be exact, Android gives none.
		s = fmt.Sprintf("ulimit -v %d && ", (memBytes+2<<30)/1024) + s
	}
	return s, nil
}

// hostEnv is the out-link of the pinned python stack a full-node phone runs
// other architectures' pool chunks with (their closures cannot run here).
func hostEnv() string { return filepath.Join(userdir.Data(), "phone-python") }

// hostEnvPkgs is that stack: the usual job imports, at the job pin.
// PIPEDPEER_PHONE_PYTHON_PKGS replaces the list (nixpkgs python attribute
// names), for a phone short of storage: torch is most of the 1.5 GB.
func hostEnvPkgs() string {
	if v := strings.TrimSpace(os.Getenv("PIPEDPEER_PHONE_PYTHON_PKGS")); v != "" {
		return v
	}
	return "numpy pandas scipy scikit-learn torch joblib cloudpickle"
}

// ProotSetup runs inside the Debian during setup: it builds the pinned python
// stack (from the public aarch64 cache) that other architectures' pool chunks
// run with, and says what it takes. A failed build leaves a node that takes
// same-arch chunks only.
func ProotSetup(w io.Writer, nixpkgs string) {
	term.OK(w, "this phone is a %s", Route())
	term.Warn(w, "%s", ChunksOnlyNote)
	pkgs := hostEnvPkgs()
	expr := fmt.Sprintf(`(builtins.getFlake "%s").legacyPackages.${builtins.currentSystem}.python3.withPackages (ps: with ps; [ %s ])`, nixpkgs, pkgs)
	fmt.Fprintf(w, "fetching python with %s for other machines' pool work (about 1.5 GB with torch, from the Nix cache)...\n", pkgs)
	out, err := runCmd("nix", "--extra-experimental-features", "nix-command flakes",
		"build", "--impure", "--out-link", hostEnv(), "--expr", expr)
	if err != nil {
		log.Warn().Err(err).Str("output", tail(out)).Msg("phone: building the pool python failed; other machines' pool chunks skip this phone")
		term.Warn(w, "could not build the pool python (%v); this phone takes whole jobs and its own architecture's pool work only", err)
		return
	}
	pyOnce = sync.Once{}
	term.OK(w, "pool python ready at %s", hostEnv())
}

// nixVersion is the Nix release installed in the Debian (the lab image's).
const nixVersion = "2.31.3"

// guestSetup installs Nix single-user in the Debian. proot gives no
// namespaces and no seccomp, hence nix.conf.
const guestSetup = `set -eu
export DEBIAN_FRONTEND=noninteractive USER=root HOME=/root
if [ ! -x /root/.nix-profile/bin/nix ]; then
	apt-get update -q
	apt-get install -y -q --no-install-recommends curl xz-utils ca-certificates
	mkdir -p /etc/nix /nix
	printf 'sandbox = false\nfilter-syscalls = false\nbuild-users-group =\nexperimental-features = nix-command flakes\n' > /etc/nix/nix.conf
	curl -fsSL https://releases.nixos.org/nix/nix-` + nixVersion + `/install | sh -s -- --no-daemon --no-channel-add --no-modify-profile
fi
/root/.nix-profile/bin/nix --version
echo "full-node phone; written by pipedpeer setup" > /etc/pipedpeer-phone
`

// runStep runs a setup command with its output to w; a variable so tests
// fake proot-distro, pkg and the Nix install.
var runStep = func(w io.Writer, name string, args ...string) error {
	cmd := termuxexec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = w, w
	return cmd.Run()
}

// SetupProot sets up the full-node route from Termux: proot-distro, its
// Debian, Nix inside it (which writes the marker that makes this binary
// forward). Idempotent; an error means the route is not usable.
func SetupProot(w io.Writer) error {
	step := func(what, name string, args ...string) error {
		fmt.Fprintf(w, "%s (%s %s), this can take a while...\n", what, name, strings.Join(args, " "))
		if err := runStep(w, name, args...); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		return nil
	}
	if runStep(io.Discard, "proot-distro", "list") != nil {
		if err := step("installing proot-distro", "pkg", "install", "-y", "proot-distro"); err != nil {
			return err
		}
	}
	if runStep(io.Discard, "proot-distro", "login", Distro, "--", "true") != nil {
		if err := step("installing Debian for the full node (~100 MB)", "proot-distro", "install", Distro); err != nil {
			return err
		}
	}
	if err := step("installing Nix in the Debian", "proot-distro", "login", Distro, "--", "/bin/sh", "-c", guestSetup); err != nil {
		return err
	}
	if !ProotReady() {
		return fmt.Errorf("setup in the Debian finished but %s is not there (proot-distro keeps its containers elsewhere?)", filepath.Join(Rootfs(), marker))
	}
	term.OK(w, "Debian with Nix ready; pipedpeer now runs there (%s)", Rootfs())
	return nil
}

// launch is Launch; a variable so tests see the forward without exiting.
var launch = Launch

// SetupTermux picks the route in Termux: the full node, then this same
// command reruns in the Debian (launch exits); else, with one log line why,
// the pool-only worker. stopDaemon stops a pool-only daemon from before,
// which would hold the port.
func SetupTermux(w io.Writer, args []string, stopDaemon func() error) error {
	err := SetupProot(w)
	if err == nil {
		if err := stopDaemon(); err != nil {
			log.Warn().Err(err).Msg("phone: could not stop the Termux pool-only daemon")
		}
		launch(args)
		return nil
	}
	log.Warn().Err(err).Msg("phone: full-node route (proot Debian + Nix) failed; falling back to a pool-only worker")
	term.Warn(w, "could not set up the full node (%v); this phone becomes a pool-only worker", err)
	return Setup(w)
}
