package setup

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/service"
	"github.com/pipedpeer/pipedpeer/internal/term"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

type prereq struct {
	name    string
	check   func() bool
	install func() error
}

type result struct {
	name   string
	status string
}

func binaryCheck(name string) func() bool {
	return func() bool {
		_, err := exec.LookPath(name)
		return err == nil
	}
}

// nixInstalled is binaryCheck("nix") plus the installer locations, because
// PATH is not where nix is - it is where the installer's profile script puts
// it, and nothing that is not a login shell runs that script. A systemd unit,
// a cron job, `ssh host command` and `docker exec` all see a machine with a
// perfectly good /nix/var/nix/profiles/default/bin/nix and an empty PATH.
// Reporting "nix missing" there sends the user to install a second nix over
// the one they have.
func nixInstalled() bool {
	_, err := nixstore.SystemNix()
	return err == nil
}

// storeDir is where nix keeps the store. NIX_STORE_DIR overrides it for the
// unusual setups that relocate it.
func storeDir() string {
	if dir := os.Getenv("NIX_STORE_DIR"); dir != "" {
		return dir
	}
	return "/nix/store"
}

// nixUsable reports whether nix can actually build something. The binary being
// on PATH is not enough: packaged builds (Fedora's nix-core RPM, for one)
// install it without ever creating the store, so `nix build` fails on the
// first real job while setup has already reported nix as fine.
func nixUsable() bool {
	// A private store counts: it carries its own nix, and nixstore.Cmd runs
	// it with the store bound at /nix.
	if nixstore.Private() {
		return true
	}
	if !nixInstalled() {
		return false
	}
	info, err := os.Stat(storeDir())
	return err == nil && info.IsDir()
}

// createNixStore makes the store directory owned by the invoking user. This is
// the single-user layout, which is all the daemon needs — it builds as
// whichever user runs it, not through nix-daemon.
func createNixStore() error {
	fmt.Printf("    → Creating the nix store at %s...\n", storeDir())

	root := filepath.Dir(storeDir())

	// No privilege needed when the root already exists and is ours (or when
	// NIX_STORE_DIR points somewhere user-writable).
	if err := os.MkdirAll(storeDir(), 0o755); err == nil {
		return nil
	}

	argv := []string{"install", "-d", "-m", "0755"}
	if os.Geteuid() != 0 {
		u, err := user.Current()
		if err != nil {
			return fmt.Errorf("looking up the current user: %w", err)
		}
		argv = append(argv, "-o", u.Uid, "-g", u.Gid)
		argv = append([]string{"sudo"}, argv...)
	}
	// Root needs privilege; once it is owned by the user, the store itself
	// does not. Creating only the root is not enough — nixUsable checks the
	// store dir, so setup would report nix missing forever and loop.
	argv = append(argv, root)

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not create %s: %w", root, err)
	}

	if err := os.MkdirAll(storeDir(), 0o755); err != nil {
		return fmt.Errorf("could not create %s: %w", storeDir(), err)
	}
	return nil
}

// trustStep is what trusting the cluster's keys needs on this machine.
type trustStep int

const (
	trustNotNeeded trustStep = iota // nix takes the keys with each import, or checks no signatures
	trustDone                       // nix.conf already lists every cluster key
	trustNoKeys                     // nix.conf decides, and the daemon has recorded no keys yet
	trustApply                      // nix.conf decides, and lacks some keys
)

// Seams for tests: what system nix trusts, the key helper, and the one
// command run as root.
var (
	systemUntrusted = nixsign.SystemUntrusted
	helperReady     = nixtrust.Ready
	helperAdd       = nixtrust.Add
	runAsRoot       = func(script string) error {
		argv := []string{"sh", "-c", script}
		if os.Geteuid() != 0 {
			argv = append([]string{"sudo"}, argv...)
		}
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		return cmd.Run()
	}
)

// clusterKeys are the keys the daemon collected from token-proven peers
// (plus its own), rewritten every poll.
func clusterKeys() ([]string, error) {
	body, err := os.ReadFile(nixsign.TrustedKeysFile())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return strings.Fields(string(body)), nil
}

// decideTrust says what system nix needs for keys.
//
// A private store and a single-user nix take the keys from each import
// (narpack.Import passes extra-trusted-public-keys), and a machine still
// carrying require-sigs = false accepts anything - left alone, because turning
// the check back on is the machine owner's decision. Only a multi-user
// nix-daemon, which ignores per-command keys from untrusted users, needs them
// in nix.conf.
func decideTrust(keys []string) (trustStep, []string, error) {
	missing, applies, err := systemUntrusted(keys)
	switch {
	case err != nil:
		return trustApply, nil, fmt.Errorf("reading what nix trusts: %w", err)
	case !applies:
		return trustNotNeeded, nil, nil
	case len(keys) == 0:
		return trustNoKeys, nil, nil
	case len(missing) == 0:
		return trustDone, nil, nil
	}
	return trustApply, missing, nil
}

// acceptsPeerClosures reports whether this node can import a peer's closure,
// now and as peers join.
//
// A multi-user nix-daemon refuses a store path no trusted key signed - "lacks
// a signature by a trusted key". Closures are signed and travel in a form that
// keeps the signature, so what such a machine needs is to trust this
// cluster's keys, and the key helper so the daemon can add the keys of peers
// that join later.
func acceptsPeerClosures() bool {
	keys, err := clusterKeys()
	if err != nil {
		fmt.Printf("    could not read %s: %v\n", nixsign.TrustedKeysFile(), err)
		return false
	}
	step, _, err := decideTrust(keys)
	if err != nil {
		fmt.Printf("    %v\n", err)
		return false
	}
	return step == trustNotNeeded || (step != trustApply && helperReady())
}

// trustClusterKeys is setup's nix-imports step: install the key helper on a
// multi-user nix and add the keys known so far. With none known yet there is
// nothing to fail about: the daemon adds them through the helper as peers
// join.
func trustClusterKeys() error {
	keys, err := clusterKeys()
	if err != nil {
		return err
	}
	step, missing, err := decideTrust(keys)
	if err != nil || step == trustNotNeeded {
		return err
	}
	return applyTrust(missing)
}

// Trust makes system nix accept the cluster's keys if it must be told, and
// says in one line what it did. It is `pipedpeer trust`.
func Trust() (string, error) {
	keys, err := clusterKeys()
	if err != nil {
		return "", err
	}
	step, missing, err := decideTrust(keys)
	switch {
	case err != nil:
		return "", err
	case step == trustNotNeeded:
		return "nothing to do: this nix takes the cluster's keys with each import " +
			"(private store, single-user nix, or signature checking off)", nil
	}
	if err := applyTrust(missing); err != nil {
		return "", err
	}
	switch step {
	case trustNoKeys:
		return "no cluster keys known yet; the daemon adds them through the key helper as peers join", nil
	case trustDone:
		return fmt.Sprintf("nothing to do: system nix already trusts all %d cluster key(s)", len(keys)), nil
	}
	return fmt.Sprintf("system nix now trusts %d more cluster key(s)", len(missing)), nil
}

// TrustAfterJoin is join's last step. On a multi-user nix without the key
// helper it installs it (one sudo prompt), so the daemon can add the keys of
// the peers just met, and of any that join later, by itself.
func TrustAfterJoin() error {
	keys, err := clusterKeys()
	if err != nil {
		return err
	}
	step, missing, err := decideTrust(keys)
	if err != nil {
		return err
	}
	if step == trustNotNeeded || (step != trustApply && helperReady()) {
		return nil
	}
	fmt.Println("this machine's nix accepts only the signing keys listed in nix.conf; " +
		"installing pipedpeer's key helper so the daemon can add the cluster's (sudo):")
	return applyTrust(missing)
}

// applyTrust has the key helper add keys, installing it first (the one sudo
// prompt) when sudo does not already let this user run it.
//
// Trust this cluster's keys. Do not switch the check off.
//
// The old repair appended `require-sigs = false`, which stops nix verifying
// signatures for EVERYTHING on the machine - every channel, every
// substituter, every user - so that pipedpeer could import one unsigned peer
// closure. That is a system-wide reduction bought for one program's
// convenience, and it outlives the program. It was needed only while
// `nix-store --export`, which drops signatures, carried every transfer;
// closures now travel as a small binary cache that keeps them.
//
// So the helper writes one setting: these specific keys are trusted, and
// everything else is refused exactly as strictly as before.
func applyTrust(keys []string) error {
	if !helperReady() {
		if err := installHelper(); err != nil {
			return err
		}
	}
	if len(keys) == 0 {
		return nil
	}
	fmt.Printf("    → Trusting %d cluster signing key(s); signature checking stays ON for everything else.\n", len(keys))
	return helperAdd(keys)
}

// nixConfTarget is the file setup adds the helper's include line to.
// Determinate marks /etc/nix/nix.conf "do not modify" and includes
// nix.custom.conf for local changes; plain multi-user installs edit nix.conf.
func nixConfTarget() string {
	if _, err := os.Stat("/etc/nix/nix.custom.conf"); err == nil {
		return "/etc/nix/nix.custom.conf"
	}
	return "/etc/nix/nix.conf"
}

// installHelper puts the key helper, its sudoers rule and the include line in
// place, as root (ADR-0018).
func installHelper() error {
	src, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding this binary to install as the key helper: %w", err)
	}
	if err := nixtrust.CheckRootOwned(nixtrust.HelperPath); err != nil {
		return fmt.Errorf("not installing the nix key helper: %w", err)
	}
	login := ""
	if geteuid() != 0 {
		u, err := user.Current()
		if err != nil {
			return fmt.Errorf("looking up the current user: %w", err)
		}
		if err := nixtrust.ValidUser(u.Username); err != nil {
			return err
		}
		login = u.Username
	}
	target := nixConfTarget()
	fmt.Println("    → Installing pipedpeer's nix key helper, so the daemon can trust")
	fmt.Println("      new peers' keys without a prompt:")
	fmt.Printf("      %s (root-owned copy of this binary)\n", nixtrust.HelperPath)
	if login != "" {
		fmt.Printf("      %s (lets %s run only that helper)\n", nixtrust.SudoersFile, login)
	}
	fmt.Printf("      one include line in %s\n", target)
	if err := runAsRoot(helperInstallScript(src, login, target)); err != nil {
		return fmt.Errorf("could not install the nix key helper: %w", err)
	}
	return nil
}

// shq quotes s for sh.
func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// helperInstallScript is what installHelper runs as root, kept apart so what
// a user's system gets can be asserted without changing it. With login empty
// (setup runs as root) no sudoers rule is needed or written. The sudoers rule
// is checked with visudo before it is moved into place: a broken file there
// would lock the machine out of sudo.
func helperInstallScript(src, login, target string) string {
	dir := filepath.Dir(nixtrust.HelperPath)
	staged := filepath.Join(dir, ".pipedpeer-nix-trust.new")
	lines := []string{
		"set -eu",
		"install -d -o root -g root -m 0755 " + shq(dir),
		"install -o root -g root -m 0755 " + shq(src) + " " + shq(staged),
		"mv -f " + shq(staged) + " " + shq(nixtrust.HelperPath),
	}
	if login != "" {
		lines = append(lines,
			// sudo skips files in sudoers.d whose name holds a dot.
			"tmp=$(mktemp "+shq(filepath.Dir(nixtrust.SudoersFile))+"/.pipedpeer.XXXXXX)",
			`trap 'rm -f "$tmp"' EXIT`,
			"printf '%s' "+shq(nixtrust.SudoersRule(login))+` > "$tmp"`,
			`chmod 0440 "$tmp"`,
			`visudo -cf "$tmp" >/dev/null`,
			`mv -f "$tmp" `+shq(nixtrust.SudoersFile),
		)
	}
	lines = append(lines,
		"grep -qxF "+shq(nixtrust.IncludeLine)+" "+shq(target)+" 2>/dev/null || "+
			"printf '\\n%s\\n' "+shq(nixtrust.IncludeLine)+" >> "+shq(target),
	)
	return strings.Join(lines, "\n") + "\n"
}

// helperUninstallScript undoes helperInstallScript and drops the keys it
// trusted; nix-daemon restarts so it stops trusting them.
func helperUninstallScript() string {
	inc := shq(nixtrust.IncludeLine)
	return strings.Join([]string{
		"set -eu",
		"rm -f " + shq(nixtrust.SudoersFile) + " " + shq(nixtrust.HelperPath) + " " + shq(nixtrust.KeysFile),
		"for f in /etc/nix/nix.conf /etc/nix/nix.custom.conf; do",
		"  if grep -qxF " + inc + ` "$f" 2>/dev/null; then`,
		"    grep -vxF " + inc + ` "$f" > "$f.pipedpeer-new" || true`,
		`    cat "$f.pipedpeer-new" > "$f"`,
		`    rm -f "$f.pipedpeer-new"`,
		"  fi",
		"done",
		"systemctl try-restart nix-daemon",
	}, "\n") + "\n"
}

// Uninstall removes what setup installed as root: the AppArmor profile, the
// key helper, its sudoers rule, the include line and the keys file. It is
// `pipedpeer setup --uninstall`.
func Uninstall() error {
	profileErr := removeProfile()
	fmt.Println("  Removing pipedpeer's nix key helper, its sudoers rule and the keys it trusted (sudo)...")
	if err := runAsRoot(helperUninstallScript()); err != nil {
		return errors.Join(profileErr, fmt.Errorf("could not remove the nix key helper: %w", err))
	}
	fmt.Println("  Done. System nix no longer trusts the cluster's keys.")
	return profileErr
}

// nixRoute is how setup gets this machine a nix.
type nixRoute int

const (
	nixUseSystem     nixRoute = iota // a system nix is installed; at most its store is missing
	nixInstallSystem                 // install the Determinate multi-user nix (needs root)
	nixUsePrivate                    // seed a private store in the user's data directory
)

// chooseNix says how setup gets a nix (ADR-0013, as amended 2026-10-12).
//
// A system nix is the reliable one: its nix-daemon builds without user
// namespaces, every tool and doc expects /nix, and closures land in the store
// other machines' setups also use. So a nix already present is used, and with
// root available the Determinate installer puts one in. The private store is
// the fallback for a machine the user cannot administer.
// PIPEDPEER_FORCE_PRIVATE_NIX=1 forces it, as nixstore.Private does.
func chooseNix(forcePrivate, systemNix, canRoot bool) nixRoute {
	switch {
	case forcePrivate:
		return nixUsePrivate
	case systemNix:
		return nixUseSystem
	case canRoot:
		return nixInstallSystem
	}
	return nixUsePrivate
}

// canBecomeRoot reports whether installing a system nix can get root here:
// running as root, sudo without a password, or sudo with a terminal to ask
// on. It cannot know whether the password will be accepted; installNix falls
// back to the private store when the installer fails.
var canBecomeRoot = func() bool {
	if geteuid() == 0 {
		return true
	}
	if _, err := exec.LookPath("sudo"); err != nil {
		return false
	}
	if exec.Command("sudo", "-n", "true").Run() == nil {
		return true
	}
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// Seams for installNix's three routes.
var (
	seedPrivate = func() error {
		return nixstore.Seed(func(f string, a ...any) { fmt.Printf(f, a...) })
	}
	installDeterminate = func() error {
		fmt.Println("    → Installing Nix (Determinate Systems installer, needs sudo)...")
		cmd := exec.Command("sh", "-c",
			`curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux --no-confirm`)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		return cmd.Run()
	}
	makeNixStore = createNixStore
)

// installNix is the nix prerequisite's install step.
func installNix() error {
	route := chooseNix(os.Getenv("PIPEDPEER_FORCE_PRIVATE_NIX") == "1", nixInstalled(), canBecomeRoot())
	switch route {
	case nixUseSystem:
		// A nix that is installed but storeless only needs the store: running
		// a full installer over a packaged nix would leave two of them.
		err := makeNixStore()
		if err == nil {
			return nil
		}
		// A machine where /nix cannot be created is what the private store
		// exists for.
		fmt.Printf("    → Could not create the system store (%v); using a private store instead\n", err)
	case nixInstallSystem:
		err := installDeterminate()
		if err == nil {
			return nil
		}
		fmt.Printf("    → The system nix installer failed (%v); using a private store instead,\n"+
			"      which needs no root but builds inside a user namespace\n", err)
	case nixUsePrivate:
		if os.Getenv("PIPEDPEER_FORCE_PRIVATE_NIX") == "1" {
			fmt.Println("    → PIPEDPEER_FORCE_PRIVATE_NIX=1: using a private store")
		} else {
			fmt.Println("    → No root here (no sudo), so nix goes in a private store in your data\n" +
				"      directory; a system nix (Determinate installer) is preferred where root is available")
		}
	}
	return seedPrivate()
}

func getPrereqs() []prereq {
	return []prereq{
		{name: "tar", check: binaryCheck("tar")},
		{name: "bash", check: binaryCheck("bash")},
		{name: "curl", check: binaryCheck("curl")},
		{name: "crun", check: binaryCheck("crun"), install: func() error {
			fmt.Println("    → Installing crun (OCI runtime)...")
			// Modern Nix (Determinate) often ships without nix-env, and some
			// machines have no nix at all — try each channel until LookPath
			// confirms a usable crun.
			attempts := [][]string{
				{"nix", "profile", "install", "nixpkgs#crun"},
				{"nix-env", "-iA", "nixpkgs.crun"},
			}
			var lastErr error
			for _, argv := range attempts {
				if _, err := exec.LookPath(argv[0]); err != nil {
					continue
				}
				cmd := exec.Command(argv[0], argv[1:]...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				lastErr = cmd.Run()
				if lastErr == nil && binaryCheck("crun")() {
					return nil
				}
			}

			// The distro package managers used to be tried here, each behind
			// sudo. They are gone: crun's own release binaries are static and
			// install into the user's bin directory, so a machine the user
			// does not administer can still join a cluster. Only reached when
			// nix is absent or has no crun, which is the common case on a
			// fresh worker.
			if err := installCrunRelease(); err != nil {
				if lastErr != nil {
					return fmt.Errorf("crun install failed: %w (nix attempt: %v)", err, lastErr)
				}
				return fmt.Errorf("crun install failed: %w", err)
			}
			if !binaryCheck("crun")() {
				return fmt.Errorf("crun installed to %s but is not on PATH", userdir.Bin())
			}
			return nil
		}},
		// crun comes before nix deliberately: a private store is bound at
		// /nix inside a namespace that crun builds, so seeding one needs
		// the runtime already in place.
		{name: "nix", check: nixUsable, install: installNix},
		{name: "nix-imports", check: acceptsPeerClosures, install: trustClusterKeys},
	}
}

func checkAll() []result {
	var results []result
	for _, p := range getPrereqs() {
		r := result{name: p.name}
		if p.check() {
			r.status = "ok"
		} else if p.install != nil {
			r.status = "missing"
		} else {
			r.status = "missing"
		}
		results = append(results, r)
	}
	return results
}

func missingWithInstaller(results []result) []prereq {
	all := getPrereqs()
	var missing []prereq
	for _, p := range all {
		for _, r := range results {
			if r.name == p.name && r.status != "ok" && p.install != nil {
				missing = append(missing, p)
				break
			}
		}
	}
	return missing
}

// Seams for the daemon step, so a test can watch the order of token and start.
var (
	daemonStatus     = daemonctl.Status
	daemonStart      = daemonctl.Start
	daemonRestart    = daemonctl.Restart
	serviceSupported = service.Supported
	serviceInstalled = service.Installed
	serviceInstall   = service.Install
)

// ask prints a [Y/n] question; empty, y and yes are yes.
func ask(q string) bool {
	fmt.Print(q + " [Y/n] ")
	var answer string
	if _, err := fmt.Scanln(&answer); err != nil && err.Error() != "unexpected newline" {
		fmt.Fprintf(os.Stderr, "pipedpeer: no answer read (%v); taking yes\n", err)
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "" || answer == "y" || answer == "yes"
}

// installService asks whether the daemon starts at boot and installs the user
// unit either way (enabled or not), so start/stop always go through systemd.
// It reports whether the unit is new.
func installService(autoYes, noInstall bool) bool {
	if noInstall {
		return false
	}
	if !serviceSupported() {
		term.Warn(os.Stdout, "no systemd user manager here; the daemon will not start at boot")
		return false
	}
	isNew := !serviceInstalled()
	boot := autoYes || ask("  Start pipedpeer at boot?")
	if err := serviceInstall(boot); err != nil {
		term.Fail(os.Stdout, "could not install the pipedpeer service: %v", err)
		term.Action(os.Stdout, "start the daemon by hand after a reboot: pipedpeer start")
		return false
	}
	if boot {
		term.OK(os.Stdout, "starts at boot (turn off: pipedpeer stop --boot)")
	} else {
		term.OK(os.Stdout, "installed as a service, not started at boot (turn on: pipedpeer start --boot)")
	}
	return isNew
}

// startDaemon brings the daemon up exactly once, and never without a cluster
// secret (Q7): a daemon started token-less served an open API and had to be
// restarted as soon as the secret arrived. created reports a new cluster.
func startDaemon(nodeID string, port int, mustRestart bool) (created bool, err error) {
	created, err = EnsureToken()
	if err != nil {
		return false, err
	}
	switch st := daemonStatus(); {
	case !st.Running:
		if err := daemonStart(nodeID, port, 0); err != nil {
			return created, fmt.Errorf("daemon start: %w", err)
		}
		term.OK(os.Stdout, "daemon running on :%d", port)
	case created || mustRestart:
		if err := daemonRestart(nodeID, port, 0); err != nil {
			return created, fmt.Errorf("daemon restart: %w", err)
		}
		term.OK(os.Stdout, "daemon restarted on :%d", port)
	default:
		term.OK(os.Stdout, "daemon already running on :%d", port)
	}
	return created, nil
}

// Run executes the setup flow. Returns the daemon port if started.
func Run(autoYes, noInstall bool, daemonPort int) (int, error) {
	fmt.Println()
	fmt.Println("  Pipedpeer setup")
	fmt.Println()

	// A phone in Termux has no Nix, crun, systemd or firewall to set up: it
	// becomes a pool-only worker on its own python.
	if phone.IsTermux() {
		if err := phone.Setup(os.Stdout); err != nil {
			return 0, fmt.Errorf("phone setup: %w", err)
		}
		return startAndReport(daemonPort, false)
	}

	results := checkAll()
	allOK := true
	for _, r := range results {
		if r.status == "ok" {
			term.OK(os.Stdout, "%s", r.name)
		} else {
			allOK = false
			term.Fail(os.Stdout, "%s %s", r.name, r.status)
		}
	}

	if !allOK && noInstall {
		term.Action(os.Stdout, "run `pipedpeer setup` without --no-install to install what is missing")
	} else if !allOK {
		toInstall := missingWithInstaller(results)
		if len(toInstall) == 0 {
			term.Action(os.Stdout, "install the missing tools above with your package manager, then run `pipedpeer setup` again")
		} else {
			if !autoYes && !ask("  Install missing prerequisites?") {
				term.Action(os.Stdout, "run `pipedpeer setup -y` to install what is missing")
				return 0, nil
			}
			for _, p := range toInstall {
				if err := p.install(); err != nil {
					return 0, fmt.Errorf("install %s failed: %w", p.name, err)
				}
				term.OK(os.Stdout, "%s installed", p.name)
			}
			for _, p := range toInstall {
				// Only bail out when nix is still unusable from this process,
				// which means a fresh install landed a binary that is not yet
				// on our PATH. Creating a missing store needs no such dance.
				if p.name == "nix" && !nixUsable() {
					term.Action(os.Stdout, "open a new terminal (or run `. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh`), then run `pipedpeer setup` again")
					return 0, nil
				}
			}
		}
	}

	// Before the daemon starts, because AppArmor attaches the profile only
	// to a process exec'd after it is loaded.
	profileLoaded := allowUserns(noInstall)

	// Before the daemon starts, so the service owns its only start.
	serviceNew := installService(autoYes, noInstall)

	// Ports before the daemon: peers that find it should be able to reach it.
	networkReport(autoYes)

	return startAndReport(daemonPort, profileLoaded || serviceNew)
}

// startAndReport starts the daemon under this machine's identity and prints
// the invite line when it created the cluster.
func startAndReport(daemonPort int, mustRestart bool) (int, error) {
	nodeID, err := identity.GetOrCreate()
	if err != nil {
		return 0, fmt.Errorf("identity: %w", err)
	}
	term.OK(os.Stdout, "this machine is %s", nodeID.ShortID())

	created, err := startDaemon(nodeID.NodeID, daemonPort, mustRestart)
	if err != nil {
		return 0, err
	}

	fmt.Println()
	if created {
		code, err := InviteCode()
		if err != nil {
			return daemonPort, err
		}
		term.OK(os.Stdout, "cluster created. On each other machine, run:")
		fmt.Println()
		fmt.Println("    pipedpeer connect " + code)
		fmt.Println()
		fmt.Println("  (pipedpeer invite prints this line again)")
		return daemonPort, nil
	}
	term.OK(os.Stdout, "setup complete")
	return daemonPort, nil
}
