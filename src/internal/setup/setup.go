package setup

import (
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

// multiUserNix reports whether nix builds go through a root nix-daemon
// (Determinate installer, distro multi-user setups). Single-user installs
// never check closure signatures for the owning user, so they need nothing.
func multiUserNix() bool {
	out, err := exec.Command("systemctl", "is-active", "nix-daemon").Output()
	return err == nil && strings.TrimSpace(string(out)) == "active"
}

// acceptsPeerClosures reports whether this node can import a peer's closure.
//
// A multi-user nix-daemon refuses a store path no trusted key signed - "lacks
// a signature by a trusted key" - so a stock worker could not receive work at
// all: jobs uploaded fine and died at import. Closures are signed now, and
// travel in a form that keeps the signature, so what a machine needs is to
// trust this cluster's keys.
func acceptsPeerClosures() bool {
	// A private store has no nix-daemon in front of it, so there is no
	// signature policy to repair and nothing to edit as root.
	if nixstore.Private() {
		return true
	}
	if !multiUserNix() {
		return true
	}
	nixPath, err := nixstore.SystemNix()
	if err != nil {
		return false
	}
	// Either answer is acceptable, and they mean different things.
	//
	// require-sigs off: an older machine still carrying the blanket
	// workaround. It works, and setup no longer creates that state - it is
	// left alone rather than "repaired", because turning the check back on
	// is a decision about the whole machine and belongs to whoever owns it.
	//
	// require-sigs on AND this cluster's keys trusted: the machine checks
	// every signature and accepts ours. That is the state worth having, and
	// the one setup now produces.
	out, err := exec.Command(nixPath, "config", "show", "require-sigs").Output()
	if err == nil && strings.TrimSpace(string(out)) == "false" {
		return true
	}
	return clusterKeysAreTrusted(nixPath)
}

// clusterKeysAreTrusted reports whether nix already accepts what this
// cluster signs with.
func clusterKeysAreTrusted(nixPath string) bool {
	want, err := os.ReadFile(nixsign.TrustedKeysFile())
	if err != nil {
		return false
	}
	fields := strings.Fields(string(want))
	if len(fields) == 0 {
		return false
	}
	out, err := exec.Command(nixPath, "config", "show", "trusted-public-keys").Output()
	if err != nil {
		return false
	}
	for _, k := range fields {
		if !strings.Contains(string(out), k) {
			return false
		}
	}
	return true
}

func trustClusterKeys() error {
	// Trust this cluster's keys. Do not switch the check off.
	//
	// The old repair appended `require-sigs = false`, which stops nix
	// verifying signatures for EVERYTHING on the machine - every channel,
	// every substituter, every user - so that pipedpeer could import one
	// unsigned peer closure. That is a system-wide reduction bought for one
	// program's convenience, and it outlives the program.
	//
	// It was necessary for a while, and the reason is worth remembering:
	// `nix-store --export`, which every transfer used, does not serialise
	// signatures, so a correctly signed closure was refused anyway. Closures
	// now travel as a small binary cache, where the signature is part of each
	// path's metadata and survives the journey - measured between two
	// machines, with the receiving one importing under require-sigs = true
	// and refusing the same closure when the sending node's key was taken out
	// of its trust list.
	//
	// So this writes one line: these specific keys are trusted, and
	// everything else is refused exactly as strictly as before.
	fmt.Println("    → Trusting this cluster's signing keys...")

	keys, err := os.ReadFile(nixsign.TrustedKeysFile())
	trusted := strings.TrimSpace(string(keys))
	if err != nil || trusted == "" {
		// Nothing to trust yet: the daemon writes this file once it knows
		// its peers. Saying so is better than editing the machine's nix
		// configuration to no effect.
		return fmt.Errorf("no cluster keys known yet (%s is empty); "+
			"start the daemon so it learns its peers, then re-run setup",
			nixsign.TrustedKeysFile())
	}

	// Determinate marks /etc/nix/nix.conf "do not modify" and includes
	// nix.custom.conf for local changes; plain multi-user installs edit
	// nix.conf itself.
	target := "/etc/nix/nix.conf"
	if _, err := os.Stat("/etc/nix/nix.custom.conf"); err == nil {
		target = "/etc/nix/nix.custom.conf"
	}

	fmt.Printf("      Adding extra-trusted-public-keys to %s.\n", target)
	fmt.Println("      Signature checking stays ON for everything else.")

	script := nixConfigRepair(trusted, target)
	argv := []string{"sh", "-c", script}
	if os.Geteuid() != 0 {
		argv = append([]string{"sudo"}, argv...)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not update %s: %w", target, err)
	}
	return nil
}

// nixConfigRepair is the edit setup makes to a machine's nix configuration.
//
// Separated from running it so what gets written to a user's system file as
// root can be asserted without editing anything. What must never reappear
// here is `require-sigs = false`: it would be one line in a diff, it would
// work, and it would silently stop the machine checking signatures on
// everything it ever installs.
func nixConfigRepair(trustedKeys, target string) string {
	return fmt.Sprintf("echo 'extra-trusted-public-keys = %s' >> %s && systemctl restart nix-daemon",
		trustedKeys, target)
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
		{name: "nix", check: nixUsable, install: func() error {
			// A nix that is installed but storeless only needs the store:
			// running a full installer over a packaged nix would leave two of
			// them on the machine.
			if nixInstalled() {
				if err := createNixStore(); err == nil {
					return nil
				}
				// Falls through to the private store: a machine where /nix
				// cannot be created is exactly the case it exists for.
			}

			// Try the unprivileged route first. The Determinate installer
			// needs root, which a user does not have on a machine they do not
			// administer, and asking for it is the single biggest barrier to
			// joining a cluster. A private store needs none: it lives in the
			// user's data directory and is bound at /nix inside a namespace.
			if err := nixstore.Seed(func(f string, a ...any) { fmt.Printf(f, a...) }); err == nil {
				return nil
			} else if os.Geteuid() != 0 {
				fmt.Printf("    → Private store unavailable (%v); falling back to the system installer\n", err)
			}

			fmt.Println("    → Installing Nix (Determinate Systems installer)...")
			cmd := exec.Command("sh", "-c",
				`curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux --no-confirm`)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Stdin = os.Stdin
			return cmd.Run()
		}},
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

// Run executes the setup flow. Returns the daemon port if started.
func Run(autoYes, noInstall bool, daemonPort int) (int, error) {
	fmt.Println()
	fmt.Println("  Pipedpeer Setup")
	fmt.Println("  ===============")
	fmt.Println()

	fmt.Println("  Checking prerequisites...")

	results := checkAll()
	for _, r := range results {
		icon := "✓"
		if r.status != "ok" {
			icon = "✗"
		}
		fmt.Printf("    %-12s %s  %s\n", r.name, icon, r.status)
	}

	allOK := true
	for _, r := range results {
		if r.status != "ok" {
			allOK = false
			break
		}
	}

	if allOK {
		fmt.Println()
		fmt.Println("  All prerequisites satisfied.")
	} else if noInstall {
		fmt.Println()
		fmt.Println("  Run without --no-install to install missing prerequisites.")
	} else {
		toInstall := missingWithInstaller(results)
		if len(toInstall) == 0 {
			fmt.Println()
			fmt.Println("  Some prerequisites are missing but cannot be auto-installed.")
		} else {
			fmt.Println()

			if !autoYes {
				fmt.Print("  Install missing prerequisites? [Y/n] ")
				var answer string
				fmt.Scanln(&answer)
				answer = strings.ToLower(strings.TrimSpace(answer))
				if answer != "" && answer != "y" && answer != "yes" {
					fmt.Println("  Skipping install.")
					fmt.Println("  Run again with -y to auto-confirm.")
					return 0, nil
				}
			}

			for _, p := range toInstall {
				if err := p.install(); err != nil {
					return 0, fmt.Errorf("install %s failed: %w", p.name, err)
				}
				fmt.Printf("    %-12s ✓  installed\n", p.name)
			}

			for _, p := range toInstall {
				// Only bail out when nix is still unusable from this process,
				// which means a fresh install landed a binary that is not yet
				// on our PATH. Creating a missing store needs no such dance.
				if p.name == "nix" && !nixUsable() {
					fmt.Println()
					fmt.Println("  Nix installed. Restart your shell or run:")
					fmt.Println("    . /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh")
					fmt.Println("  Then run 'pipedpeer setup' again to finalize.")
					return 0, nil
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("  Post Install Setup")
	fmt.Println("  ------------------")
	fmt.Println()

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		return 0, fmt.Errorf("identity: %w", err)
	}
	fmt.Printf("  Node identity: %s\n", nodeID.ShortID())
	fmt.Println()

	wasStarted, err := daemonctl.EnsureStarted(nodeID.NodeID, daemonPort)
	if err != nil {
		return 0, fmt.Errorf("daemon start: %w", err)
	}
	if wasStarted {
		fmt.Printf("  Daemon starting on :%d...  ✓\n", daemonPort)
	} else {
		fmt.Printf("  Daemon already running on :%d\n", daemonPort)
	}

	// After the daemon is up, because the report asks the router for the
	// port the daemon will use and it should be told the truth about a
	// machine in its finished state.
	networkReport(DirectPort(), autoYes)

	fmt.Println()
	fmt.Println("  Setup complete.")
	return daemonPort, nil
}
