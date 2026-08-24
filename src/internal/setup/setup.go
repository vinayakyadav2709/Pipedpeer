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
	if !binaryCheck("nix")() {
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

	argv := []string{"install", "-d", "-m", "0755"}
	if os.Geteuid() != 0 {
		u, err := user.Current()
		if err != nil {
			return fmt.Errorf("looking up the current user: %w", err)
		}
		argv = append(argv, "-o", u.Uid, "-g", u.Gid)
		argv = append([]string{"sudo"}, argv...)
	}
	// The store lives at <root>/store; create the root so nix owns both.
	argv = append(argv, filepath.Dir(storeDir()))

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not create %s: %w", filepath.Dir(storeDir()), err)
	}
	return nil
}

func getPrereqs() []prereq {
	return []prereq{
		{name: "tar", check: binaryCheck("tar")},
		{name: "bash", check: binaryCheck("bash")},
		{name: "curl", check: binaryCheck("curl")},
		{name: "nix", check: nixUsable, install: func() error {
			// A nix that is installed but storeless only needs the store:
			// running a full installer over a packaged nix would leave two of
			// them on the machine.
			if binaryCheck("nix")() {
				return createNixStore()
			}
			fmt.Println("    → Installing Nix (Determinate Systems installer)...")
			cmd := exec.Command("sh", "-c",
				`curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux --no-confirm`)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Stdin = os.Stdin
			return cmd.Run()
		}},
		{name: "crun", check: binaryCheck("crun"), install: func() error {
			fmt.Println("    → Installing crun (OCI runtime)...")
			// Modern Nix (Determinate) often ships without nix-env, and some
			// machines have no nix at all — try each channel until LookPath
			// confirms a usable crun.
			attempts := [][]string{
				{"nix", "profile", "install", "nixpkgs#crun"},
				{"nix-env", "-iA", "nixpkgs.crun"},
				{"apt-get", "install", "-y", "crun"},
				{"dnf", "install", "-y", "crun"},
			}
			var lastErr error
			for _, argv := range attempts {
				if _, err := exec.LookPath(argv[0]); err != nil {
					continue
				}
				// System package managers need root; nix installs per-user.
				if argv[0] == "apt-get" || argv[0] == "dnf" {
					if os.Geteuid() != 0 {
						argv = append([]string{"sudo"}, argv...)
					}
				}
				cmd := exec.Command(argv[0], argv[1:]...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				lastErr = cmd.Run()
				if lastErr == nil && binaryCheck("crun")() {
					return nil
				}
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("no installer available (tried nix, apt, dnf)")
			}
			return fmt.Errorf("crun install failed: %w", lastErr)
		}},
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

	fmt.Println()
	fmt.Println("  Setup complete.")
	return daemonPort, nil
}
