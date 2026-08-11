package setup

import (
	"fmt"
	"os"
	"os/exec"
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

func getPrereqs() []prereq {
	return []prereq{
		{name: "tar", check: binaryCheck("tar")},
		{name: "bash", check: binaryCheck("bash")},
		{name: "curl", check: binaryCheck("curl")},
		{name: "nix", check: binaryCheck("nix"), install: func() error {
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
			cmd := exec.Command("nix-env", "-iA", "nixpkgs.crun")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
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
				if p.name == "nix" {
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
