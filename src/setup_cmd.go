package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/service"
	"github.com/pipedpeer/pipedpeer/internal/setup"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the daemon on this machine",
		Long: "Start the daemon. The cluster it joins comes from its config file " +
			"(written by setup and connect), so no flags are needed.\n\n" +
			"--boot also starts it at every boot; --introducer-only makes this machine " +
			"introduce the others and take no jobs (for a public server; " +
			"--introducer-only=false undoes it).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			boot, _ := cmd.Flags().GetBool("boot")
			maxConcurrent, _ := cmd.Flags().GetInt("max-concurrent")
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			restart := false
			if cmd.Flags().Changed("introducer-only") {
				on, _ := cmd.Flags().GetBool("introducer-only")
				cfg, err := clustercfg.Load()
				if err != nil {
					return fmt.Errorf("reading %s: %w", clustercfg.Path(), err)
				}
				cfg.NoJobs = on
				if err := clustercfg.Save(cfg); err != nil {
					return fmt.Errorf("saving %s: %w", clustercfg.Path(), err)
				}
				restart = true
			}
			if boot {
				if !service.Supported() {
					return fmt.Errorf("--boot needs a systemd user manager, and none answers here")
				}
				if service.Installed() {
					err = service.Enable(true)
				} else {
					// A daemon started by hand is replaced by the service's.
					err = service.Install(true)
					restart = true
				}
				if err != nil {
					return err
				}
				fmt.Println("starts at boot (turn off: pipedpeer stop --boot)")
			}
			port, _ := cmd.Flags().GetInt("port")
			st := daemonctl.Status()
			switch {
			case st.Running && !restart:
				fmt.Println("daemon already running")
				return nil
			case st.Running:
				if st.Port != 0 {
					port = st.Port
				}
				err = daemonctl.Restart(nodeID.NodeID, port, maxConcurrent)
			default:
				err = daemonctl.Start(nodeID.NodeID, port, maxConcurrent)
			}
			if err != nil {
				return err
			}
			fmt.Printf("daemon running (node %s, port %d)\n", nodeID.ShortID(), port)
			return nil
		},
	}
	cmd.Flags().Bool("boot", false, "also start the daemon at every boot")
	cmd.Flags().Bool("introducer-only", false, "introduce other machines but take no jobs (public server)")
	cmd.Flags().Int("max-concurrent", 0,
		"Maximum tasks this node accepts at once (0 = unlimited)")
	// Hidden: tests start throwaway daemons off the default port.
	cmd.Flags().Int("port", daemonctl.DefaultPort, "daemon API port")
	_ = cmd.Flags().MarkHidden("port")
	return cmd
}

func newStopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the daemon on this machine",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if boot, _ := cmd.Flags().GetBool("boot"); boot {
				if !service.Installed() {
					fmt.Println("not set to start at boot")
				} else if err := service.Enable(false); err != nil {
					return err
				} else {
					fmt.Println("no longer starts at boot (turn on: pipedpeer start --boot)")
				}
			}
			if err := daemonctl.Stop(); err != nil {
				return err
			}
			fmt.Println("stopped daemon")
			return nil
		},
	}
	cmd.Flags().Bool("boot", false, "also stop starting the daemon at boot")
	return cmd
}

func newSetupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Install what pipedpeer needs, open its ports and start the daemon",
		Long: `Checks for required tools (nix, crun, tar, bash, curl) and installs missing
ones, opens pipedpeer's ports in the firewall, offers to start pipedpeer at
boot, and starts the daemon. On a machine in no cluster yet it creates one
and prints the line the other machines run.

Use --no-install to only check without modifying the system.
Use -y to answer yes to every question.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			autoYes, _ := cmd.Flags().GetBool("yes")
			noInstall, _ := cmd.Flags().GetBool("no-install")
			if cmd.Flags().Changed("public-address") {
				addr, _ := cmd.Flags().GetString("public-address")
				if err := setup.SetPublicAddress(addr); err != nil {
					return err
				}
			}
			code, _ := cmd.Flags().GetString("connect")
			if code == "" {
				// The invite carries the cluster secret; the installer passes
				// it here to keep it out of the process list.
				code = takeConnectCodeEnv(cmd.ErrOrStderr())
			} else {
				_ = takeConnectCodeEnv(io.Discard)
			}
			o := setup.Options{AutoYes: autoYes, NoInstall: noInstall,
				Port: daemonctl.DefaultPort, Joining: code != ""}
			outcome, err := runSetup(cmd.OutOrStdout(), o, code)
			if err != nil {
				return err
			}
			if outcome != connConnected {
				os.Exit(int(outcome))
			}
			return nil
		},
	}
	cmd.Flags().BoolP("yes", "y", false, "Answer yes to every question")
	cmd.Flags().Bool("no-install", false, "Check only, don't install anything")
	cmd.Flags().String("connect", "",
		"after setup, join the cluster this invite code names (same as `pipedpeer connect <code>`)")
	cmd.Flags().String("public-address", "",
		"this machine's address as seen from other networks (host or host:port, e.g. a cloud VM's public IP); "+
			"invites list it first. Empty clears it")
	return cmd
}

// connectCodeEnv is read when --connect is not given, and only together with
// connectMarkerEnv, which the installer sets: a variable left in a shell
// profile must not turn a later `pipedpeer setup` into a join.
const (
	connectCodeEnv   = "PIPEDPEER_CONNECT_CODE"
	connectMarkerEnv = "PIPEDPEER_CONNECT_FROM_INSTALLER"
)

// takeConnectCodeEnv returns the installer's invite code and removes both
// variables, so no child (warm-up, daemon, proot login) inherits the secret.
// A code without the installer's marker is ignored with a note on warn.
func takeConnectCodeEnv(warn io.Writer) string {
	code, marked := os.Getenv(connectCodeEnv), os.Getenv(connectMarkerEnv) == "1"
	os.Unsetenv(connectCodeEnv)
	os.Unsetenv(connectMarkerEnv)
	if code != "" && !marked {
		fmt.Fprintf(warn, "ignoring %s from the environment (only the installer's use is honoured); use `pipedpeer connect <code>` or --connect\n", connectCodeEnv)
		return ""
	}
	return code
}

// Seam: tests replace setup.Run so no system is touched.
var setupRun = setup.Run

// runSetup runs setup, then joins with code when one was given, or else
// starts the warm-up itself (neither under --no-install). The returned
// outcome is the exit code: a join that was not fully connected, or that did
// not run because setup stopped, is non-zero. A bad code is refused before
// setup changes anything.
func runSetup(out io.Writer, o setup.Options, code string) (connOutcome, error) {
	if code != "" {
		if _, ok := decodeInvite(out, code); !ok {
			return connFailed, nil
		}
	}
	port, err := setupRun(o)
	if err != nil {
		return connFailed, err
	}
	if port <= 0 {
		if code != "" {
			term.Action(out, "setup did not finish, so this machine has not joined: once it has, run `pipedpeer connect <code>`")
			return connFailed, nil
		}
		return connConnected, nil
	}
	if code != "" {
		return runConnectFn(out, code, !o.NoInstall)
	}
	if !o.NoInstall {
		startWarmupFn(out)
	}
	return connConnected, nil
}
