package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/service"
	"github.com/pipedpeer/pipedpeer/internal/setup"
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
			port := daemonctl.DefaultPort
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
		"Maximum tasks this node accepts at once (0 = unlimited; also settable via PIPEDPEER_MAX_CONCURRENT)")
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
			_, err := setup.Run(autoYes, noInstall, daemonctl.DefaultPort)
			return err
		},
	}
	cmd.Flags().BoolP("yes", "y", false, "Answer yes to every question")
	cmd.Flags().Bool("no-install", false, "Check only, don't install anything")
	cmd.Flags().String("public-address", "",
		"this machine's address as seen from other networks (host or host:port, e.g. a cloud VM's public IP); "+
			"invites list it first. Empty clears it")
	return cmd
}
