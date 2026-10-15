package main

import (
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/setup"
)

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the local daemon process",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := cmd.Flags().GetInt("port")
			maxConcurrent, _ := cmd.Flags().GetInt("max-concurrent")
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			if st := daemonctl.Status(); st.Running {
				fmt.Printf("daemon already running\n")
				return nil
			}
			// Passed through the environment rather than as another argument,
			// because that is how the daemon reads it - and remembered, so a
			// machine that has joined a cluster is still in it after a
			// restart. Before this, a daemon restarted by anything that did
			// not know to repeat the flag came back alone and quietly stopped
			// taking work.
			flagRV, _ := cmd.Flags().GetString("rendezvous")
			if flagRV != "" {
				norm, err := clustercfg.Normalize(flagRV)
				if err != nil {
					return fmt.Errorf("--rendezvous %s: %w", flagRV, err)
				}
				flagRV = norm
				if err := clustercfg.SetRendezvous(norm); err != nil {
					fmt.Fprintf(os.Stderr, "warning: could not remember the cluster address: %v\n", err)
				}
			}
			if addr, source := clustercfg.Effective(flagRV); addr != "" {
				os.Setenv(clustercfg.EnvVar, addr)
				if source == "saved" {
					fmt.Printf("joining cluster through %s\n", addr)
				}
			}
			if err := daemonctl.Start(nodeID.NodeID, port, maxConcurrent); err != nil {
				return err
			}
			if maxConcurrent > 0 {
				fmt.Printf("started daemon (node-id=%s, port=%d, max-concurrent=%d)\n",
					nodeID.ShortID(), port, maxConcurrent)
			} else {
				fmt.Printf("started daemon (node-id=%s, port=%d)\n", nodeID.ShortID(), port)
			}
			return nil
		},
	}
	cmd.Flags().Int("port", 38080, "Local daemon port")
	cmd.Flags().String("rendezvous", "",
		"join a cluster over the internet through this address book, host[:port] "+
			"(also PIPEDPEER_RENDEZVOUS). Remembered, so later starts need no flag. "+
			"Peers are discovered and connected automatically.")
	cmd.Flags().Int("max-concurrent", 0,
		"Maximum tasks this node accepts at once (0 = unlimited; also settable via PIPEDPEER_MAX_CONCURRENT)")
	return cmd
}

// newJoinCmd is the one command a machine runs to become part of a cluster.
//
// Everything it does could be typed by hand - remember the address, start the
// daemon, wait, look at the node table - and every demo and every set of
// instructions did exactly that, in four steps, with a flag that had to be
// repeated on every later start. Machines were left half-joined often enough
// that it is worth being one command with no flags.
//
// It also waits for a peer before returning, because "did it work" is the
// only question anybody has at this point, and a daemon that has started is
// not yet an answer to it.
func newJoinCmd() *cobra.Command {
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "join <address>",
		Short: "Join a cluster through an introducer, and wait until peers appear",
		Long: "Join the cluster whose introducer is at <address> (host, or host:port; " +
			"the port defaults to " + strconv.Itoa(clustercfg.DefaultPort) + ").\n\n" +
			"The address is remembered, so afterwards `pipedpeer start` rejoins the " +
			"same cluster with no arguments. Peers on other networks are connected " +
			"to directly; the introducer only makes the introduction.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			addr, err := clustercfg.Normalize(args[0])
			if err != nil {
				return err
			}
			if err := clustercfg.SetRendezvous(addr); err != nil {
				return fmt.Errorf("could not remember the cluster address: %w", err)
			}
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}

			// A daemon that is already up was started without this address,
			// or with a different one, so it is in the wrong cluster or in
			// none. Restarting is what makes the join take effect, and doing
			// it here is better than printing an instruction to do it.
			maxConc := 0
			if daemonctl.Status().Running {
				maxConc = daemonctl.RunningMaxConcurrent(38080)
				fmt.Println("restarting the daemon to join through " + addr)
				if err := daemonctl.Stop(); err != nil {
					return err
				}
			} else {
				fmt.Println("joining through " + addr)
			}
			os.Setenv(clustercfg.EnvVar, addr)
			if err := daemonctl.Start(nodeID.NodeID, 38080, maxConc); err != nil {
				return err
			}
			fmt.Printf("this node is %s\n", nodeID.ShortID())

			if n := waitForPeers(38080, wait); n > 0 {
				fmt.Println()
				if err := printNodes(38080); err != nil {
					return err
				}
				trustAfterJoin()
				return nil
			}

			// Not an error. A first machine has nobody to meet yet, and
			// saying so is more useful than a non-zero exit that reads like
			// the join failed.
			fmt.Printf("\nThis is normal for the first machine in a\n" +
				"cluster - run the same command on the others and they will find\n" +
				"each other. Check with: pipedpeer nodes\n")
			trustAfterJoin()
			return nil
		},
	}
	cmd.Flags().DurationVar(&wait, "wait", 90*time.Second, "how long to wait for peers before returning")
	return cmd
}

// trustAfterJoin lets a multi-user nix accept the peers just met and those
// that join later. A failure does not undo the join, so it is reported, not
// returned.
func trustAfterJoin() {
	if err := setup.TrustAfterJoin(); err != nil {
		fmt.Fprintf(os.Stderr, "could not make system nix trust the cluster's keys: %v\n"+
			"peers' closures will be refused here until `pipedpeer trust` succeeds\n", err)
	}
}

func newTrustCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "trust",
		Short: "Let system nix accept closures signed by this cluster's nodes",
		Long: "A multi-user nix (nix-daemon) accepts only signing keys listed in nix.conf. " +
			"This adds the keys the daemon has collected from the cluster's peers through " +
			"pipedpeer's root-owned key helper, installing it first if setup did not (one sudo); " +
			"after that the daemon adds new peers' keys itself, with no prompt. " +
			"It does nothing on a private store or a single-user nix, which take the keys " +
			"with each import.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			msg, err := setup.Trust()
			if err != nil {
				return err
			}
			fmt.Println(msg)
			return nil
		},
	}
}

// healthyPeerCount counts peers other than this node that are usable now.
func healthyPeerCount() int {
	nodes, err := fetchClusterNodes(38080)
	if err != nil {
		return 0
	}
	n := 0
	for _, nd := range nodes {
		// "self" is this machine, which is never the answer to "did anyone
		// else turn up".
		if nd.Source == "self" || nd.State != "healthy" {
			continue
		}
		n++
	}
	return n
}

func newStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the local daemon process",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := daemonctl.Stop(); err != nil {
				return err
			}
			fmt.Println("stopped daemon")
			return nil
		},
	}
}

func newSetupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Check prerequisites, install missing, and start daemon",
		Long: `Checks for required system tools (nix, tar, bash), optionally installs
missing ones, creates a node identity, and starts the local daemon.

Use --no-install to only check without modifying the system.
Use -y to skip the install confirmation prompt.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			autoYes, _ := cmd.Flags().GetBool("yes")
			noInstall, _ := cmd.Flags().GetBool("no-install")
			port, _ := cmd.Flags().GetInt("port")
			if uninstall, _ := cmd.Flags().GetBool("uninstall"); uninstall {
				return setup.Uninstall()
			}

			_, err := setup.Run(autoYes, noInstall, port)
			return err
		},
	}
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation, auto-install")
	cmd.Flags().Bool("no-install", false, "Check only, don't install anything")
	cmd.Flags().Bool("uninstall", false, "Remove the AppArmor profile, the nix key helper, its sudoers rule and the keys it made system nix trust")
	cmd.Flags().Int("port", 38080, "Daemon port")
	return cmd
}
