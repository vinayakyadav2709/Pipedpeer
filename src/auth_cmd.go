package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
)

// newAuthCmd manages the shared secret that gates the daemon API.
//
// Without one, anyone who can reach the port runs code as root on the
// machine, which is why the system has so far only been safe on a network
// the operator already trusted.
func newAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage the shared secret that protects this node's daemon API",
	}

	set := &cobra.Command{
		Use:   "set [token]",
		Short: "Set the shared secret (generates one when no token is given)",
		Long: "Every node in a cluster must carry the same token, and jobs\n" +
			"inherit it automatically. Restart the daemon for it to take effect.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tok := ""
			if len(args) == 1 {
				tok = args[0]
			} else {
				tok = authtoken.Generate()
				if tok == "" {
					return fmt.Errorf("could not generate a token")
				}
			}
			if err := authtoken.Set(tok); err != nil {
				return err
			}
			fmt.Println(tok)
			fmt.Fprintf(os.Stderr, "\nStored in %s (mode 0600).\n", authtoken.Path())
			fmt.Fprintln(os.Stderr, "Set the same token on every node:")
			fmt.Fprintf(os.Stderr, "  pipedpeer auth set %s\n", tok)
			fmt.Fprintln(os.Stderr, "Then restart each daemon. Until you do, this node still")
			fmt.Fprintln(os.Stderr, "answers unauthenticated requests.")
			return nil
		},
	}

	show := &cobra.Command{
		Use:   "show",
		Short: "Print the configured token, if any",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok := authtoken.Current()
			if tok == "" {
				fmt.Println("no token set: this daemon accepts any request that reaches it")
				return nil
			}
			fmt.Println(tok)
			return nil
		},
	}

	clear := &cobra.Command{
		Use:   "clear",
		Short: "Remove the shared secret, leaving the daemon open",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := authtoken.Set(""); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "token cleared; this node will accept any request that reaches it")
			return nil
		},
	}

	forget := &cobra.Command{
		Use:   "forget [host:port|--all]",
		Short: "Drop a peer's pinned certificate after it was reinstalled",
		Long: "A peer's certificate is pinned on first contact, so a changed one\n" +
			"is refused: it is either a reinstalled daemon or someone in the\n" +
			"middle, and only you can tell those apart. Use this when you know\n" +
			"it was the former.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			all, _ := cmd.Flags().GetBool("all")
			if all {
				n := tlsid.ForgetAll()
				fmt.Fprintf(os.Stderr, "forgot %d pinned peer(s)\n", n)
				return nil
			}
			if len(args) != 1 {
				return fmt.Errorf("give a host:port, or --all")
			}
			tlsid.Forget(args[0])
			fmt.Fprintf(os.Stderr, "forgot %s; it will be pinned again on next contact\n", args[0])
			return nil
		},
	}
	forget.Flags().Bool("all", false, "Forget every pinned peer")

	cmd.AddCommand(set, show, clear, forget)
	return cmd
}
