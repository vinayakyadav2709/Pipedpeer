package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
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

	cmd.AddCommand(set, show, clear)
	return cmd
}
