package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/setup"
)

// newInviteCmd prints exactly one line, `pipedpeer connect <code>`, for the
// other machines to run. With no cluster yet it creates one first (notices on
// stderr, so stdout stays the one line).
func newInviteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "invite",
		Short: "Print the one command other machines run to join this cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			created, err := setup.EnsureToken()
			if err != nil {
				return err
			}
			if created {
				fmt.Fprintln(os.Stderr, "pipedpeer: created a new cluster")
				// A daemon started without the secret must pick it up.
				if st := daemonctl.Status(); st.Running {
					nodeID, err := identity.GetOrCreate()
					if err != nil {
						return err
					}
					port := st.Port
					if port == 0 {
						port = daemonctl.DefaultPort
					}
					if err := daemonctl.Restart(nodeID.NodeID, port, daemonctl.RunningMaxConcurrent(port)); err != nil {
						return fmt.Errorf("restarting the daemon with the new secret: %w", err)
					}
				}
			}
			code, err := setup.InviteCode()
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "pipedpeer connect "+code)
			return nil
		},
	}
}
