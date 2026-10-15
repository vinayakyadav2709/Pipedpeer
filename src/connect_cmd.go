package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/invite"
	"github.com/pipedpeer/pipedpeer/internal/setup"
	"github.com/pipedpeer/pipedpeer/internal/term"
	"github.com/pipedpeer/pipedpeer/internal/warmup"
)

// connectWait is how long connect waits for the cluster before deciding.
const connectWait = 60 * time.Second

// newConnectCmd joins the cluster an invite code names: it saves the secret
// and introducers, restarts the daemon once, waits for the other machines and
// ends with connected (exit 0), partly (exit 2) or failed (exit 1), each with
// what to do. Then it builds the common environment in the background.
func newConnectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "connect <code>",
		Short: "Join the cluster of the machine that printed this code (pipedpeer invite)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out := cmd.OutOrStdout()
			inv, err := invite.Decode(args[0])
			if err != nil {
				term.Fail(out, "not connected: %v", err)
				if errors.Is(err, invite.ErrVersion) {
					term.Action(out, "install the same pipedpeer build as the inviting machine (`pipedpeer --version` there)")
				} else {
					term.Action(out, "copy the whole `pipedpeer connect ...` line from `pipedpeer invite` again")
				}
				os.Exit(int(connFailed))
			}
			nodeID, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			if err := authtoken.Set(inv.Secret); err != nil {
				return fmt.Errorf("saving the cluster secret: %w", err)
			}
			cfg, err := clustercfg.Load()
			if err != nil {
				return fmt.Errorf("reading %s: %w", clustercfg.Path(), err)
			}
			// The invite's introducers replace any older cluster's.
			cfg.Introducers = inv.Addrs
			if err := clustercfg.Save(cfg); err != nil {
				return fmt.Errorf("saving %s: %w", clustercfg.Path(), err)
			}

			port := daemonctl.DefaultPort
			if st := daemonctl.Status(); st.Running {
				if st.Port != 0 {
					port = st.Port
				}
				err = daemonctl.Restart(nodeID.NodeID, port, daemonctl.RunningMaxConcurrent(port))
			} else {
				err = daemonctl.Start(nodeID.NodeID, port, 0)
			}
			if err != nil {
				return err
			}
			term.OK(out, "joining through %s", strings.Join(inv.Addrs, ", "))
			if err := setup.TrustAfterJoin(); err != nil {
				term.Warn(out, "system nix does not trust the cluster's keys yet: %v", err)
				term.Action(out, "run `pipedpeer setup` so this machine accepts the other machines' environments")
			}

			res := waitConnected(fmt.Sprintf("http://127.0.0.1:%d", port), connectWait)
			printConnResult(out, res)
			if res.outcome != connFailed {
				exe, err := os.Executable()
				if err == nil {
					err = warmup.Start(exe, gpu.Detect().Vendor == gpu.VendorNVIDIA)
				}
				if err != nil {
					term.Warn(out, "could not start building the common environment: %v", err)
				} else {
					term.OK(out, "building the common environment in the background (`pipedpeer status` shows progress)")
				}
			}
			if res.outcome != connConnected {
				os.Exit(int(res.outcome))
			}
			return nil
		},
	}
}
