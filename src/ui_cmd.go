package main

import (
	"fmt"
	"net/url"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

func newUICmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Open the cluster web page in a browser",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !daemonctl.Status().Running {
				return fmt.Errorf("daemon stopped (start it with: pipedpeer start)")
			}
			noOpen, _ := cmd.Flags().GetBool("print")
			local := fmt.Sprintf("http://127.0.0.1:%d/ui", localDaemonPort())
			// The page reads the token from the fragment, stores it and drops
			// the hash; a fragment never reaches the server or its logs.
			withToken := local
			if tok := authtoken.Current(); tok != "" {
				withToken += "#token=" + url.QueryEscape(tok)
			}
			fmt.Println(local)
			if noOpen {
				return nil
			}
			opener := "xdg-open"
			if runtime.GOOS == "darwin" {
				opener = "open"
			}
			if err := exec.Command(opener, withToken).Start(); err != nil {
				fmt.Printf("could not open a browser (%v); paste this into one:\n%s\n", err, withToken)
			}
			return nil
		},
	}
	cmd.Flags().Bool("print", false, "Only print the address")
	return cmd
}
