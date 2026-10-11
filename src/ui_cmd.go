package main

import (
	"fmt"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"
)

func newUICmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Open the cluster web page the daemon serves",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := cmd.Flags().GetInt("port")
			noOpen, _ := cmd.Flags().GetBool("print")
			local := fmt.Sprintf("http://127.0.0.1:%d/ui", port)
			fmt.Printf("This machine:    %s\n", local)
			fmt.Printf("From elsewhere:  http://%s:%d/ui\n", detectLocalIP(), port)
			if noOpen {
				return nil
			}
			opener := "xdg-open"
			if runtime.GOOS == "darwin" {
				opener = "open"
			}
			if err := exec.Command(opener, local).Start(); err != nil {
				fmt.Println("(could not open a browser; paste the address above into one)")
			}
			return nil
		},
	}
	cmd.Flags().Int("port", 38080, "Local daemon port")
	cmd.Flags().Bool("print", false, "Only print the address")
	return cmd
}
