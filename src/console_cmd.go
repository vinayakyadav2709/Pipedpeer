package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/console"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

const consoleDefaultPort = 7878

func newConsoleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "console",
		Short: "Open the local web console: cluster analytics and a notebook",
		Long: `Start a web console on this machine and open it in the browser.

The console shows the cluster and its job history, and runs notebooks whose
cells execute in a Python kernel here. It listens on 127.0.0.1 only and every
request needs the token in the printed URL, because notebook cells run code as
you.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			port, _ := cmd.Flags().GetInt("port")
			dir, _ := cmd.Flags().GetString("dir")
			python, _ := cmd.Flags().GetString("python")
			noOpen, _ := cmd.Flags().GetBool("no-open")
			devOrigin, _ := cmd.Flags().GetString("dev-origin")

			self, err := identity.GetOrCreate()
			if err != nil {
				return err
			}
			// Without the binary's path cluster cells cannot be submitted;
			// the console still serves everything else.
			executable, _ := termuxexec.Executable()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return console.Run(ctx, console.Options{
				Port:         port,
				DaemonPort:   localDaemonPort(),
				Dir:          dir,
				Python:       python,
				DevOrigin:    devOrigin,
				SelfNodeID:   self.NodeID,
				SelfHostname: self.Hostname,
				Executable:   executable,
				Env:          os.Environ(),
				Log:          logging.WithComponent("console"),
				Ready: func(url string) {
					fmt.Printf("Pipedpeer console: %s\n", url)
					fmt.Println("Press Ctrl+C to stop.")
					if !noOpen {
						openBrowser(url)
					}
				},
			})
		},
	}
	cmd.Flags().Int("port", consoleDefaultPort, "Port the console listens on (127.0.0.1 only)")
	cmd.Flags().String("dir", ".", "Directory notebooks are read from and written to")
	cmd.Flags().String("python", "python3", "Python interpreter the notebook kernel runs")
	cmd.Flags().Bool("no-open", false, "Do not open the console in a browser")
	cmd.Flags().String("dev-origin", "", "Additionally allow this Origin (for a frontend dev server proxying /api)")
	return cmd
}

// openBrowser asks the desktop to open url. Failure is not an error: the URL
// has been printed and a headless machine has nothing to open it with.
func openBrowser(url string) {
	opener := "xdg-open"
	if runtime.GOOS == "darwin" {
		opener = "open"
	}
	cmd := termuxexec.Command(opener, url)
	if err := cmd.Start(); err != nil {
		return
	}
	go func() { _ = cmd.Wait() }()
}
