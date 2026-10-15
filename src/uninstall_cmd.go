package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/doctor"
	"github.com/pipedpeer/pipedpeer/internal/say"
	"github.com/pipedpeer/pipedpeer/internal/service"
	"github.com/pipedpeer/pipedpeer/internal/setup"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/spf13/cobra"
)

func newUninstallCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove pipedpeer from this machine: daemon, service, firewall rules, root parts, data and the binary",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			yes, _ := cmd.Flags().GetBool("yes")
			dry, _ := cmd.Flags().GetBool("dry-run")
			out := cmd.OutOrStdout()

			steps := doctor.UninstallPlan(uninstallDeps())
			if dry || !yes {
				if err := doctor.RunSteps(out, steps, true); err != nil {
					return err
				}
			}
			if dry {
				return nil
			}
			if !yes {
				fmt.Fprint(out, "Remove all of this? [y/N] ")
				ans, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if err != nil && ans == "" {
					return fmt.Errorf("no answer read (use --yes to skip the question): %w", err)
				}
				if a := strings.ToLower(strings.TrimSpace(ans)); a != "y" && a != "yes" {
					fmt.Fprintln(out, "Nothing removed.")
					return nil
				}
			}
			err := doctor.RunSteps(out, steps, false)
			fmt.Fprintln(out, "Nix was kept: other software may use it. To remove it too, follow https://nix.dev/manual/nix/stable/installation/uninstall")
			return err
		},
	}
	cmd.Flags().BoolP("yes", "y", false, "Do not ask before removing")
	cmd.Flags().Bool("dry-run", false, "Only list what would be removed")
	return cmd
}

func uninstallDeps() doctor.UninstallDeps {
	d := doctor.UninstallDeps{
		StopDaemon: func() error {
			var errs []error
			if service.Installed() {
				errs = append(errs, service.Remove())
			}
			errs = append(errs, daemonctl.Stop())
			return errors.Join(errs...)
		},
		CloseFirewall: setup.CloseFirewall,
		RemoveRoot:    setup.Uninstall,
		Dirs:          []string{userdir.Data(), userdir.State(), userdir.Cache()},
		RemoveBinary:  removeBinary,
	}
	if exe, err := os.Executable(); err != nil {
		say.Warnf("cannot find this binary to delete it: %v\n", err)
	} else if real, err := filepath.EvalSymlinks(exe); err != nil {
		say.Warnf("cannot resolve %s, deleting the link itself: %v\n", exe, err)
		d.Binary = exe
	} else {
		d.Binary = real
	}
	return d
}

// removeBinary deletes the running binary (Linux keeps the open inode alive
// until exit), through sudo when it sits in a root-owned dir such as
// /usr/local/bin.
func removeBinary(path string) error {
	err := os.Remove(path)
	if err == nil || !errors.Is(err, fs.ErrPermission) {
		return err
	}
	c := exec.Command("sudo", "rm", "-f", path)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if rerr := c.Run(); rerr != nil {
		return fmt.Errorf("sudo rm -f %s: %w", path, rerr)
	}
	return nil
}
