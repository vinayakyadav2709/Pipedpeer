package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/doctor"
	"github.com/pipedpeer/pipedpeer/internal/say"
	"github.com/pipedpeer/pipedpeer/internal/setup"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/pipedpeer/pipedpeer/internal/userns"
	"github.com/spf13/cobra"
)

func newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check this machine and its links to the cluster, with the fix for each problem",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()

			cfg, err := clustercfg.Load()
			if err != nil {
				say.Warnf("could not read %s: %v\n", clustercfg.Path(), err)
			}
			c := doctor.Cluster{
				HTTP:        &http.Client{Timeout: 5 * time.Second},
				Token:       authtoken.Current(),
				Introducers: cfg.Introducers,
				Dial:        (&net.Dialer{}).DialContext,
				// A plain transport: the clock check needs only the Date
				// header, not the token proof.
				PeerHTTP: &http.Client{Transport: &http.Transport{}, Timeout: 3 * time.Second},
				Now:      time.Now,
			}
			if daemonctl.Status().Running {
				c.API = fmt.Sprintf("http://127.0.0.1:%d", localDaemonPort())
			}

			disk := "/nix"
			if _, err := os.Stat(disk); err != nil {
				disk = userdir.Data()
				if err := os.MkdirAll(disk, 0o755); err != nil {
					say.Warnf("could not create %s: %v\n", disk, err)
				}
			}
			m := doctor.Machine{
				Firewall: setup.FirewallOpen,
				Ports:    setup.FirewallRules(),
				Userns:   userns.Available,
				Crun: func() bool {
					if _, err := exec.LookPath("crun"); err == nil {
						return true
					}
					_, err := os.Stat(filepath.Join(userdir.Bin(), "crun"))
					return err == nil
				},
				Nix:       setup.NixUsable,
				PCIRoot:   "/sys/bus/pci/devices",
				DriverDir: "/proc/driver/nvidia",
				NvidiaSMI: func() bool { _, err := exec.LookPath("nvidia-smi"); return err == nil },
				DiskPath:  disk,
				FreeBytes: doctor.Statfs,
			}

			out := cmd.OutOrStdout()
			checks := append(c.Run(ctx), m.Run(ctx)...)
			if doctor.Print(out, checks) != 0 {
				return errors.New("doctor found problems (the ✗ lines above, each with its fix)")
			}
			return nil
		},
	}
}
