package main

import (
	"io"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/term"
	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
	"github.com/pipedpeer/pipedpeer/internal/warmup"
)

// Seams: tests replace these so no daemon or build is started.
var (
	startWarmupFn = startWarmup
	runConnectFn  = runConnect
	warmupStart   = warmup.Start
)

// startWarmup begins building the common environment in the background.
// Introducer-only and pool-only machines cannot build one and say so. A
// build already running or finished for this version is left alone
// (warmup.Start returns nil without starting).
func startWarmup(out io.Writer) {
	cfg, err := clustercfg.Load()
	if err != nil {
		term.Warn(out, "could not read %s, building the common environment anyway: %v", clustercfg.Path(), err)
	} else if cfg.NoJobs {
		term.OK(out, "not building the common environment: this machine is introducer-only")
		return
	} else if cfg.PoolOnly {
		term.OK(out, "not building the common environment: this machine is pool-only (no nix)")
		return
	}
	exe, err := termuxexec.Executable()
	if err == nil {
		err = warmupStart(exe, version, gpu.Detect().Vendor == gpu.VendorNVIDIA)
	}
	if err != nil {
		term.Warn(out, "could not start building the common environment: %v", err)
		return
	}
	term.OK(out, "common environment: building in the background or already built (`pipedpeer status` shows progress)")
}
