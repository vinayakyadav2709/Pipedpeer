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
// build already running, finished or just failed for this version is left
// alone and said so.
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
	var res warmup.Result
	if err == nil {
		res, err = warmupStart(exe, version, gpu.Detect().Vendor == gpu.VendorNVIDIA)
	}
	if err != nil {
		term.Warn(out, "could not start building the common environment: %v", err)
		return
	}
	switch res {
	case warmup.AlreadyRunning:
		term.OK(out, "common environment: already building (`pipedpeer status` shows progress)")
	case warmup.AlreadyDone:
		term.OK(out, "common environment: already built")
	case warmup.RecentlyFailed:
		term.Warn(out, "common environment: the last build failed a moment ago, not retrying yet; set PIPEDPEER_WARMUP_RETRY=1 to retry now (log: %s; earlier log: %s.prev)",
			warmup.LogPath(), warmup.LogPath())
	default:
		term.OK(out, "common environment: building in the background (`pipedpeer status` shows progress)")
	}
}
