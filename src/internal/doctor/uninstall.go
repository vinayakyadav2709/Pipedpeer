package doctor

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Step is one thing `pipedpeer uninstall` removes.
type Step struct {
	What string
	Do   func() error
}

// UninstallDeps are the removers, injected so the plan is testable without
// touching the machine.
type UninstallDeps struct {
	StopDaemon    func() error // daemon process and user service
	CloseFirewall func() error
	RemoveRoot    func() error // AppArmor profile, nix key helper, trusted keys; nil skips both
	Extra         []Step       // the platform's own parts (a phone's container and packages), after the stop
	Dirs          []string     // userdir Data, State, Cache
	// Crun is setup's own crun to delete; CrunKeep, when set instead, is
	// "<path>: <why it stays>".
	Crun         string
	CrunKeep     string
	Binary       string
	RemoveBinary func(path string) error
}

// UninstallPlan lists the steps in order: stop first so nothing rewrites the
// dirs being deleted, the binary last so a failure earlier can be retried.
func UninstallPlan(d UninstallDeps) []Step {
	steps := []Step{{"stop the daemon and remove its user service", d.StopDaemon}}
	steps = append(steps, d.Extra...)
	if d.CloseFirewall != nil {
		steps = append(steps, Step{"close pipedpeer's firewall ports", d.CloseFirewall})
	}
	if d.RemoveRoot != nil {
		steps = append(steps, Step{"remove the root parts (AppArmor profile, nix key helper, trusted cluster keys)", d.RemoveRoot})
	}
	for _, dir := range d.Dirs {
		dir := dir
		steps = append(steps, Step{"delete " + dir, func() error { return os.RemoveAll(dir) }})
	}
	if d.Crun != "" {
		steps = append(steps, Step{"delete " + d.Crun + " (the crun setup installed)", func() error { return os.Remove(d.Crun) }})
	} else if d.CrunKeep != "" {
		steps = append(steps, Step{"keep " + d.CrunKeep, func() error { return nil }})
	}
	if d.Binary != "" {
		steps = append(steps, Step{"delete " + d.Binary, func() error { return d.RemoveBinary(d.Binary) }})
	}
	return steps
}

// RunSteps runs (or with dryRun only lists) every step, carrying on past a
// failure so one stuck part does not leave the rest behind.
func RunSteps(w io.Writer, steps []Step, dryRun bool) error {
	var errs []error
	for _, s := range steps {
		if dryRun {
			fmt.Fprintln(w, "would "+s.What)
			continue
		}
		if err := s.Do(); err != nil {
			term.Fail(w, "%s: %v", s.What, err)
			errs = append(errs, fmt.Errorf("%s: %w", s.What, err))
			continue
		}
		term.OK(w, "%s", s.What)
	}
	return errors.Join(errs...)
}
