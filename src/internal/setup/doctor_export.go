package setup

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Read-only views of setup's own checks, for `pipedpeer doctor`. They reuse
// the exact logic setup acts on, so doctor and setup never disagree about
// whether a port is open or nix works.

// FirewallOpen reports the running host firewall ("firewalld", "ufw", or ""
// for none) and whether it already allows every FirewallRules port. Nothing is
// changed.
func FirewallOpen(ctx context.Context) (tool string, open bool) {
	st := openFirewall(ctx, FirewallRules(), false)
	return st.tool, st.open
}

// NixUsable is setup's "can nix build something here" check.
func NixUsable() bool { return nixUsable() }

// OwnCrun reports the crun in userdir.Bin() and whether it is the one
// installCrunRelease put there: it must report the pinned crunVersion. When
// it is not ours (or absent), why says so for uninstall's message.
func OwnCrun() (path string, ours bool, why string) {
	path = filepath.Join(userdir.Bin(), "crun")
	if _, err := os.Stat(path); err != nil {
		return path, false, "not present"
	}
	out, err := termuxexec.Command(path, "--version").Output()
	if err != nil {
		return path, false, "it does not run, so it cannot be identified as setup's: " + err.Error()
	}
	if !strings.Contains(string(out), crunVersion) {
		return path, false, "it is not crun " + crunVersion + ", the version setup installs"
	}
	return path, true, ""
}
