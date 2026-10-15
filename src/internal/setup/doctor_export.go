package setup

import "context"

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
