package setup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/internet"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Two things setup can do for direct connections without weakening anything.
//
// The first is a diagnosis. Whether this machine can be reached directly is
// decided by its router, the answer is not guessable, and the failure when it
// is wrong looks like a peer that silently never appears. Saying it plainly
// during setup turns an evening of confusion into a sentence.
//
// The second is the host firewall. Fedora ships firewalld on by default, and
// a punch that the NAT would have allowed dies there instead - which is
// maddening precisely because the router did its part. Opening one UDP port
// admits nothing unauthenticated: the only thing behind it is a QUIC listener
// that verifies a signed identity before it will do anything at all.
//
// What is deliberately NOT done: nothing touches the router's configuration,
// no sysctl is changed, no protection is switched off, and no relay is
// configured behind the user's back.

// firewallStatus is what was found and what, if anything, was changed.
type firewallStatus struct {
	// tool is "firewalld", "ufw" or "" when no active firewall was found.
	tool string
	// open is whether the port is now allowed.
	open bool
	// changed is whether this run changed anything.
	changed bool
	// undo is the command that reverses it, empty when nothing was changed.
	undo string
	// err explains a failure to a user, and is not fatal.
	err error
}

// firewallCmd runs a command, as a variable so tests can drive the logic
// without a firewall.
var firewallCmd = func(ctx context.Context, name string, args ...string) (string, error) {
	// Run what lookPath found: ufw lives in /usr/sbin, which is not on a
	// Debian user's PATH, so the bare name fails exactly where lookPath's
	// fallback succeeded. Under sudo the tool is the first argument.
	if name == "sudo" && len(args) > 0 {
		if p, err := lookPath(args[0]); err == nil {
			args = append([]string{p}, args[1:]...)
		}
	} else if p, err := lookPath(name); err == nil {
		name = p
	}
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// geteuid is os.Geteuid, as a variable so tests can pretend to be root or not.
var geteuid = os.Geteuid

// asRoot prefixes sudo when not root (same pattern as setup.go).
func asRoot(name string, args ...string) (string, []string) {
	if geteuid() != 0 {
		return "sudo", append([]string{name}, args...)
	}
	return name, args
}

// runRoot runs a command that needs root through firewallCmd.
func runRoot(ctx context.Context, name string, args ...string) (string, error) {
	n, a := asRoot(name, args...)
	return firewallCmd(ctx, n, a...)
}

// lookPath is lookTool, as a variable for the same reason.
var lookPath = lookTool

// sbinDirs are where firewall tools live when PATH leaves them out: a Debian
// or Ubuntu user's PATH has no /usr/sbin, so ufw was "not installed" while it
// was active and setup reported no firewall running (V2 evidence 51).
var sbinDirs = []string{"/usr/sbin", "/sbin"}

// lookTool is exec.LookPath with a fallback to sbinDirs.
func lookTool(name string) (string, error) {
	p, err := exec.LookPath(name)
	if err == nil {
		return p, nil
	}
	for _, dir := range sbinDirs {
		if p, serr := exec.LookPath(filepath.Join(dir, name)); serr == nil {
			return p, nil
		}
	}
	return "", err
}

// openFirewall allows the direct port on whichever host firewall is running.
//
// Returns what it found even when it changes nothing, because "no firewall is
// running" and "the port was opened" are both worth printing and are
// different answers.
func openFirewall(ctx context.Context, rules []string, apply bool) firewallStatus {
	if _, err := lookPath("firewall-cmd"); err == nil {
		out, err := firewallCmd(ctx, "firewall-cmd", "--state")
		// Compared exactly, not by substring: firewalld answers "not
		// running" when it is off, and that contains "running". A stopped
		// firewall would have been treated as an active one and rules added
		// to it for nothing.
		if err == nil && strings.TrimSpace(out) == "running" {
			return openFirewalld(ctx, rules, apply)
		}
	}
	if _, err := lookPath("ufw"); err == nil {
		statusCmd := firewallCmd
		if apply {
			statusCmd = runRoot
		}
		out, err := statusCmd(ctx, "ufw", "status")
		// "Status: inactive" does not contain "Status: active", but the
		// firewalld case above shows how easily that stops being true, so
		// the negative is ruled out explicitly.
		if err == nil && strings.Contains(out, "Status: active") &&
			!strings.Contains(out, "Status: inactive") {
			return openUFW(ctx, rules, apply)
		}
	}
	return firewallStatus{open: true}
}

func openFirewalld(ctx context.Context, rules []string, apply bool) firewallStatus {
	st := firewallStatus{tool: "firewalld"}
	var missing []string
	for _, rule := range rules {
		if out, err := firewallCmd(ctx, "firewall-cmd", "--query-port="+rule); err == nil &&
			strings.HasPrefix(strings.TrimSpace(out), "yes") {
			continue
		}
		missing = append(missing, rule)
	}
	if len(missing) == 0 {
		st.open = true
		return st
	}
	if !apply {
		return st
	}
	// --permanent then --reload, so it survives a reboot. A rule that
	// disappears on restart is worse than none: it works until the machine is
	// rebooted and then fails in a way nobody connects to the reboot.
	var undo []string
	for _, rule := range missing {
		if out, err := runRoot(ctx, "firewall-cmd", "--permanent", "--add-port="+rule); err != nil {
			st.err = fmt.Errorf("firewall-cmd --add-port=%s: %v (%s)", rule, err, strings.TrimSpace(out))
			return st
		}
		undo = append(undo, "sudo firewall-cmd --permanent --remove-port="+rule)
	}
	if out, err := runRoot(ctx, "firewall-cmd", "--reload"); err != nil {
		st.err = fmt.Errorf("firewall-cmd --reload: %v (%s)", err, strings.TrimSpace(out))
		return st
	}
	st.open, st.changed = true, true
	st.undo = strings.Join(undo, " && ") + " && sudo firewall-cmd --reload"
	return st
}

// ufwRule is the ufw spelling of a firewalld rule: ranges use ':' not '-'.
func ufwRule(rule string) string { return strings.ReplaceAll(rule, "-", ":") }

func openUFW(ctx context.Context, rules []string, apply bool) firewallStatus {
	st := firewallStatus{tool: "ufw"}

	statusCmd := firewallCmd
	if apply {
		statusCmd = runRoot
	}
	out, serr := statusCmd(ctx, "ufw", "status")
	var missing []string
	for _, rule := range rules {
		if serr == nil && strings.Contains(out, ufwRule(rule)) {
			continue
		}
		missing = append(missing, ufwRule(rule))
	}
	if len(missing) == 0 {
		st.open = true
		return st
	}
	if !apply {
		return st
	}
	var undo []string
	for _, rule := range missing {
		if out, err := runRoot(ctx, "ufw", "allow", rule); err != nil {
			st.err = fmt.Errorf("ufw allow %s: %v (%s)", rule, err, strings.TrimSpace(out))
			return st
		}
		undo = append(undo, "sudo ufw delete allow "+rule)
	}
	st.open, st.changed = true, true
	st.undo = strings.Join(undo, " && ")
	return st
}

// networkReport opens this machine's ports on the host firewall (apply) or
// says which are blocked. Printed during setup rather than left for a failure
// to reveal: a blocked port looks like a peer that quietly never appears.
// The router is not asked for a mapping any more (no router mapping call):
// peers punch, and the introducer learns the public address.
func networkReport(apply bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rules := FirewallRules()
	list := strings.Join(rules, ", ")
	fw := openFirewall(ctx, rules, apply)
	switch {
	case fw.err != nil:
		term.Fail(os.Stdout, "firewall: %s is running and %s could not be opened: %v", fw.tool, list, fw.err)
		term.Action(os.Stdout, "open %s in %s yourself, or peers cannot reach this machine", list, fw.tool)
	case fw.tool == "":
		term.OK(os.Stdout, "firewall: none running; %s reachable", list)
	case fw.changed:
		term.OK(os.Stdout, "firewall: opened %s in %s (undo: %s)", list, fw.tool, fw.undo)
	case fw.open:
		term.OK(os.Stdout, "firewall: %s already allows %s", fw.tool, list)
	default:
		term.Fail(os.Stdout, "firewall: %s is running and blocks %s", fw.tool, list)
		term.Action(os.Stdout, "run `pipedpeer setup -y` to open them, or allow them yourself")
	}
}

// FirewallRules is every port a peer must reach on this machine, firewalld
// spelling: the daemon API (tcp) and LAN discovery (udp) on
// daemonctl.DefaultPort, direct QUIC on internet.DefaultPort, the introducer
// on clustercfg.DefaultPort.
func FirewallRules() []string {
	return []string{
		fmt.Sprintf("%d/tcp", daemonctl.DefaultPort),
		fmt.Sprintf("%d/udp", daemonctl.DefaultPort),
		fmt.Sprintf("%d/udp", DirectPort()),
		fmt.Sprintf("%d/udp", clustercfg.DefaultPort),
	}
}

// legacyRules are the ports builds before the 4242 block opened; uninstall
// closes them too, so a machine set up by an old build is left clean.
var legacyRules = []string{"38080/tcp", "38099/udp", "38447/udp", "38445/udp", "29500-29509/tcp"}

// DirectPort is the port the report and the firewall rule are about.
func DirectPort() int { return internet.DefaultPort }

// CloseFirewall removes every rule setup (this build or an older one) added
// to firewalld or ufw. No firewall running is not an error.
func CloseFirewall() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rules := append(FirewallRules(), legacyRules...)
	var errs []error
	if _, err := lookPath("firewall-cmd"); err == nil {
		if out, err := firewallCmd(ctx, "firewall-cmd", "--state"); err == nil && strings.TrimSpace(out) == "running" {
			removed := false
			for _, rule := range rules {
				if out, err := firewallCmd(ctx, "firewall-cmd", "--permanent", "--query-port="+rule); err != nil ||
					!strings.HasPrefix(strings.TrimSpace(out), "yes") {
					continue
				}
				if out, err := runRoot(ctx, "firewall-cmd", "--permanent", "--remove-port="+rule); err != nil {
					errs = append(errs, fmt.Errorf("firewall-cmd --remove-port=%s: %v (%s)", rule, err, strings.TrimSpace(out)))
					continue
				}
				removed = true
			}
			if removed {
				if out, err := runRoot(ctx, "firewall-cmd", "--reload"); err != nil {
					errs = append(errs, fmt.Errorf("firewall-cmd --reload: %v (%s)", err, strings.TrimSpace(out)))
				}
			}
		}
	}
	if _, err := lookPath("ufw"); err == nil {
		out, err := runRoot(ctx, "ufw", "status")
		if err == nil && strings.Contains(out, "Status: active") && !strings.Contains(out, "Status: inactive") {
			for _, rule := range rules {
				r := ufwRule(rule)
				if !strings.Contains(out, r) {
					continue
				}
				if o, err := runRoot(ctx, "ufw", "delete", "allow", r); err != nil {
					errs = append(errs, fmt.Errorf("ufw delete allow %s: %v (%s)", r, err, strings.TrimSpace(o)))
				}
			}
		}
	}
	return errors.Join(errs...)
}
