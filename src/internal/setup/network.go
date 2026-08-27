package setup

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/internet"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
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
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// lookPath is exec.LookPath, as a variable for the same reason.
var lookPath = exec.LookPath

// openFirewall allows the direct port on whichever host firewall is running.
//
// Returns what it found even when it changes nothing, because "no firewall is
// running" and "the port was opened" are both worth printing and are
// different answers.
func openFirewall(ctx context.Context, port int, apply bool) firewallStatus {
	if _, err := lookPath("firewall-cmd"); err == nil {
		out, err := firewallCmd(ctx, "firewall-cmd", "--state")
		// Compared exactly, not by substring: firewalld answers "not
		// running" when it is off, and that contains "running". A stopped
		// firewall would have been treated as an active one and rules added
		// to it for nothing.
		if err == nil && strings.TrimSpace(out) == "running" {
			return openFirewalld(ctx, port, apply)
		}
	}
	if _, err := lookPath("ufw"); err == nil {
		out, err := firewallCmd(ctx, "ufw", "status")
		// "Status: inactive" does not contain "Status: active", but the
		// firewalld case above shows how easily that stops being true, so
		// the negative is ruled out explicitly.
		if err == nil && strings.Contains(out, "Status: active") &&
			!strings.Contains(out, "Status: inactive") {
			return openUFW(ctx, port, apply)
		}
	}
	return firewallStatus{open: true}
}

func openFirewalld(ctx context.Context, port int, apply bool) firewallStatus {
	st := firewallStatus{tool: "firewalld"}
	rule := fmt.Sprintf("%d/udp", port)

	if out, err := firewallCmd(ctx, "firewall-cmd", "--query-port="+rule); err == nil &&
		strings.HasPrefix(strings.TrimSpace(out), "yes") {
		st.open = true
		return st
	}
	if !apply {
		return st
	}
	// --permanent then --reload, so it survives a reboot. A rule that
	// disappears on restart is worse than none: it works until the machine is
	// rebooted and then fails in a way nobody connects to the reboot.
	if out, err := firewallCmd(ctx, "firewall-cmd", "--permanent", "--add-port="+rule); err != nil {
		st.err = fmt.Errorf("firewall-cmd --add-port: %v (%s)", err, strings.TrimSpace(out))
		return st
	}
	if out, err := firewallCmd(ctx, "firewall-cmd", "--reload"); err != nil {
		st.err = fmt.Errorf("firewall-cmd --reload: %v (%s)", err, strings.TrimSpace(out))
		return st
	}
	st.open, st.changed = true, true
	st.undo = fmt.Sprintf("sudo firewall-cmd --permanent --remove-port=%s && sudo firewall-cmd --reload", rule)
	return st
}

func openUFW(ctx context.Context, port int, apply bool) firewallStatus {
	st := firewallStatus{tool: "ufw"}
	rule := fmt.Sprintf("%d/udp", port)

	if out, err := firewallCmd(ctx, "ufw", "status"); err == nil && strings.Contains(out, rule) {
		st.open = true
		return st
	}
	if !apply {
		return st
	}
	if out, err := firewallCmd(ctx, "ufw", "allow", rule); err != nil {
		st.err = fmt.Errorf("ufw allow: %v (%s)", err, strings.TrimSpace(out))
		return st
	}
	st.open, st.changed = true, true
	st.undo = fmt.Sprintf("sudo ufw delete allow %s", rule)
	return st
}

// networkReport prints what this machine can do about direct connections.
//
// Printed during setup rather than left for a failure to reveal, because the
// failure it prevents is a peer that quietly never appears - nothing errors,
// nothing logs, work simply goes elsewhere and the machine looks idle.
func networkReport(port int, apply bool) {
	fmt.Println()
	fmt.Println("  Direct connections")
	fmt.Println("  ------------------")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fw := openFirewall(ctx, port, apply)
	switch {
	case fw.err != nil:
		fmt.Printf("    firewall     ✗  %s is running and port %d/udp could not be opened:\n", fw.tool, port)
		fmt.Printf("                    %v\n", fw.err)
		fmt.Printf("                    Peers will not be able to reach this machine directly.\n")
	case fw.tool == "":
		fmt.Printf("    firewall     ✓  none running; port %d/udp is reachable\n", port)
	case fw.changed:
		fmt.Printf("    firewall     ✓  opened %d/udp in %s\n", port, fw.tool)
		fmt.Printf("                    Only authenticated, encrypted QUIC is accepted there.\n")
		fmt.Printf("                    Undo with: %s\n", fw.undo)
	case fw.open:
		fmt.Printf("    firewall     ✓  %s already allows %d/udp\n", fw.tool, port)
	default:
		fmt.Printf("    firewall     ✗  %s is running and blocks %d/udp\n", fw.tool, port)
		fmt.Printf("                    Re-run with -y to open it, or allow it yourself.\n")
	}

	m, err := portmap.Map(ctx, uint16(port))
	switch {
	case err == nil && m.Public:
		fmt.Printf("    router       ✓  granted %s\n", m.External)
		fmt.Printf("                    This machine is directly reachable; peers need no punching.\n")
	case err == nil && !m.Public:
		fmt.Printf("    router       ~  granted %s, which is not a public address\n", m.External)
		fmt.Printf("                    There is a second layer of NAT above this router - a phone\n")
		fmt.Printf("                    tether or a carrier network. Peers will punch instead.\n")
	default:
		fmt.Printf("    router       ~  no port mapping offered\n")
		fmt.Printf("                    Normal: many routers have it switched off. Peers will punch.\n")
	}
}

// DirectPort is the port the report and the firewall rule are about.
func DirectPort() int { return internet.DefaultPort }
