package setup

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// fakeFirewall drives the firewall logic without one being installed, which
// is the only way these paths run at all on most machines - the ufw path
// would otherwise never run on a Fedora developer's laptop, nor firewalld on
// an Ubuntu one.
type fakeFirewall struct {
	present map[string]bool
	replies map[string]string
	fails   map[string]bool
	ran     []string
}

func (f *fakeFirewall) install(t *testing.T) {
	t.Helper()
	oldLook, oldCmd := lookPath, firewallCmd
	lookPath = func(name string) (string, error) {
		if f.present[name] {
			return "/usr/bin/" + name, nil
		}
		return "", fmt.Errorf("not found")
	}
	firewallCmd = func(_ context.Context, name string, args ...string) (string, error) {
		key := strings.Join(append([]string{name}, args...), " ")
		f.ran = append(f.ran, key)
		if f.fails[key] {
			return "refused", fmt.Errorf("exit status 1")
		}
		if out, ok := f.replies[key]; ok {
			return out, nil
		}
		return "", nil
	}
	t.Cleanup(func() { lookPath, firewallCmd = oldLook, oldCmd })
}

func (f *fakeFirewall) didRun(want string) bool {
	for _, r := range f.ran {
		if r == want {
			return true
		}
	}
	return false
}

// TestAnActiveFirewallIsOpenedForTheDirectPort.
//
// Fedora ships firewalld on by default. A punch the router allowed dies at
// the host firewall instead, which is maddening precisely because the hard
// part worked - so setup opens the one port, and says so.
func TestAnActiveFirewallIsOpenedForTheDirectPort(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"firewall-cmd": true},
		replies: map[string]string{
			"firewall-cmd --state":                "running\n",
			"firewall-cmd --query-port=38447/udp": "no\n",
		},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, true)
	if !st.open || !st.changed {
		t.Fatalf("port not opened: %+v", st)
	}
	if st.tool != "firewalld" {
		t.Errorf("tool = %q, want firewalld", st.tool)
	}
	// Permanent, or the rule vanishes on reboot and fails in a way nobody
	// connects to the reboot.
	if !f.didRun("firewall-cmd --permanent --add-port=38447/udp") {
		t.Errorf("the rule was not made permanent; ran %v", f.ran)
	}
	if !f.didRun("firewall-cmd --reload") {
		t.Errorf("firewalld was not reloaded, so the rule is not in effect yet")
	}
	// And the user is told how to undo what was done to their machine.
	if !strings.Contains(st.undo, "remove-port=38447/udp") {
		t.Errorf("undo = %q, which does not reverse the change", st.undo)
	}
}

// TestAPortThatIsAlreadyOpenIsLeftAlone. Setup should not keep editing a
// machine that is already correct.
func TestAPortThatIsAlreadyOpenIsLeftAlone(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"firewall-cmd": true},
		replies: map[string]string{
			"firewall-cmd --state":                "running\n",
			"firewall-cmd --query-port=38447/udp": "yes\n",
		},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, true)
	if !st.open {
		t.Error("an already-open port was reported closed")
	}
	if st.changed {
		t.Error("an already-open port was changed anyway")
	}
	if f.didRun("firewall-cmd --permanent --add-port=38447/udp") {
		t.Errorf("a rule was added that was already there; ran %v", f.ran)
	}
}

// TestNothingIsChangedWithoutConsent. Without -y setup reports and does not
// touch the machine.
func TestNothingIsChangedWithoutConsent(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"firewall-cmd": true},
		replies: map[string]string{
			"firewall-cmd --state":                "running\n",
			"firewall-cmd --query-port=38447/udp": "no\n",
		},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, false)
	if st.changed {
		t.Error("the firewall was changed without consent")
	}
	if st.open {
		t.Error("a blocked port was reported open")
	}
	for _, r := range f.ran {
		if strings.Contains(r, "add-port") {
			t.Errorf("a rule was added without consent: %s", r)
		}
	}
}

// TestUFWIsHandledToo. The other common firewall, and the one a Fedora
// machine would never exercise.
func TestUFWIsHandledToo(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"ufw": true},
		replies: map[string]string{"ufw status": "Status: active\n"},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, true)
	if st.tool != "ufw" || !st.changed {
		t.Fatalf("ufw not handled: %+v", st)
	}
	if !f.didRun("ufw allow 38447/udp") {
		t.Errorf("ufw was not asked to allow the port; ran %v", f.ran)
	}
	if !strings.Contains(st.undo, "delete allow") {
		t.Errorf("undo = %q, which does not reverse the change", st.undo)
	}
}

// TestAnInactiveFirewallIsNotTouched. A firewall that is installed but not
// running blocks nothing, and adding rules to it would be changing a machine
// for no reason.
func TestAnInactiveFirewallIsNotTouched(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"firewall-cmd": true, "ufw": true},
		replies: map[string]string{
			"firewall-cmd --state": "not running\n",
			"ufw status":           "Status: inactive\n",
		},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, true)
	if st.tool != "" {
		t.Errorf("tool = %q, want none: neither firewall is running", st.tool)
	}
	if !st.open {
		t.Error("the port was reported blocked though no firewall is running")
	}
	if st.changed {
		t.Error("an inactive firewall was modified")
	}
}

// TestAFailureToOpenIsReportedNotSwallowed. A machine where this did not work
// will silently never receive a direct connection, so the failure has to
// reach the user.
func TestAFailureToOpenIsReportedNotSwallowed(t *testing.T) {
	f := &fakeFirewall{
		present: map[string]bool{"firewall-cmd": true},
		replies: map[string]string{
			"firewall-cmd --state":                "running\n",
			"firewall-cmd --query-port=38447/udp": "no\n",
		},
		fails: map[string]bool{"firewall-cmd --permanent --add-port=38447/udp": true},
	}
	f.install(t)

	st := openFirewall(context.Background(), 38447, true)
	if st.err == nil {
		t.Fatal("a failed rule change was reported as success")
	}
	if st.open {
		t.Error("the port was reported open after the change failed")
	}
}

// TestTheReportedPortIsTheOneUsed. A report about a different port than the
// daemon listens on is worse than none.
func TestTheReportedPortIsTheOneUsed(t *testing.T) {
	if DirectPort() != 38447 {
		t.Errorf("DirectPort() = %d; it must match internet.DefaultPort, or setup "+
			"opens a port nothing listens on", DirectPort())
	}
}
