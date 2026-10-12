package clustercfg

import (
	"os"
	"strconv"
	"testing"
)

// A machine that has joined a cluster must still be in it after a restart.
//
// The address used to live only in a flag and an environment variable, so a
// daemon restarted by a reboot, by `pipedpeer stop; pipedpeer start`, or by
// anything that did not know to repeat the flag came back alone and quietly
// stopped taking work. Nothing failed; the node simply sat there looking
// healthy and idle.
func TestAJoinedClusterSurvivesARestart(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if got := Rendezvous(); got != "" {
		t.Fatalf("a fresh machine claims to be in cluster %q", got)
	}
	if err := SetRendezvous("35.234.222.177:38445"); err != nil {
		t.Fatal(err)
	}
	if got := Rendezvous(); got != "35.234.222.177:38445" {
		t.Errorf("after joining, Rendezvous() = %q", got)
	}
	// What the next start sees, with no flag and no environment.
	os.Unsetenv(EnvVar)
	addr, source := Effective("")
	if addr != "35.234.222.177:38445" || source != "saved" {
		t.Errorf("a later start got (%q, %q), want the saved address", addr, source)
	}
}

// A flag is how somebody points one machine at a different cluster, so it has
// to beat both the environment and what was saved.
func TestWhatWasTypedBeatsWhatWasRemembered(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := SetRendezvous("saved.example:38445"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvVar, "env.example:38445")

	if addr, src := Effective("flag.example:38445"); addr != "flag.example:38445" || src != "flag" {
		t.Errorf("flag lost: got (%q, %q)", addr, src)
	}
	if addr, src := Effective(""); addr != "env.example:38445" || src != "environment" {
		t.Errorf("environment lost to the saved value: got (%q, %q)", addr, src)
	}
	os.Unsetenv(EnvVar)
	if addr, src := Effective(""); addr != "saved.example:38445" || src != "saved" {
		t.Errorf("saved value not used: got (%q, %q)", addr, src)
	}
}

// The port is an implementation detail somebody was told once. Typing the
// address of a machine is how people think about joining.
func TestABareHostGetsTheDefaultPort(t *testing.T) {
	def := ":" + strconv.Itoa(DefaultPort)
	for in, want := range map[string]string{
		"35.234.222.177":        "35.234.222.177" + def,
		"demo.example.com":      "demo.example.com" + def,
		"35.234.222.177:38445":  "35.234.222.177:38445",
		"demo.example.com:9999": "demo.example.com:9999",
		// Pasted from a browser or a chat message.
		"http://35.234.222.177": "35.234.222.177" + def,
		"https://demo.example/": "demo.example" + def,
		"  35.234.222.177  ":    "35.234.222.177" + def,
		// IPv6 needs brackets before it can carry a port.
		"2001:db8::1":       "[2001:db8::1]" + def,
		"[2001:db8::1]":     "[2001:db8::1]" + def,
		"[2001:db8::1]:999": "[2001:db8::1]:999",
	} {
		got, err := Normalize(in)
		if err != nil {
			t.Errorf("Normalize(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

// Refused rather than saved, because a bad address is only discovered later
// as a machine that never meets anybody.
func TestAnUnusableAddressIsRefused(t *testing.T) {
	for _, in := range []string{"", "   ", ":38445", "host:notaport", "[2001:db8::1"} {
		if got, err := Normalize(in); err == nil {
			t.Errorf("Normalize(%q) accepted it as %q", in, got)
		}
	}
}
