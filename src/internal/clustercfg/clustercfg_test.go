package clustercfg

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

func TestAJoinedClusterSurvivesARestart(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if got := Rendezvous(); got != "" {
		t.Fatalf("a fresh machine claims to be in cluster %q", got)
	}
	if err := SetRendezvous("35.234.222.177:4244"); err != nil {
		t.Fatal(err)
	}
	addr, source := Effective("")
	if addr != "35.234.222.177:4244" || source != "saved" {
		t.Errorf("a later start got (%q, %q), want the saved address", addr, source)
	}
	if addr, src := Effective("flag.example:4244"); addr != "flag.example:4244" || src != "flag" {
		t.Errorf("flag lost: got (%q, %q)", addr, src)
	}
}

func TestMigratesOldRendezvousAndAuthToken(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dir)
	if err := os.MkdirAll(userdir.Data(), 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(Path(), []byte(`{"rendezvous":"1.2.3.4:4244"}`), 0o600)
	os.WriteFile(legacyTokenPath(), []byte("sekret\n"), 0o600)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Token != "sekret" || len(c.Introducers) != 1 || c.Introducers[0] != "1.2.3.4:4244" {
		t.Fatalf("migrated config = %+v", c)
	}
	if _, err := os.Stat(legacyTokenPath()); !os.IsNotExist(err) {
		t.Errorf("auth_token should be deleted after a successful save, stat err = %v", err)
	}
	b, _ := os.ReadFile(Path())
	if strings.Contains(string(b), "rendezvous") {
		t.Errorf("saved file still has rendezvous: %s", b)
	}
	if c2, _ := Load(); c2.Token != "sekret" || len(c2.Introducers) != 1 {
		t.Errorf("second load = %+v", c2)
	}
}

func TestRememberIntroducersDedupesAndKeepsEight(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	for i := 0; i < 10; i++ {
		if err := RememberIntroducers("h" + strconv.Itoa(i) + ":4244"); err != nil {
			t.Fatal(err)
		}
	}
	RememberIntroducers("h5:4244")
	c, _ := Load()
	if len(c.Introducers) != 8 || c.Introducers[7] != "h5:4244" || c.Introducers[0] != "h2:4244" {
		t.Errorf("introducers = %v", c.Introducers)
	}
}

// The port is an implementation detail somebody was told once. Typing the
// address of a machine is how people think about joining.
func TestABareHostGetsTheDefaultPort(t *testing.T) {
	def := ":" + strconv.Itoa(DefaultPort)
	for in, want := range map[string]string{
		"35.234.222.177":        "35.234.222.177" + def,
		"demo.example.com":      "demo.example.com" + def,
		"35.234.222.177:4244":   "35.234.222.177:4244",
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
	for _, in := range []string{"", "   ", ":4244", "host:notaport", "[2001:db8::1"} {
		if got, err := Normalize(in); err == nil {
			t.Errorf("Normalize(%q) accepted it as %q", in, got)
		}
	}
}
