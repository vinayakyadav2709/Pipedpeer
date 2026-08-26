package userdir

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHonoursXDGVars(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("XDG_CACHE_HOME", "/xdg/cache")
	t.Setenv("XDG_DATA_HOME", "/xdg/data")
	for _, tc := range []struct {
		got, want string
	}{
		{State(), "/xdg/state/pipedpeer"},
		{Cache(), "/xdg/cache/pipedpeer"},
		{Data(), "/xdg/data/pipedpeer"},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

func TestFallsBackToHomeNotTemp(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}
	// The whole point: without XDG set, these must land under the user's home
	// and not in a tmpfs that is wiped on reboot and charged against RAM.
	for _, got := range []string{State(), Cache(), Data()} {
		if !strings.HasPrefix(got, home) {
			t.Errorf("%q is not under the home directory", got)
		}
		if strings.HasPrefix(got, os.TempDir()) {
			t.Errorf("%q is in a temp dir", got)
		}
	}
}

func TestScratchIsOnDisk(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	dir, err := Scratch("thing-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if filepath.Dir(filepath.Dir(dir)) != Cache() {
		t.Errorf("scratch %q is not under the cache root %q", dir, Cache())
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("scratch dir not usable: %v", err)
	}
}
