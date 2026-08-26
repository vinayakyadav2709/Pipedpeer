package daemonapi

import (
	"os"
	"testing"
)

// TestMain isolates the token store.
//
// authtoken reads the operator's token from XDG_DATA_HOME, so on a machine
// where someone has actually configured one, every test that starts a server
// begins refusing its own requests with 401 — a failure about the developer's
// machine rather than the code. Tests get an empty directory and therefore no
// token; the ones that care about auth set their own.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "pipedpeer-test-xdg-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	os.Setenv("XDG_DATA_HOME", dir)
	os.Unsetenv("PIPEDPEER_TOKEN")
	os.Exit(m.Run())
}
