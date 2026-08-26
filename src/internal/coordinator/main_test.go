package coordinator

import (
	"os"
	"testing"
)

// TestMain isolates the token store. authtoken reads the operator's token
// from XDG_DATA_HOME, so on a machine where one is configured every test that
// starts or calls a daemon begins getting 401s — a failure about the
// developer's machine rather than the code, and one that presents as a
// timeout rather than an error.
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
