package main

import (
	"os"
	"testing"
)

// TestMain isolates the token store. The CLI tests start real daemons, and
// authtoken reads the operator's token from XDG_DATA_HOME; on a machine where
// one is configured those daemons start refusing the tests' own requests.
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
