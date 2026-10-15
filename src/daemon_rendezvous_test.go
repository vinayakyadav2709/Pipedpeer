package main

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
)

// B4: a daemon started with no flag and no env registers with the saved cluster.
func TestDaemonUsesSavedCluster(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir()) // userdir.Data() root
	if err := clustercfg.SetRendezvous("35.234.222.177:38445"); err != nil {
		t.Fatal(err)
	}
	if got := daemonRendezvous(""); got != "35.234.222.177:38445" {
		t.Errorf("no flag: got %q, want saved", got)
	}
	if got := daemonRendezvous("5.6.7.8:1"); got != "5.6.7.8:1" {
		t.Errorf("flag: got %q", got)
	}
}
