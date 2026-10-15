package main

import (
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/internet"
)

// B4: a daemon started with no flags reads its cluster from cluster.json.
func TestDaemonUsesSavedCluster(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir()) // userdir.Data() root
	// The token half is authtoken.Current(), which a test binary never reads
	// from disk (authtoken.underTest), so only the file half is checked.
	want := clustercfg.Config{Token: "tok", Introducers: []string{"35.234.222.177:4244"}, NoJobs: true}
	if err := clustercfg.Save(want); err != nil {
		t.Fatal(err)
	}
	cfg, _ := daemonCluster()
	if len(cfg.Introducers) != 1 || cfg.Introducers[0] != "35.234.222.177:4244" {
		t.Errorf("introducers: got %v", cfg.Introducers)
	}
	if r := daemonRole(cfg); r != heartbeat.RoleIntroducerOnly {
		t.Errorf("role: got %q", r)
	}
}

func TestDaemonRole(t *testing.T) {
	cases := []struct {
		cfg  clustercfg.Config
		want string
	}{
		{clustercfg.Config{}, ""},
		{clustercfg.Config{PoolOnly: true}, heartbeat.RolePoolOnly},
		{clustercfg.Config{NoJobs: true, PoolOnly: true}, heartbeat.RoleIntroducerOnly},
	}
	for _, c := range cases {
		if got := daemonRole(c.cfg); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.cfg, got, c.want)
		}
	}
}

// Only introducers that answered, and never our own loopback one, are kept.
func TestAnsweredIntroducers(t *testing.T) {
	now := time.Now()
	got := answeredIntroducers([]internet.IntroducerState{
		{Addr: "1.2.3.4:4244", LastReply: now},
		{Addr: "5.6.7.8:4244", LastReply: now, LastErr: "timeout"},
		{Addr: "9.9.9.9:4244"},
		{Addr: "127.0.0.1:4244", LastReply: now},
	})
	if len(got) != 1 || got[0] != "1.2.3.4:4244" {
		t.Errorf("got %v", got)
	}
}
