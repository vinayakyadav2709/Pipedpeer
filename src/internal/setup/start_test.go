package setup

import (
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/invite"
)

// fakeDaemon records starts and the token each one saw.
type fakeDaemon struct {
	running bool
	starts  []string
}

func (f *fakeDaemon) install(t *testing.T) {
	t.Helper()
	oldS, oldStart, oldRe := daemonStatus, daemonStart, daemonRestart
	daemonStatus = func() daemonctl.State { return daemonctl.State{Running: f.running} }
	daemonStart = func(string, int, int) error {
		f.starts = append(f.starts, "start:"+authtoken.Current())
		f.running = true
		return nil
	}
	daemonRestart = func(string, int, int) error {
		f.starts = append(f.starts, "restart:"+authtoken.Current())
		return nil
	}
	t.Cleanup(func() { daemonStatus, daemonStart, daemonRestart = oldS, oldStart, oldRe })
}

func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv(authtoken.EnvVar, "")
	if err := authtoken.Set(""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
}

// TestSetupNeverStartsATokenlessDaemon (Q7): the secret exists before the
// daemon's first start, and that start is the only one.
func TestSetupNeverStartsATokenlessDaemon(t *testing.T) {
	isolate(t)
	f := &fakeDaemon{}
	f.install(t)
	created, err := startDaemon("n1", daemonctl.DefaultPort, false)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first machine did not create a cluster")
	}
	if len(f.starts) != 1 || f.starts[0] == "start:" {
		t.Fatalf("starts = %v; want one start with a token", f.starts)
	}
	// A second setup keeps the cluster and leaves the running daemon alone.
	created, err = startDaemon("n1", daemonctl.DefaultPort, false)
	if err != nil || created || len(f.starts) != 1 {
		t.Fatalf("second setup: created=%v err=%v starts=%v", created, err, f.starts)
	}
}

// TestTokenlessRunningDaemonIsRestartedOnce: an old daemon started without a
// secret is restarted once, with it.
func TestTokenlessRunningDaemonIsRestartedOnce(t *testing.T) {
	isolate(t)
	f := &fakeDaemon{running: true}
	f.install(t)
	if _, err := startDaemon("n1", daemonctl.DefaultPort, false); err != nil {
		t.Fatal(err)
	}
	if len(f.starts) != 1 || !strings.HasPrefix(f.starts[0], "restart:") || f.starts[0] == "restart:" {
		t.Fatalf("starts = %v", f.starts)
	}
}

func TestInviteCodeCarriesSecretAndAddrs(t *testing.T) {
	isolate(t)
	if err := authtoken.Set("sekrit"); err != nil {
		t.Fatal(err)
	}
	code, err := InviteCode()
	if err != nil {
		t.Fatal(err)
	}
	inv, err := invite.Decode(code)
	if err != nil || inv.Secret != "sekrit" {
		t.Fatalf("decode: %+v %v", inv, err)
	}
	got := inviteAddrs([]string{"10.0.0.2", "10.0.0.2"}, []string{"intro.example", "1.2.3.4:4244", "a", "b", "c", "d", "e"})
	if len(got) != maxInviteAddrs || got[0] != "10.0.0.2:4244" || got[1] != "intro.example:4244" {
		t.Fatalf("addrs = %v", got)
	}
}
