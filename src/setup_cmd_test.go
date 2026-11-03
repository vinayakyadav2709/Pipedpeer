package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/invite"
	"github.com/pipedpeer/pipedpeer/internal/setup"
	"github.com/pipedpeer/pipedpeer/internal/warmup"
)

// stubSetup replaces the three seams and counts calls.
func stubSetup(t *testing.T, port int, outcome connOutcome) (warm, conn *[]string) {
	t.Helper()
	w, c, _ := stubSetupRuns(t, port, outcome)
	return w, c
}

// stubSetupRuns also counts how often setup itself ran.
func stubSetupRuns(t *testing.T, port int, outcome connOutcome) (warm, conn *[]string, runs *int) {
	t.Helper()
	w, c, n := &[]string{}, &[]string{}, new(int)
	oldRun, oldConn, oldWarm := setupRun, runConnectFn, startWarmupFn
	t.Cleanup(func() { setupRun, runConnectFn, startWarmupFn = oldRun, oldConn, oldWarm })
	setupRun = func(setup.Options) (int, error) { *n++; return port, nil }
	runConnectFn = func(_ io.Writer, code string, warm bool) (connOutcome, error) {
		*c = append(*c, fmt.Sprintf("%s warm=%v", code, warm))
		return outcome, nil
	}
	startWarmupFn = func(io.Writer) { *w = append(*w, "x") }
	return w, c, n
}

var goodCode = invite.Encode(invite.Invite{Secret: "s3cret", Addrs: []string{"203.0.113.5:4242"}})

func TestRunSetupWarmupAndConnect(t *testing.T) {
	cases := []struct {
		name      string
		port      int
		code      string
		noInstall bool
		outcome   connOutcome
		warm      int
		conn      []string
		wantExit  connOutcome
		wantPrint string
		wantNot   string
	}{
		{"1 no code", 4242, "", false, connConnected, 1, nil, connConnected, "", ""},
		{"2 code", 4242, goodCode, false, connConnected, 0, []string{goodCode + " warm=true"}, connConnected, "", ""},
		{"3 declined", 0, "", false, connConnected, 0, nil, connConnected, "", ""},
		// H1: setup did not finish, so the join did not run: not a success.
		{"4 declined with code", 0, goodCode, false, connConnected, 0, nil, connFailed, "pipedpeer connect <code>", "s3cret"},
		{"5 partly", 4242, goodCode, false, connPartly, 0, []string{goodCode + " warm=true"}, connPartly, "", ""},
		// M1: check-only mode starts no build, joins without one.
		{"8 no-install, no code", 4242, "", true, connConnected, 0, nil, connConnected, "", ""},
		{"9 no-install, code", 4242, goodCode, true, connConnected, 0, []string{goodCode + " warm=false"}, connConnected, "", ""},
		// H2: a bad code stops before any system change.
		{"10 bad code", 4242, "garbage", false, connConnected, 0, nil, connFailed, "not connected", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			warm, conn, runs := stubSetupRuns(t, c.port, c.outcome)
			var out bytes.Buffer
			got, err := runSetup(&out, setup.Options{Port: 4242, NoInstall: c.noInstall, Joining: c.code != ""}, c.code)
			if err != nil || got != c.wantExit {
				t.Fatalf("outcome %v err %v, want %v", got, err, c.wantExit)
			}
			if len(*warm) != c.warm || strings.Join(*conn, ",") != strings.Join(c.conn, ",") {
				t.Fatalf("warm %d conn %v, want %d %v", len(*warm), *conn, c.warm, c.conn)
			}
			if c.name == "10 bad code" && *runs != 0 {
				t.Fatalf("setup ran %d times with a bad code", *runs)
			}
			if c.name != "10 bad code" && *runs != 1 {
				t.Fatalf("setup ran %d times, want 1", *runs)
			}
			if !strings.Contains(out.String(), c.wantPrint) {
				t.Fatalf("output %q lacks %q", out.String(), c.wantPrint)
			}
			if c.wantNot != "" && strings.Contains(out.String(), c.wantNot) {
				t.Fatalf("output %q leaks %q", out.String(), c.wantNot)
			}
		})
	}
}

// TestStartWarmupMessages (L4): the line says which thing happened.
func TestStartWarmupMessages(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	for _, c := range []struct {
		res  warmup.Result
		want string
	}{
		{warmup.Started, "building in the background"},
		{warmup.AlreadyRunning, "already building"},
		{warmup.AlreadyDone, "already built"},
		{warmup.RecentlyFailed, "failed a moment ago"},
	} {
		old := warmupStart
		warmupStart = func(string, string, bool) (warmup.Result, error) { return c.res, nil }
		var out bytes.Buffer
		startWarmup(&out)
		warmupStart = old
		if !strings.Contains(out.String(), c.want) {
			t.Errorf("%v: output %q lacks %q", c.res, out.String(), c.want)
		}
	}
}

func TestStartWarmupSkipsMachinesThatCannotBuild(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  clustercfg.Config
		want string
	}{
		{"6 introducer-only", clustercfg.Config{NoJobs: true}, "introducer-only"},
		{"7 pool-only", clustercfg.Config{PoolOnly: true}, "pool-only"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := clustercfg.Save(c.cfg); err != nil {
				t.Fatal(err)
			}
			old := warmupStart
			t.Cleanup(func() { warmupStart = old })
			started := 0
			warmupStart = func(string, string, bool) (warmup.Result, error) { started++; return warmup.Started, nil }
			var out bytes.Buffer
			startWarmup(&out)
			if started != 0 || !strings.Contains(out.String(), c.want) {
				t.Fatalf("started %d, output %q", started, out.String())
			}
		})
	}
}

func TestConnectBadCodeFails(t *testing.T) { // row 11
	var out bytes.Buffer
	got, err := runConnect(&out, "not-a-code", true)
	if err != nil || got != connFailed {
		t.Fatalf("got %v %v, want connFailed", got, err)
	}
	if !strings.Contains(out.String(), "not connected") || !strings.Contains(out.String(), "copy the whole") {
		t.Fatalf("messages missing: %q", out.String())
	}
}

func TestTakeConnectCodeEnv(t *testing.T) {
	cases := []struct {
		name, code, marker, want string
		warns                    bool
	}{
		{"installer code with marker is used", "PP1-x", "1", "PP1-x", false},
		{"stale profile code without marker is ignored", "PP1-x", "", "", true},
		{"no code", "", "1", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(connectCodeEnv, c.code)
			t.Setenv(connectMarkerEnv, c.marker)
			var w strings.Builder
			if got := takeConnectCodeEnv(&w); got != c.want {
				t.Fatalf("code = %q, want %q", got, c.want)
			}
			if (w.Len() > 0) != c.warns {
				t.Fatalf("warning = %q, want warn=%v", w.String(), c.warns)
			}
			if os.Getenv(connectCodeEnv) != "" || os.Getenv(connectMarkerEnv) != "" {
				t.Fatal("variables must be unset after reading (children would inherit the secret)")
			}
		})
	}
}
