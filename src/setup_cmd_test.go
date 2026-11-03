package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/setup"
)

// stubSetup replaces the three seams and counts calls.
func stubSetup(t *testing.T, port int, outcome connOutcome) (warm, conn *[]string) {
	t.Helper()
	w, c := &[]string{}, &[]string{}
	oldRun, oldConn, oldWarm := setupRun, runConnectFn, startWarmupFn
	t.Cleanup(func() { setupRun, runConnectFn, startWarmupFn = oldRun, oldConn, oldWarm })
	setupRun = func(setup.Options) (int, error) { return port, nil }
	runConnectFn = func(_ io.Writer, code string) (connOutcome, error) {
		*c = append(*c, code)
		return outcome, nil
	}
	startWarmupFn = func(io.Writer) { *w = append(*w, "x") }
	return w, c
}

func TestRunSetupWarmupAndConnect(t *testing.T) {
	cases := []struct {
		name      string
		port      int
		code      string
		outcome   connOutcome
		warm      int
		conn      []string
		wantExit  connOutcome
		wantPrint string
	}{
		{"1 no code", 4242, "", connConnected, 1, nil, connConnected, ""},
		{"2 code", 4242, "PP1-x", connConnected, 0, []string{"PP1-x"}, connConnected, ""},
		{"3 declined", 0, "", connConnected, 0, nil, connConnected, ""},
		{"4 declined with code", 0, "X", connConnected, 0, nil, connConnected, "pipedpeer connect X"},
		{"5 partly", 4242, "PP1-x", connPartly, 0, []string{"PP1-x"}, connPartly, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			warm, conn := stubSetup(t, c.port, c.outcome)
			var out bytes.Buffer
			got, err := runSetup(&out, setup.Options{Port: 4242, Joining: c.code != ""}, c.code)
			if err != nil || got != c.wantExit {
				t.Fatalf("outcome %v err %v, want %v", got, err, c.wantExit)
			}
			if len(*warm) != c.warm || strings.Join(*conn, ",") != strings.Join(c.conn, ",") {
				t.Fatalf("warm %d conn %v, want %d %v", len(*warm), *conn, c.warm, c.conn)
			}
			if !strings.Contains(out.String(), c.wantPrint) {
				t.Fatalf("output %q lacks %q", out.String(), c.wantPrint)
			}
		})
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
			warmupStart = func(string, string, bool) error { started++; return nil }
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
	got, err := runConnect(&out, "not-a-code")
	if err != nil || got != connFailed {
		t.Fatalf("got %v %v, want connFailed", got, err)
	}
	if !strings.Contains(out.String(), "not connected") || !strings.Contains(out.String(), "copy the whole") {
		t.Fatalf("messages missing: %q", out.String())
	}
}
