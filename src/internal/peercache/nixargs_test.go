package peercache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type routeCase struct {
	name    string
	port    int
	multi   bool
	listed  bool
	keys    string
	status  int
	body    string
	closed  bool
	want    []string
	logWant string // substring in the log; "" = none required
	noProbe bool
}

func TestNixArgs(t *testing.T) {
	url4242 := "http://127.0.0.1:4242/nix-cache"
	url4250 := "http://127.0.0.1:4250/nix-cache"
	info := CacheInfo
	cases := []routeCase{
		{name: "1 port 0", port: 0, status: 200, body: info, noProbe: true},
		{name: "2 private, keys", port: 4242, keys: "k1 k2", status: 200, body: info,
			want: []string{"--option", "extra-substituters", url4242, "--option", "extra-trusted-public-keys", "k1 k2",
				"--option", "narinfo-cache-negative-ttl", "0", "--option", "fallback", "true"}},
		{name: "3 single-user, no keys file", port: 4250, status: 200, body: info,
			want: []string{"--option", "extra-substituters", url4250, "--option", "narinfo-cache-negative-ttl", "0",
				"--option", "fallback", "true"}},
		{name: "4 multi-user, listed", port: 4242, multi: true, listed: true, status: 200, body: info,
			want: []string{"--option", "extra-substituters", url4242, "--option", "fallback", "true"}},
		{name: "5 multi-user, not listed", port: 4242, multi: true, status: 200, body: info, logWant: "pipedpeer setup", noProbe: true},
		{name: "6 multi-user, listed, port 4250", port: 4250, multi: true, listed: true, status: 200, body: info, logWant: "default daemon port", noProbe: true},
		{name: "7a probe 404", port: 4242, status: 404, body: "no", logWant: "peer cache off"},
		{name: "7b connection refused", port: 4242, closed: true, logWant: "peer cache off"},
		{name: "8 not a nix cache", port: 4242, status: 200, body: "<html>", logWant: "not a nix cache"},
	}
	oldM, oldL, oldK, oldP := multiUser, substituterListed, clusterKeys, probeBase
	oldLog := log.Logger
	defer func() {
		multiUser, substituterListed, clusterKeys, probeBase, log.Logger = oldM, oldL, oldK, oldP, oldLog
	}()

	for _, c := range cases {
		probes := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			probes++
			if r.URL.Path != "/nix-cache/nix-cache-info" {
				t.Errorf("%s: probed %s", c.name, r.URL.Path)
			}
			w.WriteHeader(c.status)
			w.Write([]byte(c.body))
		}))
		base := srv.URL
		if c.closed {
			srv.Close()
		}
		multiUser = func() bool { return c.multi }
		substituterListed = func() bool { return c.listed }
		clusterKeys = func() string { return c.keys }
		probeBase = func(int) string { return base }
		var logs bytes.Buffer
		log.Logger = zerolog.New(&logs)

		got := NixArgs(c.port)
		srv.Close()
		if strings.Join(got, "\x00") != strings.Join(c.want, "\x00") {
			t.Errorf("%s: args %q, want %q", c.name, got, c.want)
		}
		if c.noProbe && probes != 0 {
			t.Errorf("%s: probed %d times, want none", c.name, probes)
		}
		if c.logWant != "" && !strings.Contains(logs.String(), c.logWant) {
			t.Errorf("%s: log %q lacks %q", c.name, logs.String(), c.logWant)
		}
		if c.logWant != "" && strings.Count(logs.String(), "\n") != 1 {
			t.Errorf("%s: want one log line, got %q", c.name, logs.String())
		}
	}
}
