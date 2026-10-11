package main

import "testing"

func TestParseEventLine(t *testing.T) {
	cases := []struct {
		line, kind, text string
		minor            bool
	}{
		{"\x1b[90m8:47PM\x1b[0m \x1b[32mINF\x1b[0m \x1b[1mpeer left\x1b[0m \x1b[36mcomponent=\x1b[0mdaemon \x1b[36mpeer=\x1b[0m788d5efa0b86342c", "leave", "788d5efa0b86342c left", false},
		{"8:47PM INF internet: peer 788d5efa reachable at 127.0.0.1:36885 (punched-in) component=daemon", "link", "788d5efa reachable (punched-in)", false},
		{"20:47:01 INF internet: peer b4fd388c is trying to reach us; punching back component=daemon", "link", "b4fd388c is calling; answering", false},
		{"8:47PM INF internet: could not meet b4fd388c: no direct path to b4fd388c after trying 1 address(es) component=daemon", "nopath", "no direct path to b4fd388c", false},
		{"8:47PM INF internet: direct: 788d5efa connected from 1.2.3.4:5 component=daemon", "net", "direct: 788d5efa connected from 1.2.3.4:5", true},
		{`{"level":"info","peers":2,"time":"2026-10-11T20:47:06Z","message":"broadcasting closure to peers"}`, "env", "broadcasting closure to peers", false},
		{`{"level":"info","peer":"abcd1234","host":"pc-3","at":"10.0.0.3:38080","message":"peer up"}`, "join", "pc-3 is up (10.0.0.3:38080)", false},
		{`{"level":"info","peer":"abcd1234","host":"pc-3","message":"peer down"}`, "leave", "pc-3 is gone", false},
		{`{"level":"info","lease":"l1","job":"j1","submitter":"pc-2","mem":5,"message":"lease granted"}`, "work", "lease granted: job j1 from pc-2", false},
		{`{"level":"info","lease":"l1","status":"done","message":"lease released"}`, "work", "lease released (done)", false},
		{`{"level":"warn","lease":"l1","state":"active","message":"lease expired"}`, "warn", "lease expired (active)", false},
		{`{"level":"info","job":"j1","script":"a.py","gpu":false,"message":"job started"}`, "work", "job j1 started: a.py", false},
		{`{"level":"info","job":"j1","exit":0,"secs":1.5,"message":"job finished"}`, "work", "job j1 finished: exit 0 in 1.5 s", false},
		{`{"level":"info","job":"j1","secs":2,"message":"job cancelled"}`, "cancel", "job j1 cancelled after 2 s", false},
		{`{"level":"info","store":"/nix/store/x","secs":3,"how":"full","message":"closure imported"}`, "env", "environment imported (full, 3 s)", false},
		{`{"level":"info","message":"system nix now trusts these cluster keys"}`, "trust", "system nix now trusts these cluster keys", false},
		{"2026/10/11 20:47:02 [pool] chunk: 40 items -> 3 part(s), 2 remote, 1 local", "pool", "chunk: 40 items -> 3 part(s), 2 remote, 1 local", false},
		{"8:47PM INF daemon starting component=daemon max_concurrent=0 node_id=652290ff port=38080", "start", "daemon starting (port 38080)", false},
		{`22:28:00 INF peer has no direct path; not offering it work component=daemon peer=56acaf8942e73a3e reason="no answer from any address"`, "nopath", "56acaf8942e73a3e has no direct path (no answer from any address); not offering it work", false},
		{"2026/10/12 03:45:45 [pool] received pool/map from 127.0.0.1:34750: items=60 b64=false cache_keys=0 required_mem=6713398886 forwarded=\"\"", "pool", "received pool/map: 60 items", false},
		{"22:28:00 INF internet: peer 1777eb7a reachable at 127.0.0.1:35247 (punched-in) component=daemon", "link", "1777eb7a reachable (punched-in)", false},
		{`{"level":"warn","message":"withholding the cluster token"}`, "warn", "withholding the cluster token", true},
	}
	for _, c := range cases {
		e, ok := parseEventLine(c.line)
		if !ok || e.Kind != c.kind || e.Text != c.text || e.minor != c.minor {
			t.Errorf("%q\n got %+v ok=%v\nwant %s %q minor=%v", c.line, e, ok, c.kind, c.text, c.minor)
		}
	}
	if _, ok := parseEventLine("2026/10/11 20:47:02 connection doesn't allow setting"); ok {
		t.Error("stray stdlib line should not be an event")
	}
}

func TestCollapseAndNames(t *testing.T) {
	in := []clusterEvent{{Kind: "link", Text: "x"}, {Kind: "link", Text: "x"}, {Kind: "link", Text: "x"}, {Kind: "join", Text: "788d5efa0b86342c up"}}
	out := collapseEvents(in)
	if len(out) != 2 || out[0].Text != "x (×3)" {
		t.Fatalf("collapse: %+v", out)
	}
	got := out[1].withNames(map[string]string{"788d5efa": "pc-2"}).Text
	if got != "pc-2 up" {
		t.Fatalf("names: %q", got)
	}
	if s := (clusterEvent{Kind: "join", Text: "t"}).String(); s != "          JOIN   t" {
		t.Fatalf("string: %q", s)
	}
}

func TestFingerprintNamedViaForwarderAddr(t *testing.T) {
	names := map[string]string{"127.0.0.1:35247": "pc2"}
	e, _ := parseEventLine("22:28:00 INF internet: peer 1777eb7a reachable at 127.0.0.1:35247 (punched-in) component=daemon")
	e.learn(names)
	left := clusterEvent{Kind: "leave", Text: "1777eb7afcac95a8 left"}.withNames(names)
	if left.Text != "pc2 left" {
		t.Fatalf("got %q", left.Text)
	}
}
