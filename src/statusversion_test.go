package main

import (
	"errors"
	"testing"
)

func TestStatusVersion(t *testing.T) {
	cases := []struct {
		name   string
		cli    string
		daemon string
		err    error
		shown  string
		warn   string
	}{
		{"same build", "0.0.1-a", "0.0.1-a", nil, "0.0.1-a", ""},
		{"newer CLI", "0.0.1-b", "0.0.1-a", nil, "0.0.1-a (this CLI: 0.0.1-b)", ""},
		{"older daemon", "0.0.1-b", "", nil, "unknown (this CLI: 0.0.1-b)",
			"pipedpeer: the daemon does not report its version (older build)"},
		{"unreadable", "0.0.1-b", "", errors.New("connection refused"),
			"unknown (this CLI: 0.0.1-b)",
			"pipedpeer: could not read the daemon's version: connection refused"},
		{"dev build", "dev", "dev", nil, "dev", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			shown, warn := statusVersion(c.cli, c.daemon, c.err)
			if shown != c.shown {
				t.Errorf("shown = %q; want %q", shown, c.shown)
			}
			if warn != c.warn {
				t.Errorf("warn = %q; want %q", warn, c.warn)
			}
		})
	}
}
