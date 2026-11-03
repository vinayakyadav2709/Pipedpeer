package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestConnectNoWarmupSkipsTheBackgroundBuild(t *testing.T) {
	if newConnectCmd().Flags().Lookup("no-warmup") == nil {
		t.Fatal("connect has no --no-warmup flag")
	}
	for _, tc := range []struct {
		name    string
		skip    bool
		err     error
		started bool
		say     string
	}{
		{"default builds it", false, nil, true, "building the common environment in the background"},
		{"--no-warmup skips it", true, nil, false, "not building the common environment (--no-warmup)"},
		{"a failed start is a warning", false, errors.New("boom"), true, "could not start building the common environment: boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			started := false
			warmupAfterConnect(&out, tc.skip, func() error { started = true; return tc.err })
			if started != tc.started || !strings.Contains(out.String(), tc.say) {
				t.Fatalf("started=%v out=%q, want started=%v and %q", started, out.String(), tc.started, tc.say)
			}
		})
	}
}
