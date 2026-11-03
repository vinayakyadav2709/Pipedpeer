package setup

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
)

type subEnv struct {
	multi, listed, ready bool
	syncErrs             []error // returned by successive Sync calls
	answer               bool
	syncs, installs      int
	asked                int
}

func newSubEnv(t *testing.T) *subEnv {
	e := &subEnv{answer: true}
	om, ol, or, os, oa, oi := multiUser, substituterListed, helperReady, helperSync, askYes, installHelperFn
	t.Cleanup(func() {
		multiUser, substituterListed, helperReady, helperSync, askYes, installHelperFn = om, ol, or, os, oa, oi
	})
	multiUser = func() bool { return e.multi }
	substituterListed = func() bool { return e.listed }
	helperReady = func() bool { return e.ready }
	helperSync = func() error {
		var err error
		if e.syncs < len(e.syncErrs) {
			err = e.syncErrs[e.syncs]
		}
		e.syncs++
		return err
	}
	askYes = func(string) bool { e.asked++; return e.answer }
	installHelperFn = func() error { e.installs++; return nil }
	return e
}

func TestEnsureSubstituter(t *testing.T) {
	tests := []struct {
		name                    string
		set                     func(*subEnv)
		interactive, autoYes    bool
		wantErr                 error
		wantSyncs, wantInstalls int
		wantOut                 string
	}{
		{"9 single-user", func(e *subEnv) {}, true, false, nil, 0, 0, ""},
		{"10 listed", func(e *subEnv) { e.multi, e.listed = true, true }, true, false, nil, 0, 0, ""},
		{"11 ready sync ok", func(e *subEnv) { e.multi, e.ready = true, true }, false, false, nil, 1, 0, ""},
		{"12 old helper autoYes", func(e *subEnv) {
			e.multi, e.ready = true, true
			e.syncErrs = []error{nixtrust.ErrOldHelper}
		}, true, true, nil, 2, 1, ""},
		{"13 old helper non-interactive", func(e *subEnv) {
			e.multi, e.ready = true, true
			e.syncErrs = []error{nixtrust.ErrOldHelper}
		}, false, false, ErrNeedsSetup, 1, 0, ""},
		{"13b no helper non-interactive", func(e *subEnv) { e.multi = true }, false, false, ErrNeedsSetup, 0, 0, ""},
		{"14 user says no", func(e *subEnv) { e.multi, e.answer = true, false }, true, false, nil, 0, 0,
			"Peers' builds stay off on this machine until `pipedpeer setup`"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newSubEnv(t)
			tc.set(e)
			var out bytes.Buffer
			err := EnsureSubstituter(&out, tc.interactive, tc.autoYes)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if e.syncs != tc.wantSyncs || e.installs != tc.wantInstalls {
				t.Errorf("syncs=%d installs=%d, want %d %d", e.syncs, e.installs, tc.wantSyncs, tc.wantInstalls)
			}
			if tc.autoYes && e.asked != 0 {
				t.Error("asked despite autoYes")
			}
			if !strings.Contains(out.String(), tc.wantOut) {
				t.Errorf("output %q lacks %q", out.String(), tc.wantOut)
			}
		})
	}
}

func TestEnsureSubstituterReturnsAFailedSyncWrapped(t *testing.T) {
	e := newSubEnv(t)
	e.multi, e.ready = true, true
	boom := errors.New("boom")
	e.syncErrs = []error{boom}
	if err := EnsureSubstituter(&bytes.Buffer{}, true, true); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
}
