package daemonapi

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func fakeSystemTrust(t *testing.T, installed bool, addErr error) (asked, added *[][]string, trusted map[string]bool) {
	t.Helper()
	su, hi, ha := systemUntrusted, helperInstalled, helperAdd
	t.Cleanup(func() { systemUntrusted, helperInstalled, helperAdd = su, hi, ha })
	asked, added, trusted = new([][]string), new([][]string), map[string]bool{"self": true}
	systemUntrusted = func(keys []string) ([]string, bool, error) {
		*asked = append(*asked, keys)
		var missing []string
		for _, k := range keys {
			if !trusted[k] {
				missing = append(missing, k)
			}
		}
		return missing, true, nil
	}
	helperInstalled = func() bool { return installed }
	helperAdd = func(keys []string) error {
		*added = append(*added, keys)
		if addErr != nil {
			return addErr
		}
		for _, k := range keys {
			trusted[k] = true
		}
		return nil
	}
	return asked, added, trusted
}

// TestAnUntrustedClusterKeyIsWarnedAboutOnce: without the key helper the
// daemon names `pipedpeer trust` for each key system nix refuses, once per
// retry interval, and does not ask nix again for keys it has settled.
func TestAnUntrustedClusterKeyIsWarnedAboutOnce(t *testing.T) {
	asked, added, _ := fakeSystemTrust(t, false, nil)

	s := &Server{}
	if got := s.warnUntrustedKeys([]string{"self", "peer-b"}); !reflect.DeepEqual(got, []string{"peer-b"}) {
		t.Fatalf("warned about %v, want [peer-b]", got)
	}
	if got := s.warnUntrustedKeys([]string{"self", "peer-b"}); got != nil {
		t.Errorf("warned again about %v on the next poll", got)
	}
	s.warnUntrustedKeys([]string{"self", "peer-b", "peer-c"})
	if len(*asked) != 2 || !reflect.DeepEqual((*asked)[1], []string{"peer-c"}) {
		t.Errorf("nix was asked %v; want only the new key on the second check", *asked)
	}
	if len(*added) != 0 {
		t.Errorf("called a helper that is not installed: %v", *added)
	}

	// Past the retry interval the refused key is tried again; "self" is not.
	s.trustNext["peer-b"] = time.Now().Add(-time.Second)
	s.warnUntrustedKeys([]string{"self", "peer-b", "peer-c"})
	if len(*asked) != 3 || !reflect.DeepEqual((*asked)[2], []string{"peer-b"}) {
		t.Errorf("retry asked %v, want [peer-b]", *asked)
	}
}

// TestTheHelperAddsANewPeersKeyWithoutAPrompt: with the helper installed a
// new key is added through it and never warned about or retried.
func TestTheHelperAddsANewPeersKeyWithoutAPrompt(t *testing.T) {
	asked, added, _ := fakeSystemTrust(t, true, nil)
	s := &Server{}
	if got := s.warnUntrustedKeys([]string{"self", "peer-b"}); got != nil {
		t.Fatalf("warned about %v although the helper added it", got)
	}
	if !reflect.DeepEqual(*added, [][]string{{"peer-b"}}) {
		t.Fatalf("helper added %v, want [[peer-b]]", *added)
	}
	s.warnUntrustedKeys([]string{"self", "peer-b"})
	if len(*asked) != 1 || len(*added) != 1 {
		t.Errorf("a settled key was checked again: asked %v added %v", *asked, *added)
	}
}

// TestAFailingHelperFallsBackToTheWarning: a helper that fails (sudo rule
// gone, nix-daemon will not restart) leaves the warning naming the command.
func TestAFailingHelperFallsBackToTheWarning(t *testing.T) {
	_, added, _ := fakeSystemTrust(t, true, errors.New("sudo: a password is required"))
	s := &Server{}
	if got := s.warnUntrustedKeys([]string{"peer-b"}); !reflect.DeepEqual(got, []string{"peer-b"}) {
		t.Fatalf("warned about %v, want [peer-b]", got)
	}
	if len(*added) != 1 {
		t.Errorf("helper calls %v, want one", *added)
	}
	if s.trustNext["peer-b"].Equal(trustSettled) {
		t.Error("a key the helper failed to add is never retried")
	}
}
