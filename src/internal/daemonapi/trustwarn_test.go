package daemonapi

import (
	"reflect"
	"testing"
)

// TestAnUntrustedClusterKeyIsWarnedAboutOnce: on a multi-user nix the daemon
// names `pipedpeer trust` for each key system nix refuses, once per key, and
// does not ask nix again for keys it has already checked.
func TestAnUntrustedClusterKeyIsWarnedAboutOnce(t *testing.T) {
	orig := systemUntrusted
	t.Cleanup(func() { systemUntrusted = orig })
	var asked [][]string
	systemUntrusted = func(keys []string) ([]string, bool, error) {
		asked = append(asked, keys)
		var missing []string
		for _, k := range keys {
			if k == "peer-b" {
				missing = append(missing, k)
			}
		}
		return missing, true, nil
	}

	s := &Server{}
	if got := s.warnUntrustedKeys([]string{"self", "peer-b"}); !reflect.DeepEqual(got, []string{"peer-b"}) {
		t.Fatalf("warned about %v, want [peer-b]", got)
	}
	if got := s.warnUntrustedKeys([]string{"self", "peer-b"}); got != nil {
		t.Errorf("warned again about %v on the next poll", got)
	}
	s.warnUntrustedKeys([]string{"self", "peer-b", "peer-c"})
	if len(asked) != 2 || !reflect.DeepEqual(asked[1], []string{"peer-c"}) {
		t.Errorf("nix was asked %v; want only the new key on the second check", asked)
	}
}
