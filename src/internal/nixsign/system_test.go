package nixsign

import (
	"errors"
	"reflect"
	"testing"
)

// fakeNix stands in for the machine's nix for one test.
func fakeNix(t *testing.T, private, multiUser bool, config map[string]string) {
	t.Helper()
	p, m, c := privateStore, multiUserNix, nixConfig
	t.Cleanup(func() { privateStore, multiUserNix, nixConfig = p, m, c })
	privateStore = func() bool { return private }
	multiUserNix = func() bool { return multiUser }
	nixConfig = func(name string) (string, error) {
		v, ok := config[name]
		if !ok {
			return "", errors.New("no such setting")
		}
		return v, nil
	}
}

// TestOnlyAMultiUserNixNeedsTheKeysInNixConf: which nix mode decides whether
// `pipedpeer trust` has anything to do.
func TestOnlyAMultiUserNixNeedsTheKeysInNixConf(t *testing.T) {
	keys := []string{"pipedpeer-a:AAAA=", "pipedpeer-b:BBBB="}
	cases := []struct {
		name           string
		private, multi bool
		config         map[string]string
		wantMissing    []string
		wantApplies    bool
	}{
		{"private store", true, true, nil, nil, false},
		{"single-user nix", false, false, nil, nil, false},
		{"require-sigs off", false, true, map[string]string{"require-sigs": "false\n"}, nil, false},
		{"multi-user, one key trusted", false, true,
			map[string]string{"require-sigs": "true\n", "trusted-public-keys": "cache.nixos.org-1:x= pipedpeer-a:AAAA=\n"},
			[]string{"pipedpeer-b:BBBB="}, true},
		{"multi-user, all trusted", false, true,
			map[string]string{"require-sigs": "true\n", "trusted-public-keys": "pipedpeer-b:BBBB= pipedpeer-a:AAAA=\n"},
			nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fakeNix(t, c.private, c.multi, c.config)
			missing, applies, err := SystemUntrusted(keys)
			if err != nil {
				t.Fatal(err)
			}
			if applies != c.wantApplies || !reflect.DeepEqual(missing, c.wantMissing) {
				t.Errorf("got missing=%v applies=%v, want %v %v", missing, applies, c.wantMissing, c.wantApplies)
			}
		})
	}
}

// TestAnUnreadableNixConfigIsAnError: a multi-user nix whose trust list cannot
// be read must not be reported as trusting everything.
func TestAnUnreadableNixConfigIsAnError(t *testing.T) {
	fakeNix(t, false, true, map[string]string{"require-sigs": "true\n"})
	if _, applies, err := SystemUntrusted([]string{"pipedpeer-a:AAAA="}); err == nil || !applies {
		t.Errorf("want an error on a multi-user nix, got applies=%v err=%v", applies, err)
	}
}
