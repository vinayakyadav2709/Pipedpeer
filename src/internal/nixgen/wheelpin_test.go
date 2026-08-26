package nixgen

import (
	"strings"
	"testing"
)

// TestPinnedPackageReachesTheFlake. The pin is the whole point: without it the
// closure gets nixpkgs' version, which is a different one from the host
// environment, and the same script behaves differently in the two places.
func TestPinnedPackageReachesTheFlake(t *testing.T) {
	pin, ok := pinFor("scikit-learn", "x86_64-linux")
	if !ok {
		t.Fatal("scikit-learn has no pin on x86_64-linux")
	}
	flake := GenerateFlake([]string{"scikit-learn"}, "python3", false)

	if !strings.Contains(flake, pin.SHA256) {
		t.Error("the flake does not carry the pinned hash, so nix will fetch " +
			"whatever the url happens to serve")
	}
	if !strings.Contains(flake, pin.URL) {
		t.Error("the flake does not carry the pinned url")
	}
	if !strings.Contains(flake, `version = "`+pin.Version+`"`) {
		t.Errorf("the flake does not pin version %s", pin.Version)
	}
	for _, dep := range pin.Deps {
		if !strings.Contains(flake, dep) {
			t.Errorf("the flake is missing %s, which the pinned wheel imports and "+
				"the wheel's own metadata does not bring in", dep)
		}
	}
}

// TestPinIsPlatformSpecific. A manylinux x86_64 wheel is not an answer on
// aarch64 or darwin, and shipping it there produces a closure that cannot run.
func TestPinIsPlatformSpecific(t *testing.T) {
	if _, ok := pinFor("scikit-learn", "aarch64-linux"); ok {
		t.Error("an x86_64 wheel was offered for aarch64")
	}
	flake := GenerateFlake([]string{"scikit-learn"}, "python3", false)
	if !strings.Contains(flake, "manylinux") {
		t.Skip("NOT VERIFIED: this build is not x86_64-linux, so the pin does not apply")
	}
}

// TestUnpinnedPackagesAreLeftAlone. Everything that nixpkgs already gets right
// should stay on nixpkgs, or the table becomes a second package set nobody
// maintains.
func TestUnpinnedPackagesAreLeftAlone(t *testing.T) {
	flake := GenerateFlake([]string{"numpy", "pandas"}, "python3", false)
	if strings.Contains(flake, "files.pythonhosted.org") {
		t.Error("a package with no pin was given a pinned wheel")
	}
	if strings.Contains(flake, "overridePythonAttrs") {
		t.Error("a package with no pin was overridden")
	}
	for _, want := range []string{"ps.numpy", "ps.pandas"} {
		if !strings.Contains(flake, want) {
			t.Errorf("the flake is missing %s", want)
		}
	}
}

// TestEveryPinIsUsable checks the table itself rather than one entry, so a new
// row with a missing field fails here instead of in a nix build an hour later.
func TestEveryPinIsUsable(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range wheelPins {
		key := p.Package + "/" + p.System
		if seen[key] {
			t.Errorf("%s is pinned twice; which one applies depends on table order", key)
		}
		seen[key] = true

		if p.Package == "" || p.System == "" || p.Version == "" {
			t.Errorf("%+v: package, system and version are all required", p)
		}
		if !strings.HasPrefix(p.URL, "https://") {
			t.Errorf("%s: url %q is not https; a wheel fetched over plain http is "+
				"whatever the network hands back", key, p.URL)
		}
		if len(p.SHA256) != 64 {
			t.Errorf("%s: sha256 %q is not 64 hex characters, so nix cannot verify "+
				"what it downloaded", key, p.SHA256)
		}
		if !strings.Contains(p.URL, p.Version) &&
			!strings.Contains(p.URL, strings.ReplaceAll(p.Version, ".", "_")) {
			t.Errorf("%s: url does not mention version %s, so the pin may be "+
				"fetching something else entirely", key, p.Version)
		}
		if p.Why == "" {
			t.Errorf("%s: no reason recorded. A pin stops the package following "+
				"nixpkgs, and a later reader has to be able to tell whether it "+
				"still applies", key)
		}
	}
}

// TestHyphenatedNamesAreQuoted. Nix attributes cannot carry a hyphen unquoted,
// and nixpkgs spells these packages with one - so the reference has to be
// quoted rather than rewritten, which would point at an attribute that does
// not exist.
func TestHyphenatedNamesAreQuoted(t *testing.T) {
	if got := attrName("scikit-learn"); got != `"scikit-learn"` {
		t.Errorf("attrName(scikit-learn) = %s, want it quoted", got)
	}
	if got := attrName("numpy"); got != "numpy" {
		t.Errorf("attrName(numpy) = %s, want it bare", got)
	}
}
