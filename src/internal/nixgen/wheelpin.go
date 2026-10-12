package nixgen

import (
	"fmt"
	"strings"
)

// Packages whose nixpkgs version is the wrong one for us, pinned to a specific
// wheel instead.
//
// This was a hardcoded branch in the flake generator, with the Nix expression
// and the URL and the hash all inline. Adding a second package meant copying
// twenty lines of Nix and changing four fields in it, and nothing checked that
// the copy stayed in step with the original. It is a table now: a pin is data,
// the expression that applies it is written once.
//
// Pinning is not the preferred answer and should stay rare. Every entry here
// is a place where the closure no longer follows nixpkgs, so it will not pick
// up a fix until somebody edits this file - which is the cost of matching a
// version the host environment expects.
type wheelPin struct {
	// Package is the nixpkgs python attribute name.
	Package string
	// System restricts the pin to one platform; a wheel built for
	// manylinux_x86_64 is not an answer on aarch64 or darwin.
	System string
	// Version is what the wheel actually is, which must match the URL.
	Version string
	URL     string
	SHA256  string
	// Deps are extra nixpkgs python packages the wheel imports but which the
	// wheel's own metadata does not bring in, because nothing here reads that
	// metadata.
	Deps []string
	// Why records the reason, so a later reader can tell whether it still
	// applies rather than having to guess.
	Why string
}

// wheelPins is the table. Ordered by package name for readability only.
var wheelPins = []wheelPin{
	{
		Package: "scikit-learn",
		System:  "x86_64-linux",
		Version: "1.9.0",
		URL: "https://files.pythonhosted.org/packages/f0/af/4d72d9e475ac83719160c662619e4bf7b95c19507cd582e7d0167a3c3dae/" +
			"scikit_learn-1.9.0-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
		SHA256: "1fea2cc5677ab49d6f5bade978c866da44957b712d92e9635e8b4f723013c3cb",
		Deps:   []string{"ps.narwhals"},
		Why: "nixpkgs-unstable still ships 1.8.x; the host dev environment is on " +
			"1.9.0, and a closure that behaves differently there and in a container " +
			"is worse than either. 1.9.0 hard-imports narwhals.",
	},
}

// pinFor returns the pin for a package on a system, if there is one.
func pinFor(pkg, nixSystem string) (wheelPin, bool) {
	for _, p := range wheelPins {
		if p.Package == pkg && p.System == nixSystem {
			return p, true
		}
	}
	return wheelPin{}, false
}

// expr renders the nixpkgs override that installs the pinned wheel.
//
// The long list of dont* flags is what turns a source build into a wheel
// unpack: nixpkgs' derivation expects a source tree and will otherwise try to
// patch, build and test one that is not there.
func (p wheelPin) expr() string {
	return fmt.Sprintf(`(ps.%s.overridePythonAttrs (old: {
  version = "%s";
  format = "wheel";
  pyproject = null;
  dontPatch = true;
  patches = [];
  doCheck = false;
  dontBuild = true;
  dontCheckRuntimeDeps = true;
  pythonImportsCheck = [];
  src = pkgs.fetchurl {
    url = "%s";
    sha256 = "%s";
  };
}))`, attrName(p.Package), p.Version, p.URL, p.SHA256)
}

// attrName turns a pypi name into the nixpkgs attribute for it. Nix
// attributes cannot contain a hyphen unquoted, and nixpkgs spells these with
// one, so the reference has to be quoted rather than rewritten - renaming it
// would point at an attribute that does not exist.
func attrName(pkg string) string {
	if strings.ContainsAny(pkg, "-.") {
		return `"` + pkg + `"`
	}
	return pkg
}
