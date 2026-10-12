package app

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
)

// A requirements.txt beside the script counts as --pkg: its mapped names are
// packages, its unmapped names are probed (uv.lock or not), and an import it
// names is not reported as unpackaged a second time.
func TestPlanPackagesReadsRequirementsOnce(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("numpy\nsomedist\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "main.py")
	numpy := pythondeps.ResolveNixPackage("numpy")

	nix, unknown := PlanPackages(script, []string{"numpy", "somedist", "otherimp"}, nil, false)
	if !slices.Equal(nix, []string{numpy}) {
		t.Errorf("nix = %v, want [%s]", nix, numpy)
	}
	slices.Sort(unknown)
	if !slices.Equal(unknown, []string{"otherimp", "somedist"}) {
		t.Errorf("unknown = %v, want [otherimp somedist] once each", unknown)
	}

	// From a uv.lock: the import check is off, the requirements still count.
	nix, unknown = PlanPackages(script, []string{"numpy"}, nil, true)
	if !slices.Equal(nix, []string{numpy}) || !slices.Equal(unknown, []string{"somedist"}) {
		t.Errorf("with uv.lock: nix = %v, unknown = %v; want [%s], [somedist]", nix, unknown, numpy)
	}

	// --pkg python3Packages.otherimp covers the import.
	_, unknown = PlanPackages(script, []string{"otherimp"}, []string{"python3Packages.otherimp"}, false)
	if !slices.Equal(unknown, []string{"somedist"}) {
		t.Errorf("--pkg: unknown = %v, want [somedist]", unknown)
	}
}
