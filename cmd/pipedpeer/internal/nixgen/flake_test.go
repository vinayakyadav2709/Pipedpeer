package nixgen

import (
	"strings"
	"testing"
)

func TestGenerateFlakeWithoutPackages(t *testing.T) {
	flake := GenerateFlake("main.py", nil, []string{"main.py", "helpers.py"})
	if !strings.Contains(flake, "pkgs.python3") {
		t.Fatalf("expected plain python runtime in flake")
	}
	if strings.Contains(flake, "withPackages") {
		t.Fatalf("did not expect withPackages when nix package list is empty")
	}
	if !strings.Contains(flake, "cp ${./helpers.py} \"$srcdir/helpers.py\"") {
		t.Fatalf("expected local source copy setup")
	}
}

func TestGenerateFlakeWithPackages(t *testing.T) {
	flake := GenerateFlake("main.py", []string{"numpy", "pandas"}, []string{"main.py", "pkg/__init__.py"})
	if !strings.Contains(flake, "python3.withPackages") {
		t.Fatalf("expected withPackages section")
	}
	if !strings.Contains(flake, "ps.numpy") || !strings.Contains(flake, "ps.pandas") {
		t.Fatalf("expected mapped python packages in flake")
	}
	if !strings.Contains(flake, "mkdir -p \"$srcdir/pkg\"") {
		t.Fatalf("expected package directory materialization")
	}
}
