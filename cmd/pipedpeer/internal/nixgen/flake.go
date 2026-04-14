package nixgen

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

func GenerateFlake(scriptRelPath string, nixPkgs []string, sourceFiles []string) string {
	if len(sourceFiles) == 0 {
		sourceFiles = []string{scriptRelPath}
	}
	sourceSetup := buildSourceSetup(sourceFiles)

	if len(nixPkgs) == 0 {
		return fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
      in
      pkgs.writeShellScriptBin "run" ''
%s
        exec ${pkgs.python3}/bin/python3 "$srcdir/%s"
      '';
  };
}
`, sourceSetup, scriptRelPath)
	}

	var psPkgs []string
	for _, pkg := range nixPkgs {
		psPkgs = append(psPkgs, "ps."+pkg)
	}
	pkgsList := strings.Join(psPkgs, "\n          ")
	return fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        python = pkgs.python3.withPackages (ps: [
          %s
        ]);
      in
      pkgs.writeShellScriptBin "run" ''
%s
        exec ${python}/bin/python3 "$srcdir/%s"
      '';
  };
}
`, pkgsList, sourceSetup, scriptRelPath)
}

func buildSourceSetup(sourceFiles []string) string {
	seen := map[string]bool{}
	var clean []string
	for _, f := range sourceFiles {
		f = filepath.ToSlash(filepath.Clean(f))
		if f == "." || f == "" {
			continue
		}
		if !seen[f] {
			seen[f] = true
			clean = append(clean, f)
		}
	}
	sort.Strings(clean)

	var b strings.Builder
	b.WriteString("        srcdir=$(mktemp -d)\n")
	b.WriteString("        trap 'rm -rf \"$srcdir\"' EXIT\n")
	for _, rel := range clean {
		dir := filepath.ToSlash(filepath.Dir(rel))
		if dir != "." {
			b.WriteString(fmt.Sprintf("        mkdir -p \"$srcdir/%s\"\n", dir))
		}
		b.WriteString(fmt.Sprintf("        cp ${./%s} \"$srcdir/%s\"\n", rel, rel))
	}
	b.WriteString("        cd \"$srcdir\"\n")
	return b.String()
}
