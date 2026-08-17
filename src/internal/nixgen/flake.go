package nixgen

import (
	"fmt"
	"runtime"
	"strings"
)

// NixArch returns the Nix system identifier for the current platform.
func NixArch() string {
	arch := runtime.GOARCH
	os := runtime.GOOS
	nixArch := "x86_64"
	switch arch {
	case "arm64":
		nixArch = "aarch64"
	case "arm":
		nixArch = "armv7l"
	}
	nixOS := "linux"
	if os == "darwin" {
		nixOS = "darwin"
	}
	return nixArch + "-" + nixOS
}

func GenerateFlake(nixPkgs []string, pythonVersion string) string {
	return GenerateFlakeForArch(nixPkgs, pythonVersion, NixArch())
}

func GenerateFlakeForArch(nixPkgs []string, pythonVersion string, nixSystem string) string {
	if pythonVersion == "" {
		pythonVersion = "python3"
	}
	if len(nixPkgs) == 0 {
		return fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }: {
    packages.%s.default =
      let
        pkgs = nixpkgs.legacyPackages.%s;
      in
      pkgs.writeShellScriptBin "run" ''
        exec ${pkgs.%s}/bin/python3 "$@"
      '';
  };
}
`, nixSystem, nixSystem, pythonVersion)
	}

	var psPkgs []string
	for _, pkg := range nixPkgs {
		psPkgs = append(psPkgs, "ps."+pkg)
	}
	pkgsList := strings.Join(psPkgs, "\n          ")
	return fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }: {
    packages.%s.default =
      let
        pkgs = nixpkgs.legacyPackages.%s;
        python = pkgs.%s.withPackages (ps: [
          %s
        ]);
      in
      pkgs.writeShellScriptBin "run" ''
        exec ${python}/bin/python3 "$@"
      '';
  };
}
`, nixSystem, nixSystem, pythonVersion, pkgsList)
}
