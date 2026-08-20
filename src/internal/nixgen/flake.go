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
	torchCUDA := false
	for _, pkg := range nixPkgs {
		if pkg == "scikit-learn" && nixSystem == "x86_64-linux" {
			// nixpkgs-unstable still ships scikit-learn 1.8.x; pin the cp314
			// manylinux wheel to match the host dev environment (1.9.0) so the
			// built closure behaves identically on this machine and in docker.
			// 1.9.0 hard-imports narwhals, so pull nixpkgs' narwhals in too.
			psPkgs = append(psPkgs, "ps.narwhals")
			psPkgs = append(psPkgs, `(ps.scikit-learn.overridePythonAttrs (old: {
  version = "1.9.0";
  format = "wheel";
  pyproject = null;
  dontPatch = true;
  patches = [];
  doCheck = false;
  dontBuild = true;
  dontCheckRuntimeDeps = true;
  pythonImportsCheck = [];
  src = pkgs.fetchurl {
    url = "https://files.pythonhosted.org/packages/f0/af/4d72d9e475ac83719160c662619e4bf7b95c19507cd582e7d0167a3c3dae/scikit_learn-1.9.0-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl";
    sha256 = "1fea2cc5677ab49d6f5bade978c866da44957b712d92e9635e8b4f723013c3cb";
  };
}))`)
			continue
		}
		if pkg == "torch" && nixSystem == "x86_64-linux" {
			// nixpkgs' ps.torch builds from source (nvcc + triton-llvm: hours).
			// Instead pip-install the official CUDA wheel (cu126) into a site
			// dir at build time — the closure's nix is single-user/unsandboxed
			// so the build reaches the index. The run wrapper prepends the site
			// to PYTHONPATH. Requires the host NVIDIA driver; the container
			// bridges it via nvidia-container-toolkit.
			torchCUDA = true
			continue
		}
		psPkgs = append(psPkgs, "ps."+pkg)
	}
	pkgsList := strings.Join(psPkgs, "\n          ")
	if torchCUDA {
		return fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }: {
    packages.%s.default =
      let
        pkgs = nixpkgs.legacyPackages.%s;
        python = pkgs.%s.withPackages (ps: [
          %s
        ]);
        pythonpip = pkgs.python3.withPackages (ps: [ ps.pip ]);
        stdenvcc = pkgs.gcc.cc.lib;
        torchsite = pkgs.runCommand "torch-cu126-site" {
          nativeBuildInputs = [ pythonpip stdenvcc ];
        } ''
          mkdir -p $out/lib
          # plain pip install; pip resolves the whole dep tree (incl. triton,
          # cudnn, cublas, ...) itself. no manual wheel list to maintain.
          python -m pip install --no-cache-dir --target $out/lib torch \
            --index-url https://download.pytorch.org/whl/cu126 \
            --extra-index-url https://pypi.org/simple
          LD_LIBRARY_PATH=${stdenvcc}/lib PYTHONPATH=$out/lib \
            python -c "import torch; assert torch.version.cuda, 'no cuda torch'"
        '';
      in
      pkgs.writeShellScriptBin "run" ''
        export PYTHONPATH=${torchsite}/lib:$PYTHONPATH
        export LD_LIBRARY_PATH=${stdenvcc}/lib:$LD_LIBRARY_PATH
        exec ${python}/bin/python3 "$@"
      '';
  };
}
`, nixSystem, nixSystem, pythonVersion, pkgsList)
	}
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
