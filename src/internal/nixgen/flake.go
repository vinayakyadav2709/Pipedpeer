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

// nixpkgsRef pins the package set every generated flake builds against.
//
// It has to be an exact revision, not a branch. A branch resolves at build
// time, so two nodes that build the same script on different days produce
// different store paths — and UploadJob's closure cache keys on the store
// path, so instead of shipping the environment once it re-uploads a
// multi-hundred-megabyte NAR for every job. Bump deliberately.
const nixpkgsRef = "github:NixOS/nixpkgs/56c02bc00adcf003215cc4bd996d6efaf4cff188"

// torchRequirements pins the full cu126 wheel set — torch plus every
// transitive dependency pip would resolve. The pinning is what makes the
// download a fixed-output derivation (stable outputHash), which in turn is
// what lets the build reach the wheel index under nix's default sandbox.
//
// To bump: pip-install the new torch into a scratch dir, list the
// *.dist-info directories for the versions, update torchWheelsHash by
// building once with a wrong hash and copying the "got:" value from the
// mismatch error.
const torchRequirements = `torch==2.13.0+cu126
cuda-bindings==12.9.7
cuda-pathfinder==1.7.0
cuda-toolkit==12.6.3
filelock==3.32.4
fsspec==2026.7.0
jinja2==3.1.6
markupsafe==3.0.3
mpmath==1.3.0
networkx==3.6.1
nvidia-cublas-cu12==12.6.4.1
nvidia-cuda-cupti-cu12==12.6.80
nvidia-cuda-nvrtc-cu12==12.6.85
nvidia-cuda-runtime-cu12==12.6.77
nvidia-cudnn-cu12==9.10.2.21
nvidia-cufft-cu12==11.3.0.4
nvidia-cufile-cu12==1.11.1.6
nvidia-curand-cu12==10.3.7.77
nvidia-cusolver-cu12==11.7.1.2
nvidia-cusparse-cu12==12.5.4.2
nvidia-cusparselt-cu12==0.7.1
nvidia-nccl-cu12==2.29.3
nvidia-nvjitlink-cu12==12.6.85
nvidia-nvshmem-cu12==3.4.5
nvidia-nvtx-cu12==12.6.77
setuptools==84.0.0
sympy==1.14.0
triton==3.7.1
typing-extensions==4.16.0
`

// torchWheelsHash is the recursive sha256 of the torchRequirements wheel set.
const torchWheelsHash = "sha256-0C73Ki7qj6q3wK78ocYCu0AhcSsBtZvidwS4pG33zS8="

func GenerateFlake(nixPkgs []string, pythonVersion string) string {
	return GenerateFlakeForArch(nixPkgs, pythonVersion, NixArch())
}

func GenerateFlakeForArch(nixPkgs []string, pythonVersion string, nixSystem string) string {
	if pythonVersion == "" {
		pythonVersion = "python3"
	}
	if len(nixPkgs) == 0 {
		return fmt.Sprintf(`{
  inputs.nixpkgs.url = "`+nixpkgsRef+`";

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
			// Instead fetch the official CUDA wheels (cu126) in a fixed-output
			// derivation — FODs are allowed network even under nix's default
			// sandbox, which is what lets this build on a stock nix install —
			// then pip-install them offline into a site dir. The run wrapper
			// prepends the site to PYTHONPATH. Requires the host NVIDIA
			// driver; the daemon bind-mounts it into sandboxed jobs.
			torchCUDA = true
			continue
		}
		psPkgs = append(psPkgs, "ps."+pkg)
	}
	if torchCUDA {
		// torch imports numpy at runtime for array interop. pip installs only
		// torch's own tree into the site dir, so numpy has to come from the
		// nix env — and when torch is a script's only import that env would
		// otherwise be empty, leaving torch to warn "Failed to initialize
		// NumPy" and quietly drop .numpy()/from_numpy().
		hasNumpy := false
		for _, p := range psPkgs {
			if p == "ps.numpy" {
				hasNumpy = true
				break
			}
		}
		if !hasNumpy {
			psPkgs = append(psPkgs, "ps.numpy")
		}
	}

	pkgsList := strings.Join(psPkgs, "\n          ")
	if torchCUDA {
		return fmt.Sprintf(`{
  inputs.nixpkgs.url = "`+nixpkgsRef+`";

  outputs = { self, nixpkgs }: {
    packages.%s.default =
      let
        pkgs = nixpkgs.legacyPackages.%s;
        python = pkgs.%s.withPackages (ps: [
          %s
        ]);
        pythonpip = pkgs.python3.withPackages (ps: [ ps.pip ]);
        stdenvcc = pkgs.gcc.cc.lib;
        # The wheel set is fully pinned so the download is a fixed-output
        # derivation: reproducible, and — unlike a plain pip install — allowed
        # network access even when the nix sandbox is on, so this builds on a
        # stock nix install. Bumping torch means regenerating this list and
        # the hash (see nixgen: torchRequirements / torchWheelsHash).
        torchreqs = pkgs.writeText "torch-cu126-reqs.txt" ''
`+torchRequirements+`'';
        torchwheels = pkgs.runCommand "torch-cu126-wheels" {
          nativeBuildInputs = [ pythonpip pkgs.cacert ];
          outputHashMode = "recursive";
          outputHashAlgo = "sha256";
          outputHash = "`+torchWheelsHash+`";
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        } ''
          mkdir -p $out
          python -m pip download --no-cache-dir --no-deps --only-binary=:all: \
            --dest $out -r ${torchreqs} \
            --index-url https://download.pytorch.org/whl/cu126 \
            --extra-index-url https://pypi.org/simple
        '';
        torchsite = pkgs.runCommand "torch-cu126-site" {
          nativeBuildInputs = [ pythonpip stdenvcc ];
        } ''
          mkdir -p $out/lib
          python -m pip install --no-cache-dir --no-index \
            --find-links ${torchwheels} --target $out/lib torch
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
  inputs.nixpkgs.url = "`+nixpkgsRef+`";

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
