package nixgen

import (
	"fmt"
	"regexp"
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

// torchCPURequirements is the CPU build of the same torch: what a node
// without an NVIDIA GPU gets, so every node of a job (a DDP ring of a GPU
// laptop and CPU-only peers, a CPU-only lab) runs one torch version. nixpkgs'
// ps.torch was a different version (2.12.0 at nixpkgs pin vs 2.13.0 here).
// Bump together with torchRequirements, the same way.
const torchCPURequirements = `torch==2.13.0+cpu
filelock==3.32.4
fsspec==2026.7.0
jinja2==3.1.6
markupsafe==3.0.3
mpmath==1.3.0
networkx==3.6.1
setuptools==84.0.0
sympy==1.14.0
typing-extensions==4.16.0
`

// torchCPUWheelsHash is the recursive sha256 of the torchCPURequirements set.
const torchCPUWheelsHash = "sha256-xp57wYwHjc3QaaNswLRQakFnqqqisnx9Y7EhC/R3lr8="

// torchWheelSet is one pinned torch build: its requirement list, the FOD
// hash, the index it comes from and the check its site dir must pass.
type torchWheelSet struct {
	name, reqs, hash, index, check string
}

var (
	torchCUDAWheels = torchWheelSet{"cu126", torchRequirements, torchWheelsHash,
		"https://download.pytorch.org/whl/cu126", "assert torch.version.cuda, 'no cuda torch'"}
	torchCPUWheels = torchWheelSet{"cpu", torchCPURequirements, torchCPUWheelsHash,
		"https://download.pytorch.org/whl/cpu", "assert torch.__version__ == '2.13.0+cpu', torch.__version__"}
)

// GenerateFlake builds the environment. gpu selects torch's build: the CUDA
// wheel set costs several gigabytes, so a script that imports torch without
// wanting a GPU should not pay for it.
func GenerateFlake(nixPkgs []string, pythonVersion string, gpu bool) string {
	return GenerateFlakeForArch(nixPkgs, pythonVersion, NixArch(), gpu)
}

func GenerateFlakeForArch(nixPkgs []string, pythonVersion string, nixSystem string, gpu bool) string {
	if pythonVersion == "" {
		pythonVersion = "python3"
	}
	var psPkgs []string
	var wheels *torchWheelSet
	hasTorch := false
	for _, pkg := range nixPkgs {
		if pkg == "torch" {
			hasTorch = true
		}
		// Packages pinned to a specific wheel, from the table in wheelpin.go.
		// A pin used to be a hardcoded branch here with its Nix expression,
		// URL and hash inline; a second one meant copying all of it.
		if pin, ok := pinFor(pkg, nixSystem); ok {
			psPkgs = append(psPkgs, pin.Deps...)
			psPkgs = append(psPkgs, pin.expr())
			continue
		}
		if pkg == "torch" && nixSystem == "x86_64-linux" {
			// nixpkgs' ps.torch builds from source with CUDA (nvcc +
			// triton-llvm: hours), and its CPU build is another version.
			// Instead fetch the official wheels in a fixed-output derivation
			// — FODs are allowed network even under nix's default sandbox,
			// which is what lets this build on a stock nix install — then
			// pip-install them offline into a site dir. The run wrapper
			// prepends the site to PYTHONPATH.
			//
			// The CUDA set (cu126, ~6.6 GB unpacked; needs the host NVIDIA
			// driver, which the daemon bind-mounts into sandboxed jobs) only
			// when a GPU was asked for: it once fired on the mere import, and
			// a script that never touched a device shipped cuBLAS, cuDNN,
			// NCCL, triton and the toolkit to every node. Otherwise the CPU
			// build of the same version.
			wheels = &torchCPUWheels
			if gpu {
				wheels = &torchCUDAWheels
			}
			continue
		}
		psPkgs = append(psPkgs, "ps."+pkg)
	}
	if hasTorch {
		// torch needs numpy for array interop, and pipedpeer needs it more
		// than most torch users do: the shim's gradient exchange moves
		// tensors through .numpy(). Without it a DDP run dies at the first
		// sync with "Numpy is not available", having already trained a step.
		//
		// Neither the wheel sets (pip installs only torch's own tree) nor
		// nixpkgs' torch carry numpy, so every build needs it added here.
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

	// cloudpickle in every environment, including one that asked for no
	// packages at all.
	//
	// It is how a lambda, a closure or a method reaches a worker. The shim
	// ships a kernel as source where it can, but source only exists for a
	// plain named function; everything else used to be counted
	// "unshippable" and run locally forever. cloudpickle serialises those
	// by value, and the worker needs it importable to load one - so it has
	// to be in the environment that TRAVELS, not merely in the submitter's.
	//
	// Unconditional because the callable's shape is not knowable when the
	// environment is built: the same env runs whatever the script does
	// next. It is pure Python and small, and the transfer diffs per store
	// path, so an existing peer receives cloudpickle and the rebuilt
	// wrapper rather than the whole environment again.
	psPkgs = append(psPkgs, "ps.cloudpickle")

	pkgsList := strings.Join(psPkgs, "\n          ")
	if wheels != nil {
		w := *wheels
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
        torchreqs = pkgs.writeText "torch-`+w.name+`-reqs.txt" ''
`+w.reqs+`'';
        torchwheels = pkgs.runCommand "torch-`+w.name+`-wheels" {
          nativeBuildInputs = [ pythonpip pkgs.cacert ];
          outputHashMode = "recursive";
          outputHashAlgo = "sha256";
          outputHash = "`+w.hash+`";
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        } ''
          mkdir -p $out
          python -m pip download --no-cache-dir --no-deps --only-binary=:all: \
            --dest $out -r ${torchreqs} \
            --index-url `+w.index+` \
            --extra-index-url https://pypi.org/simple
        '';
        torchsite = pkgs.runCommand "torch-`+w.name+`-site" {
          nativeBuildInputs = [ pythonpip stdenvcc ];
        } ''
          mkdir -p $out/lib
          python -m pip install --no-cache-dir --no-index \
            --find-links ${torchwheels} --target $out/lib torch
          LD_LIBRARY_PATH=${stdenvcc}/lib PYTHONPATH=$out/lib \
            python -c "import torch; `+w.check+`"
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

// Recipe is what a job's environment is made of. A node of another system
// cannot run the submitter's closure, and the flake itself names one system
// and that system's wheel pins, so the node is sent this and generates its
// own flake (Flake) from it.
type Recipe struct {
	Pkgs   []string `json:"pkgs"`
	Python string   `json:"python,omitempty"`
	GPU    bool     `json:"gpu,omitempty"`
}

var (
	recipePkgRe    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_'-]*$`)
	recipePythonRe = regexp.MustCompile(`^python[0-9]*$`)
)

// Flake is the recipe's flake for nixSystem. It refuses names that are not
// plain nix attribute names: a recipe arrives from a peer and is pasted into
// a nix expression.
func (r Recipe) Flake(nixSystem string) (string, error) {
	if r.Python != "" && !recipePythonRe.MatchString(r.Python) {
		return "", fmt.Errorf("bad python attribute %q", r.Python)
	}
	for _, p := range r.Pkgs {
		if !recipePkgRe.MatchString(p) {
			return "", fmt.Errorf("bad package name %q", p)
		}
	}
	return GenerateFlakeForArch(r.Pkgs, r.Python, nixSystem, r.GPU), nil
}
