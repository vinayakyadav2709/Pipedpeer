package nixgen

import (
	"strings"
	"testing"
)

func TestGenerateFlakeWithoutPackages(t *testing.T) {
	flake := GenerateFlake(nil, "", true)
	if !strings.Contains(flake, "pkgs.python3") {
		t.Fatalf("expected plain python runtime in flake")
	}
	if strings.Contains(flake, "withPackages") {
		t.Fatalf("did not expect withPackages when nix package list is empty")
	}
}

func TestGenerateFlakeWithPackages(t *testing.T) {
	flake := GenerateFlake([]string{"numpy", "pandas"}, "python310", true)
	if !strings.Contains(flake, "pkgs.python310.withPackages") {
		t.Fatalf("expected withPackages section with python310")
	}
	if !strings.Contains(flake, "ps.numpy") || !strings.Contains(flake, "ps.pandas") {
		t.Fatalf("expected mapped python packages in flake")
	}
}

func TestGenerateFlakePythonVersionDefault(t *testing.T) {
	flake := GenerateFlake([]string{"numpy"}, "", true)
	if !strings.Contains(flake, "pkgs.python3.withPackages") {
		t.Fatalf("expected default python3 when version is empty, got:\n%s", flake)
	}
}

func TestGenerateFlakePythonVersionExplicit(t *testing.T) {
	flake := GenerateFlake(nil, "python311", true)
	if !strings.Contains(flake, "pkgs.python311") {
		t.Fatalf("expected python311 in flake, got:\n%s", flake)
	}
}

func TestGenerateFlakeForArchAarch64(t *testing.T) {
	flake := GenerateFlakeForArch([]string{"numpy"}, "", "aarch64-linux", true)
	if !strings.Contains(flake, "packages.aarch64-linux.default") {
		t.Fatalf("expected aarch64-linux in flake, got:\n%s", flake)
	}
	if !strings.Contains(flake, "legacyPackages.aarch64-linux") {
		t.Fatalf("expected aarch64-linux legacyPackages, got:\n%s", flake)
	}
}

func TestGenerateFlakeSklearnPinnedOnX8664(t *testing.T) {
	flake := GenerateFlakeForArch([]string{"numpy", "scikit-learn"}, "", "x86_64-linux", true)
	if !strings.Contains(flake, `version = "1.9.0"`) {
		t.Fatalf("expected sklearn 1.9.0 wheel override, got:\n%s", flake)
	}
	if !strings.Contains(flake, "manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl") {
		t.Fatalf("expected manylinux x86_64 wheel in flake")
	}
	if !strings.Contains(flake, "ps.numpy") {
		t.Fatalf("expected numpy still mapped plainly")
	}
}

func TestGenerateFlakeSklearnUnpinnedOnOtherArch(t *testing.T) {
	flake := GenerateFlakeForArch([]string{"scikit-learn"}, "", "aarch64-linux", true)
	if strings.Contains(flake, `version = "1.9.0"`) {
		t.Fatalf("did not expect wheel override on non-x86_64 arch:\n%s", flake)
	}
	if !strings.Contains(flake, "ps.scikit-learn") {
		t.Fatalf("expected plain ps.scikit-learn on aarch64")
	}
}

func TestNixArchReturnsValidFormat(t *testing.T) {
	arch := NixArch()
	if !strings.Contains(arch, "-") {
		t.Fatalf("expected arch-os format, got: %s", arch)
	}
	// Should contain a valid Nix system like x86_64-linux or aarch64-linux
	validPrefixes := []string{"x86_64", "aarch64", "armv7l"}
	found := false
	for _, p := range validPrefixes {
		if strings.HasPrefix(arch, p) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("unexpected architecture prefix in: %s", arch)
	}
}

// The nixpkgs input has to be an exact revision. A branch resolves at build
// time, so two nodes building the same script on different days land on
// different store paths, and UploadJob's closure cache — which keys on the
// store path — never hits. Every job then re-ships a multi-hundred-megabyte
// NAR that should have travelled once.
func TestGeneratedFlakesPinNixpkgs(t *testing.T) {
	flakes := map[string]string{
		"no packages":  GenerateFlakeForArch(nil, "", "x86_64-linux", true),
		"with numpy":   GenerateFlakeForArch([]string{"numpy"}, "python3", "x86_64-linux", true),
		"with torch":   GenerateFlakeForArch([]string{"torch"}, "python3", "x86_64-linux", true),
		"non-x86 arch": GenerateFlakeForArch([]string{"numpy"}, "python3", "aarch64-linux", true),
	}

	for name, flake := range flakes {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(flake, nixpkgsRef) {
				t.Fatalf("flake does not pin nixpkgs to %s:\n%s", nixpkgsRef, flake)
			}
			if strings.Contains(flake, "nixos-unstable") {
				t.Fatalf("flake still tracks a moving branch:\n%s", flake)
			}
		})
	}
}

// torch pulls its own tree into a pip site dir, but it imports numpy from the
// surrounding nix env at runtime. A script whose only import is torch used to
// produce an env with no packages at all, so torch logged "Failed to
// initialize NumPy" and lost array interop.
func TestTorchFlakeAlwaysIncludesNumpy(t *testing.T) {
	tests := []struct {
		name string
		pkgs []string
	}{
		{"torch alone", []string{"torch"}},
		{"torch with numpy already asked for", []string{"torch", "numpy"}},
		{"torch beside something else", []string{"torch", "pandas"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flake := GenerateFlakeForArch(tt.pkgs, "python3", "x86_64-linux", true)
			if !strings.Contains(flake, "ps.numpy") {
				t.Fatalf("torch env without numpy:\n%s", flake)
			}
			if strings.Count(flake, "ps.numpy") != 1 {
				t.Fatalf("numpy listed %d times, want 1:\n%s",
					strings.Count(flake, "ps.numpy"), flake)
			}
		})
	}
}

// The torch wheel download must stay a fixed-output derivation: FODs get
// network access even under nix's default sandbox, so workers with a stock
// nix install can build the env. A plain pip install inside a normal
// derivation only worked with sandbox = false on every machine.
func TestTorchFlakeUsesFixedOutputWheelDownload(t *testing.T) {
	flake := GenerateFlakeForArch([]string{"torch"}, "python3", "x86_64-linux", true)

	for _, needle := range []string{
		"outputHash = \"" + torchWheelsHash + "\"",
		"outputHashMode = \"recursive\"",
		"pip download",
		"--no-deps",
		"pip install --no-cache-dir --no-index",
		"torch==2.13.0+cu126",
	} {
		if !strings.Contains(flake, needle) {
			t.Fatalf("torch flake missing %q:\n%s", needle, flake)
		}
	}

	// Every requirement must be pinned, or the FOD hash drifts as the index
	// moves and every torch env build starts failing with a hash mismatch.
	for _, line := range strings.Split(strings.TrimSpace(torchRequirements), "\n") {
		if !strings.Contains(line, "==") {
			t.Fatalf("unpinned requirement %q", line)
		}
	}
}

// TestTorchWithoutGPUSkipsCUDAClosure pins what importing torch costs.
//
// The CUDA branch fired on the mere presence of the import, so a script that
// never touched a device still shipped the whole wheel set — torch plus
// cuBLAS, cuDNN, NCCL, triton and the toolkit, several gigabytes — to every
// node that ran it, even under --gpu off. Wanting a GPU and importing torch
// are different claims.
func TestTorchWithoutGPUSkipsCUDAClosure(t *testing.T) {
	cpu := GenerateFlakeForArch([]string{"torch"}, "", "x86_64-linux", false)
	if strings.Contains(cpu, "download.pytorch.org") || strings.Contains(cpu, "torchwheels") {
		t.Error("a CPU job is still fetching the CUDA wheel set")
	}
	if !strings.Contains(cpu, "ps.torch") {
		t.Error("CPU job lost torch entirely; it should come from nixpkgs")
	}
	// The shim's gradient exchange moves tensors through .numpy(), so a DDP
	// run without numpy trains one step and dies at the first sync. nixpkgs'
	// CPU torch does not pull it in on its own.
	if !strings.Contains(cpu, "ps.numpy") {
		t.Error("CPU closure is missing numpy; DDP sync would fail at the first exchange")
	}

	gpu := GenerateFlakeForArch([]string{"torch"}, "", "x86_64-linux", true)
	if !strings.Contains(gpu, "download.pytorch.org") {
		t.Error("a GPU job is not fetching the CUDA wheels")
	}
	// torch imports numpy for array interop; the wheel set does not carry it.
	if !strings.Contains(gpu, "ps.numpy") {
		t.Error("CUDA closure is missing numpy")
	}

	// The two must not collide in the Nix store, or one would be served from
	// the other's cache entry.
	if cpu == gpu {
		t.Error("CPU and GPU closures are identical")
	}
}
