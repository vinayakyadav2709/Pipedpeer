{
  # The one Python environment every system in the lab campaign runs under
  # (plain, pipedpeer's submitter tools, Ray, Dask): docs/benchmarks/lab-campaign.md 1.3.
  # Same nixpkgs revision as the closures pipedpeer builds
  # (src/internal/nixgen/flake.go, nixpkgsRef), and the same wheel pins
  # (src/internal/nixgen/wheelpin.go), so library versions match. Without the
  # pin, plain, Ray and Dask ran scikit-learn 1.8.0 and pipedpeer 1.9.0, and
  # W4's best_params differed at the rehearsal's size
  # (scripts/campaign/tests/test_env_pins.py keeps the two in step).
  description = "pipedpeer lab campaign: python with numpy, pandas, scikit-learn, joblib, torch (CPU), ray, dask";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/56c02bc00adcf003215cc4bd996d6efaf4cff188";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      python = pkgs.python3;
    in {
      packages.${system}.default = python.withPackages (ps: with ps; [
        numpy
        pandas
        # wheelpin.go's scikit-learn pin, the expression pipedpeer writes.
        ps.narwhals
        (ps."scikit-learn".overridePythonAttrs (old: {
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
        }))
        joblib
        torch
        ray
        dask
        distributed
      ] ++ ps.ray.optional-dependencies.default);
    };
}
