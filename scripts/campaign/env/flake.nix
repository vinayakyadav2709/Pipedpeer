{
  # The one Python environment every system in the lab campaign runs under
  # (plain, pipedpeer's submitter tools, Ray, Dask): docs/benchmarks/lab-campaign.md 1.3.
  # Same nixpkgs revision as the closures pipedpeer builds
  # (src/internal/nixgen/flake.go, nixpkgsRef), so library versions match.
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
        scikit-learn
        joblib
        torch
        ray
        dask
        distributed
      ] ++ ps.ray.optional-dependencies.default);
    };
}
