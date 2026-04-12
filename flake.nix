{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
      in
      pkgs.writeShellScriptBin "run" ''
        ${pkgs.python3}/bin/python3 ${./main.py}
      '';
  };
}
