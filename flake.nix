{
  description = "ver: a simple, non-git-like version control system";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs: rec {
        ver = pkgs.rustPlatform.buildRustPackage {
          pname = "ver";
          version = (builtins.fromTOML (builtins.readFile ./crates/ver/Cargo.toml)).package.version;

          src = self;
          cargoLock.lockFile = ./Cargo.lock;

          cargoBuildFlags = [ "--package" "ver-vcs" ];
          cargoTestFlags = [ "--package" "ver-vcs" ];

          meta = {
            description = "A simple, non-git-like version control system";
            homepage = "https://ver-vcs.net";
            mainProgram = "ver";
          };
        };
        default = ver;
      });

      apps = forAllSystems (pkgs: rec {
        ver = {
          type = "app";
          program = "${self.packages.${pkgs.system}.ver}/bin/ver";
        };
        default = ver;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [ pkgs.cargo pkgs.rustc pkgs.rust-analyzer pkgs.clippy ];
        };
      });
    };
}
