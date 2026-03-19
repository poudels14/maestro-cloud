{
  description = "Maestro - deployment controller";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "aarch64-darwin" "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      pkgsFor = system: import nixpkgs { inherit system; };
    in {
      packages = forAllSystems (system:
        let pkgs = pkgsFor system; in {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "maestro";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            nativeBuildInputs = with pkgs; [ pkg-config protobuf ];

            buildInputs = with pkgs; [
              openssl
            ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.apple-sdk_15
            ];
          };
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/maestro";
        };
      });

      devShells = forAllSystems (system:
        let pkgs = pkgsFor system; in {
          default = pkgs.mkShell {
            inputsFrom = [ self.packages.${system}.default ];
            packages = with pkgs; [
              cargo
              rustc
              rust-analyzer
              clippy
            ];
          };
        }
      );

      nixosModules.default = { config, lib, pkgs, ... }:
        let
          cfg = config.services.maestro;
        in {
          options.services.maestro = {
            enable = lib.mkEnableOption "Maestro deployment controller";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "The maestro package to use";
            };

            config = lib.mkOption {
              type = lib.types.str;
              description = "Config URI (file path or aws-secret://...)";
            };

            dataDir = lib.mkOption {
              type = lib.types.path;
              default = "/data/maestro";
              description = "Directory for etcd data, logs, and state";
            };
          };

          config = lib.mkIf cfg.enable {
            virtualisation.docker.enable = true;

            systemd.services.maestro = {
              description = "Maestro deployment controller";
              after = [ "network-online.target" "docker.service" ];
              wants = [ "network-online.target" ];
              wantedBy = [ "multi-user.target" ];

              serviceConfig = {
                Type = "simple";
                Restart = "on-failure";
                RestartSec = 5;
                ExecStart = lib.concatStringsSep " " [
                  "${cfg.package}/bin/maestro"
                  "start"
                  "--config" cfg.config
                  "--data-dir" (toString cfg.dataDir)
                ];
              };
            };
          };
        };
    };
}
