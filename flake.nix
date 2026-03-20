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
          isNerdctl = cfg.runtime == "nerdctl";
          isDocker = cfg.runtime == "docker";
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

            runtime = lib.mkOption {
              type = lib.types.enum [ "docker" "nerdctl" ];
              default = "nerdctl";
              description = "Container runtime to use (docker or nerdctl)";
            };

            extraArgs = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [];
              description = "Extra arguments to pass to maestro start";
            };

          };

          config = lib.mkIf cfg.enable {

            # --- Docker runtime ---
            virtualisation.docker.enable = lib.mkIf isDocker true;

            # --- Containerd/nerdctl runtime ---
            virtualisation.containerd.enable = lib.mkIf isNerdctl true;
            virtualisation.containerd.settings = lib.mkIf isNerdctl {
              plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options.SystemdCgroup = true;
            };

            # BuildKit daemon for nerdctl build
            systemd.services.buildkitd = lib.mkIf isNerdctl {
              description = "BuildKit daemon";
              after = [ "containerd.service" ];
              requires = [ "containerd.service" ];
              wantedBy = [ "multi-user.target" ];
              serviceConfig = {
                Type = "simple";
                ExecStart = "${pkgs.buildkit}/bin/buildkitd --oci-worker=false --containerd-worker=true --addr unix:///run/buildkit/buildkitd.sock";
                Restart = "on-failure";
                RestartSec = 3;
                StateDirectory = "buildkit";
                RuntimeDirectory = "buildkit";
              };
            };

            # System packages available on the host
            environment.systemPackages = [
              cfg.package
            ] ++ lib.optionals isNerdctl [
              pkgs.nerdctl
              pkgs.buildkit
            ];

            # Copy maestro source for Dockerfile builds
            system.activationScripts.maestro-source = ''
              rm -rf /etc/maestro/source
              cp -r ${cfg.package.src} /etc/maestro/source
              chmod -R u+w /etc/maestro/source
            '';

            # --- Maestro service ---
            systemd.services.maestro = {
              description = "Maestro deployment controller";
              after = [
                "network-online.target"
              ] ++ (if isNerdctl
                then [ "containerd.service" "buildkitd.service" ]
                else [ "docker.service" ]);
              wants = [ "network-online.target" ];
              wantedBy = [ "multi-user.target" ];

              path = [
                pkgs.docker
                pkgs.nerdctl
                pkgs.cni-plugins
                pkgs.iptables
                pkgs.iproute2
                pkgs.buildkit
                pkgs.util-linux
                pkgs.kmod
                pkgs.nix
                pkgs.nixos-rebuild
              ];

              serviceConfig = {
                Type = "simple";
                Restart = "on-failure";
                RestartSec = 5;
                ExecStart = lib.concatStringsSep " " ([
                  "${cfg.package}/bin/maestro"
                  "start"
                  "--config" cfg.config
                  "--data-dir" (toString cfg.dataDir)
                  "--system" "nixos"
                  "--runtime" cfg.runtime
                  "--force"
                  "--project-dir" "/etc/maestro/source"
                ] ++ cfg.extraArgs);
              } // lib.optionalAttrs isNerdctl {
                Environment = [
                  "CONTAINERD_ADDRESS=/run/containerd/containerd.sock"
                  "CNI_PATH=${pkgs.cni-plugins}/bin"
                  "NETCONFPATH=/etc/cni/net.d"
                ];
              };
            };

            # Ensure CNI config directory exists
            systemd.tmpfiles.rules = lib.mkIf isNerdctl [
              "d /etc/cni/net.d 0755 root root -"
            ];
          };
        };
    };
}
