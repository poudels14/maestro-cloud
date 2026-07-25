{
  description = "Maestro - deployment controller";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    supportedSystems = ["aarch64-darwin" "x86_64-linux" "aarch64-linux"];
    forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    rustOverlay = builtins.getFlake "github:oxalica/rust-overlay/a286e5b998e852297a403786f063fb2c9fe7f57a";
    pkgsFor = system:
      import nixpkgs {
        inherit system;
        overlays = [rustOverlay.overlays.default];
      };
    rustToolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    maestroVersion = (builtins.fromTOML (builtins.readFile ./controller/Cargo.toml)).package.version;
    rewriteVersion =
      (builtins.fromTOML (builtins.readFile ./crates/apps/cli/Cargo.toml)).package.version;
  in {
    packages = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
        rewritePanel = import ./nix/rewrite-panel.nix {
          inherit pkgs;
          inherit rewriteVersion;
        };
        rewritePackage = import ./nix/rewrite-package.nix {
          inherit pkgs rustToolchain;
          panel = rewritePanel;
        };
      in {
        legacy = rustPlatform.buildRustPackage {
          pname = "maestro";
          version = maestroVersion;
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = with pkgs; [pkg-config protobuf];

          buildInputs = with pkgs;
            [
              openssl
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.apple-sdk_15
            ];
        };

        default = rewritePackage;
        rewrite = rewritePackage;
        rewrite-panel = rewritePanel;
      }
      // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux (let
        muslPkgs =
          {
            aarch64-linux = pkgs.pkgsCross.aarch64-multiplatform-musl;
            x86_64-linux = pkgs.pkgsCross.musl64;
          }
          .${system};
        staticTarget = muslPkgs.stdenv.hostPlatform.rust.rustcTarget;
        staticRewrite = import ./nix/rewrite-package.nix {
          inherit muslPkgs pkgs;
          panel = rewritePanel;
          rustToolchain = (rustToolchainFor pkgs).override {
            targets = [staticTarget];
          };
        };
        rewriteDaemonImage = import ./nix/rewrite-daemon-image.nix {
          inherit pkgs;
          rewritePackage = staticRewrite;
        };
        imageArchitecture =
          {
            aarch64-linux = "arm64";
            x86_64-linux = "amd64";
          }
          .${system};
      in {
        rewrite-static = staticRewrite;
        rewrite-static-bundle = import ./nix/rewrite-release-bundle.nix {
          inherit pkgs;
          rewritePackage = staticRewrite;
        };
        rewrite-daemon-image = rewriteDaemonImage;
        rewrite-daemon-image-bundle = import ./nix/rewrite-daemon-image-bundle.nix {
          daemonImage = rewriteDaemonImage;
          inherit imageArchitecture pkgs;
          rewritePackage = staticRewrite;
        };
      })
    );

    apps = forAllSystems (system: let
      app = package: program: {
        type = "app";
        program = "${package}/bin/${program}";
      };
    in {
      default = app self.packages.${system}.default "maestro";
      legacy = app self.packages.${system}.legacy "maestro";
      rewrite = app self.packages.${system}.rewrite "maestro";
      daemon = app self.packages.${system}.rewrite "maestro-daemon";
      migrate = app self.packages.${system}.rewrite "maestro-migrate";
    });

    checks = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rewritePackage = self.packages.${system}.rewrite;
      in
        {
          rewrite = rewritePackage;
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          rewrite-daemon-image-bundle =
            self.packages.${system}.rewrite-daemon-image-bundle;
          rewrite-module = import ./nix/rewrite-module-check.nix {
            inherit pkgs rewritePackage;
            config = (nixpkgs.lib.nixosSystem {
              inherit system;
              modules = [
                self.nixosModules.default
                {
                  system.stateVersion = "24.11";
                  services.maestro = {
                    enable = true;
                    config = "/run/maestro/launch.json";
                  };
                }
              ];
            }).config;
          };
          rewrite-static-bundle =
            self.packages.${system}.rewrite-static-bundle;
        }
    );

    devShells = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
      in {
        default = pkgs.mkShell {
          inputsFrom = [self.packages.${system}.default];
          packages = with pkgs; [
            rustToolchain
            rust-analyzer
          ];
        };
      }
    );

    nixosModules.legacy = {
      config,
      lib,
      pkgs,
      utils,
      ...
    }: let
      cfg = config.services.maestro;
      isNerdctl = cfg.runtime == "nerdctl";
      isDocker = cfg.runtime == "docker";
      depotPackage = import ./nix/depot-package.nix {inherit pkgs;};
    in {
      options.services.maestro = {
        enable = lib.mkEnableOption "Maestro deployment controller";

        package = lib.mkOption {
          type = lib.types.package;
          default = self.packages.${pkgs.system}.legacy;
          description = "The maestro package to use";
        };

        source = lib.mkOption {
          type = lib.types.path;
          default = self.outPath;
          description = "Source tree to copy for Dockerfile builds";
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
          type = lib.types.enum ["docker" "nerdctl"];
          default = "nerdctl";
          description = "Container runtime to use (docker or nerdctl)";
        };

        extraArgs = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
          description = "Extra arguments to pass to maestro daemon start";
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
          after = ["containerd.service"];
          requires = ["containerd.service"];
          wantedBy = ["multi-user.target"];
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
        environment.systemPackages =
          [
            cfg.package
            depotPackage
            pkgs.nftables
          ]
          ++ lib.optionals isNerdctl [
            pkgs.nerdctl
            pkgs.buildkit
          ];

        # Copy maestro source for Dockerfile builds
        system.activationScripts.maestro-source = ''
          mkdir -p /etc/maestro
          rm -rf /etc/maestro/source
          cp -r ${cfg.source} /etc/maestro/source
          chmod -R u+w /etc/maestro/source
        '';

        # Remove old Nix generations/store paths during activation.
        system.activationScripts.maestro-nix-gc = ''
          ${pkgs.nix}/bin/nix-collect-garbage -d || true
        '';

        systemd.services.aws-linklocal-routes = {
          description = "Route AWS link-local traffic via primary interface";
          after = ["network-online.target"];
          before = ["maestro.service"];
          wants = ["network-online.target"];
          wantedBy = ["multi-user.target"];

          path = [
            pkgs.iproute2
            pkgs.gawk
          ];

          serviceConfig = {
            Type = "oneshot";
          };
          script = ''
            set -eu

            state_dir=/run/aws-linklocal-routes
            state_file=$state_dir/iface
            mkdir -p "$state_dir"

            iface=$(${pkgs.iproute2}/bin/ip -4 route show default | ${pkgs.gawk}/bin/awk 'NR==1 {print $5}')
            [ -n "$iface" ] || exit 1

            case "$iface" in
              veth*|cni*|br-*)
                exit 1
                ;;
            esac

            prev_iface=""
            if [ -f "$state_file" ]; then
              prev_iface=$(cat "$state_file")
            fi

            matches() {
              case "$1" in
                *"$2"*) return 0 ;;
                *) return 1 ;;
              esac
            }

            changed=0
            [ "$prev_iface" = "$iface" ] || changed=1

            route_imds=$(${pkgs.iproute2}/bin/ip -4 route show table 100 169.254.169.254/32 2>/dev/null || true)
            route_ntp=$(${pkgs.iproute2}/bin/ip -4 route show table 100 169.254.169.123/32 2>/dev/null || true)
            route_dns=$(${pkgs.iproute2}/bin/ip -4 route show table 100 169.254.169.253/32 2>/dev/null || true)
            rule_imds=$(${pkgs.iproute2}/bin/ip -4 rule show to 169.254.169.254/32 2>/dev/null || true)
            rule_ntp=$(${pkgs.iproute2}/bin/ip -4 rule show to 169.254.169.123/32 2>/dev/null || true)
            rule_dns=$(${pkgs.iproute2}/bin/ip -4 rule show to 169.254.169.253/32 2>/dev/null || true)

            matches "$route_imds" "dev $iface" || changed=1
            matches "$route_ntp" "dev $iface" || changed=1
            matches "$route_dns" "dev $iface" || changed=1
            matches "$rule_imds" "lookup 100" || changed=1
            matches "$rule_ntp" "lookup 100" || changed=1
            matches "$rule_dns" "lookup 100" || changed=1

            ${pkgs.iproute2}/bin/ip -4 route replace table 100 169.254.169.254/32 dev "$iface" scope link
            ${pkgs.iproute2}/bin/ip -4 route replace table 100 169.254.169.123/32 dev "$iface" scope link
            ${pkgs.iproute2}/bin/ip -4 route replace table 100 169.254.169.253/32 dev "$iface" scope link

            ${pkgs.iproute2}/bin/ip -4 rule add pref 100 to 169.254.169.254/32 table 100 2>/dev/null || true
            ${pkgs.iproute2}/bin/ip -4 rule add pref 101 to 169.254.169.123/32 table 100 2>/dev/null || true
            ${pkgs.iproute2}/bin/ip -4 rule add pref 102 to 169.254.169.253/32 table 100 2>/dev/null || true

            if [ "$changed" -eq 1 ]; then
              printf '%s\n' "$iface" > "$state_file"
              echo "aws-linklocal-routes: updated routing for $iface"
            fi
          '';
        };

        systemd.timers.aws-linklocal-routes = {
          description = "Periodically reconcile AWS link-local routes";
          wantedBy = ["timers.target"];
          timerConfig = {
            OnBootSec = "30s";
            OnUnitActiveSec = "60s";
            Unit = "aws-linklocal-routes.service";
          };
        };

        # --- Maestro service ---
        systemd.services.maestro = {
          description = "Maestro deployment controller";
          after =
            [
              "network-online.target"
              "aws-linklocal-routes.service"
            ]
            ++ (
              if isNerdctl
              then ["containerd.service" "buildkitd.service"]
              else ["docker.service"]
            );
          wants = ["network-online.target"];
          requires = ["aws-linklocal-routes.service"];
          wantedBy = ["multi-user.target"];

          path = [
            pkgs.docker
            pkgs.nerdctl
            pkgs.cni-plugins
            pkgs.iptables
            pkgs.nftables
            pkgs.iproute2
            pkgs.buildkit
            pkgs.util-linux
            pkgs.kmod
            pkgs.nix
            pkgs.nixos-rebuild
            pkgs.git
            pkgs.coreutils
            depotPackage
          ];

          serviceConfig =
            {
              Type = "simple";
              Restart = "on-failure";
              RestartSec = 5;
              ExecStart = utils.escapeSystemdExecArgs ([
                  "${cfg.package}/bin/maestro"
                  "daemon"
                  "start"
                  "--config"
                  cfg.config
                  "--data-dir"
                  (toString cfg.dataDir)
                  "--system"
                  "nixos"
                  "--runtime"
                  cfg.runtime
                  "--force"
                  "--project-dir"
                  "/etc/maestro/source"
                ]
                ++ cfg.extraArgs);
            }
            // lib.optionalAttrs isNerdctl {
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

    nixosModules.default = import ./nix/rewrite-module.nix {inherit self;};
    nixosModules.rewrite = self.nixosModules.default;
  };
}
