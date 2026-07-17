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
  in {
    packages = forAllSystems (
      system: let
        pkgs = pkgsFor system;
        rustToolchain = rustToolchainFor pkgs;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
      in {
        default = rustPlatform.buildRustPackage {
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
      }
    );

    apps = forAllSystems (system: {
      default = {
        type = "app";
        program = "${self.packages.${system}.default}/bin/maestro";
      };
    });

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

    nixosModules.default = {
      config,
      lib,
      pkgs,
      utils,
      ...
    }: let
      cfg = config.services.maestro;
      isNerdctl = cfg.runtime == "nerdctl";
      isDocker = cfg.runtime == "docker";
      depotVersion = "2.101.43";
      depotArch =
        {
          x86_64-linux = "amd64";
          aarch64-linux = "arm64";
        }
        .${pkgs.system};
      depotSha256 =
        {
          x86_64-linux = "fa80b793eb74e04d5620fa19f32010d93a30250d75ccd074b6a61d65124138d1";
          aarch64-linux = "985ff1808ccdc0b9fa20b6d3be737b5cc669dfe6ba8634e9ebdb334eb115cad6";
        }
        .${pkgs.system};
      depotPackage = pkgs.stdenvNoCC.mkDerivation {
        pname = "depot";
        version = depotVersion;

        src = pkgs.fetchurl {
          url = "https://dl.depot.dev/cli/download/Linux/${depotArch}/${depotVersion}";
          sha256 = depotSha256;
        };

        dontUnpack = true;
        nativeBuildInputs = [pkgs.gnutar];

        installPhase = ''
          runHook preInstall
          mkdir -p "$out/bin"
          tmpdir=$(mktemp -d)
          ${pkgs.gnutar}/bin/tar -xzf "$src" -C "$tmpdir"
          install -m755 "$tmpdir/bin/depot" "$out/bin/depot"
          rm -rf "$tmpdir"
          runHook postInstall
        '';
      };
    in {
      options.services.maestro = {
        enable = lib.mkEnableOption "Maestro deployment controller";

        package = lib.mkOption {
          type = lib.types.package;
          default = self.packages.${pkgs.system}.default;
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
  };
}
