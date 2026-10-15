{
  description = "Maestro - deployment system";

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
    maestroVersion = (builtins.fromTOML (builtins.readFile ./crates/maestro/Cargo.toml)).package.version;
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

          cargoBuildFlags = ["--package" "maestro"];
          doCheck = false;

          nativeBuildInputs = with pkgs; [pkg-config protobuf];

          buildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.apple-sdk_15
          ];

          postInstall = ''
            ln -s maestro "$out/bin/maestro-log-driver"
          '';
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
      dataDir = toString cfg.dataDir;
      buildkitConfig = pkgs.writeText "buildkitd.toml" ''
        [worker.oci]
          enabled = false

        [worker.containerd]
          enabled = true
          namespace = "maestro"
          snapshotter = "overlayfs"
      '';
      provision = pkgs.writeShellScript "maestro-provision" ''
        set -eu
        if [ -f ${dataDir}/node.json ]; then
          exit 0
        fi
        ${
          if cfg.join.server != null
          then ''
            ${cfg.package}/bin/maestro join \
              --data-dir ${dataDir} \
              --server ${cfg.join.server} \
              --token "$(cat ${toString cfg.join.tokenFile})" \
              --role ${cfg.join.role} \
              --advertise ${cfg.advertise}${lib.optionalString (cfg.configFile != null) " --config ${toString cfg.configFile}"}
          ''
          else ''
            ${cfg.package}/bin/maestro server init \
              --data-dir ${dataDir} \
              --advertise ${cfg.advertise}${lib.optionalString (cfg.configFile != null) " --config ${toString cfg.configFile}"}
          ''
        }
      '';
      agentArgs =
        [
          "${cfg.package}/bin/maestro"
          "agent"
          "--data-dir"
          dataDir
          "--listen"
          cfg.apiListen
        ]
        ++ lib.optionals (cfg.configFile != null) ["--config" (toString cfg.configFile)]
        ++ lib.optionals cfg.adminUi ["--admin-source" "/etc/maestro/source"]
        ++ cfg.extraArgs;
    in {
      options.services.maestro = {
        enable = lib.mkEnableOption "Maestro node";

        package = lib.mkOption {
          type = lib.types.package;
          default = self.packages.${pkgs.system}.default;
          description = "The maestro package to use.";
        };

        source = lib.mkOption {
          type = lib.types.path;
          default = self.outPath;
          description = "Maestro source tree, used to build the admin UI image once per version.";
        };

        dataDir = lib.mkOption {
          type = lib.types.path;
          default = "/var/lib/maestro";
          description = "Directory for cluster state, keys, logs, and build workspaces.";
        };

        advertise = lib.mkOption {
          type = lib.types.str;
          example = "10.20.0.11:7400";
          description = "Private IP:port other nodes use to reach this node's gRPC endpoint.";
        };

        apiListen = lib.mkOption {
          type = lib.types.str;
          default = "127.0.0.1:3000";
          description = "Address of the token-protected HTTP API on voter nodes.";
        };

        configFile = lib.mkOption {
          type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
          default = null;
          example = "aws-secret://maestro/prod/config.json";
          description = "Optional cluster config: a maestro.jsonc path or an aws-secret:// source.";
        };

        join = {
          server = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            default = null;
            example = "10.20.0.11:7400";
            description = "Voter to join through. When null, the first start initializes a new cluster.";
          };

          tokenFile = lib.mkOption {
            type = lib.types.nullOr lib.types.path;
            default = null;
            description = "File containing the join token from `maestro server join-token`.";
          };

          role = lib.mkOption {
            type = lib.types.enum ["voter" "worker"];
            default = "worker";
            description = "Role of a joining node.";
          };
        };

        registry = {
          dockerConfigFile = lib.mkOption {
            type = lib.types.nullOr lib.types.path;
            default = null;
            example = "/run/secrets/docker-config.json";
            description = ''
              Docker config.json with registry credentials (`auths`, `credHelpers`, `credsStore`)
              for private image pulls, digest lookups, and BuildKit pushes. It is passed to the
              unit as a systemd credential, so keep it outside the Nix store. When null, root's
              ~/.docker/config.json is used.
            '';
          };

          credentialHelpers = lib.mkOption {
            type = lib.types.listOf lib.types.package;
            default = [];
            example = lib.literalExpression "[ pkgs.amazon-ecr-credential-helper ]";
            description = "Docker credential helpers (docker-credential-*) available to Maestro and BuildKit.";
          };
        };

        adminUi = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Run the admin UI container on voter nodes.";
        };

        openFirewall = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Open the gRPC (7400/tcp), WireGuard (51820/udp), and ingress ports.";
        };

        ingressPorts = lib.mkOption {
          type = lib.types.listOf lib.types.port;
          default = [80 443];
          description = "Ingress ports to open when openFirewall is set; must match the cluster config.";
        };

        telemetryMemoryMax = lib.mkOption {
          type = lib.types.str;
          default = "1G";
          description = "Memory cap for the telemetry process.";
        };

        buildMemoryMax = lib.mkOption {
          type = lib.types.str;
          default = "50%";
          description = "Memory cap for image builds.";
        };

        awsLinkLocalRoutes = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Pin AWS link-local endpoints (IMDS, NTP, DNS) to the primary interface.";
        };

        extraArgs = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
          description = "Extra arguments passed to `maestro agent`.";
        };
      };

      config = lib.mkIf cfg.enable {
        assertions = [
          {
            assertion = cfg.join.server == null || cfg.join.tokenFile != null;
            message = "services.maestro.join.tokenFile is required when join.server is set";
          }
        ];

        virtualisation.containerd.enable = true;

        boot.kernelModules = ["wireguard"];
        boot.kernel.sysctl."net.ipv4.ip_forward" = 1;

        networking.firewall = lib.mkIf cfg.openFirewall {
          allowedTCPPorts = [7400] ++ cfg.ingressPorts;
          allowedUDPPorts = [51820];
          trustedInterfaces = ["maestro0" "maestro-wg"];
        };

        systemd.slices = {
          maestro-system.sliceConfig.CPUWeight = 1000;
          maestro-workloads.sliceConfig.CPUWeight = 100;
          maestro-builds.sliceConfig = {
            CPUWeight = 50;
            MemoryMax = cfg.buildMemoryMax;
          };
        };

        systemd.services.buildkitd = {
          description = "BuildKit daemon";
          after = ["containerd.service"];
          requires = ["containerd.service"];
          wantedBy = ["multi-user.target"];
          path = [pkgs.git pkgs.runc];
          serviceConfig = {
            Type = "simple";
            Slice = "maestro-builds.slice";
            ExecStart = "${pkgs.buildkit}/bin/buildkitd --config ${buildkitConfig} --addr unix:///run/buildkit/buildkitd.sock";
            Restart = "on-failure";
            RestartSec = 3;
            StateDirectory = "buildkit";
            RuntimeDirectory = "buildkit";
          };
        };

        environment.systemPackages = [
          cfg.package
          pkgs.buildkit
          pkgs.nerdctl
          pkgs.wireguard-tools
          pkgs.nftables
        ];

        system.activationScripts.maestro-source = lib.mkIf cfg.adminUi ''
          mkdir -p /etc/maestro
          rm -rf /etc/maestro/source
          cp -r ${cfg.source} /etc/maestro/source
          chmod -R u+w /etc/maestro/source
        '';

        system.activationScripts.maestro-nix-gc = ''
          ${pkgs.nix}/bin/nix-collect-garbage -d || true
        '';

        systemd.services.aws-linklocal-routes = lib.mkIf cfg.awsLinkLocalRoutes {
          description = "Route AWS link-local traffic via primary interface";
          after = ["network-online.target"];
          before = ["maestro.service"];
          wants = ["network-online.target"];
          wantedBy = ["multi-user.target"];
          path = [pkgs.iproute2 pkgs.gawk];
          serviceConfig.Type = "oneshot";
          script = ''
            set -eu
            iface=$(ip -4 route show default | awk 'NR==1 {print $5}')
            [ -n "$iface" ] || exit 1
            case "$iface" in
              veth*|cni*|maestro*) exit 1 ;;
            esac
            for address in 169.254.169.254 169.254.169.123 169.254.169.253; do
              ip -4 route replace table 100 "$address/32" dev "$iface" scope link
            done
            ip -4 rule add pref 100 to 169.254.169.254/32 table 100 2>/dev/null || true
            ip -4 rule add pref 101 to 169.254.169.123/32 table 100 2>/dev/null || true
            ip -4 rule add pref 102 to 169.254.169.253/32 table 100 2>/dev/null || true
          '';
        };

        systemd.timers.aws-linklocal-routes = lib.mkIf cfg.awsLinkLocalRoutes {
          description = "Periodically reconcile AWS link-local routes";
          wantedBy = ["timers.target"];
          timerConfig = {
            OnBootSec = "30s";
            OnUnitActiveSec = "60s";
            Unit = "aws-linklocal-routes.service";
          };
        };

        systemd.services.maestro = {
          description = "Maestro node";
          after = ["network-online.target" "containerd.service" "buildkitd.service"];
          wants = ["network-online.target" "buildkitd.service"];
          requires = ["containerd.service"];
          wantedBy = ["multi-user.target"];
          path = [
            pkgs.cni-plugins
            pkgs.iproute2
            pkgs.nftables
            pkgs.wireguard-tools
            pkgs.buildkit
            pkgs.git
            pkgs.openssh
            pkgs.systemd
            pkgs.coreutils
          ] ++ cfg.registry.credentialHelpers;
          environment =
            {
              CNI_PATH = "${pkgs.cni-plugins}/bin";
            }
            // lib.optionalAttrs (cfg.registry.dockerConfigFile != null) {
              DOCKER_CONFIG = "%d";
            };
          serviceConfig = {
            Type = "simple";
            Slice = "maestro-system.slice";
            LoadCredential = lib.optional (cfg.registry.dockerConfigFile != null) "config.json:${cfg.registry.dockerConfigFile}";
            ExecStartPre = "${provision}";
            ExecStart = utils.escapeSystemdExecArgs agentArgs;
            Restart = "always";
            RestartSec = 5;
          };
        };

        systemd.services.maestro-telemetry = {
          description = "Maestro telemetry";
          after = ["maestro.service"];
          wantedBy = ["multi-user.target"];
          serviceConfig = {
            Type = "simple";
            Slice = "maestro-system.slice";
            MemoryMax = cfg.telemetryMemoryMax;
            ExecStart = utils.escapeSystemdExecArgs ([
              "${cfg.package}/bin/maestro"
              "telemetry"
              "--log-dir"
              "${dataDir}/logs"
              "--data-dir"
              "${dataDir}/telemetry"
              "--node-dir"
              dataDir
            ]
            ++ lib.optionals (cfg.configFile != null) ["--config" (toString cfg.configFile)]);
            Restart = "always";
            RestartSec = 5;
          };
        };
      };
    };
  };
}
