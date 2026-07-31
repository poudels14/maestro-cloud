{self}: {
  config,
  lib,
  pkgs,
  utils,
  ...
}: let
  cfg = config.services.maestro;
  depotPackage = import ./depot-package.nix {inherit pkgs;};
in {
  options.services.maestro = {
    enable = lib.mkEnableOption "Maestro control plane";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      description = "Maestro package to run";
    };

    source = lib.mkOption {
      type = lib.types.path;
      default = self.outPath;
      description = "Maestro source revision used by the NixOS upgrade adapter";
    };

    config = lib.mkOption {
      type = lib.types.str;
      description = "Cluster configuration source fetched on every Maestro daemon start";
    };

    dataDir = lib.mkOption {
      type = lib.types.str;
      default = "/data";
      description = "Absolute node data directory containing the encrypted launch document and daemon state";
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      description = "Additional non-secret arguments passed to maestro-daemon start";
    };

    etcdPackage = lib.mkOption {
      type = lib.types.package;
      default = pkgs.etcd;
      description = "etcd package referenced by control-plane launch documents";
    };

    extraPackages = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [];
      description = "Additional host tools exposed to the Maestro daemon";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.config != "";
        message = "services.maestro.config must name a cluster configuration source";
      }
      {
        assertion = lib.hasPrefix "/" cfg.dataDir;
        message = "services.maestro.dataDir must be an absolute runtime path";
      }
    ];

    virtualisation.containerd.enable = true;
    virtualisation.containerd.settings = {
      plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options.SystemdCgroup = true;
    };
    boot.kernel.sysctl."net.ipv4.ip_forward" = 1;
    nix.settings.experimental-features = [
      "nix-command"
      "flakes"
    ];

    systemd.services.buildkitd = {
      description = "BuildKit daemon for Maestro";
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

    environment.systemPackages = [cfg.package cfg.etcdPackage depotPackage];

    systemd.services.maestro = {
      description = "Maestro control plane";
      after = [
        "network-online.target"
        "containerd.service"
        "buildkitd.service"
      ];
      wants = ["network-online.target"];
      requires = [
        "containerd.service"
        "buildkitd.service"
      ];
      wantedBy = ["multi-user.target"];

      path =
        [
          cfg.etcdPackage
          pkgs.buildkit
          pkgs.coreutils
          depotPackage
          pkgs.git
          pkgs.iproute2
          pkgs.kmod
          pkgs.nftables
          pkgs.nix
          pkgs.nixos-rebuild
          pkgs.util-linux
        ]
        ++ cfg.extraPackages;

      serviceConfig = {
        Type = "simple";
        ExecStart = utils.escapeSystemdExecArgs (
          [
            "${cfg.package}/bin/maestro-daemon"
            "start"
            "--config"
            cfg.config
            "--data-dir"
            cfg.dataDir
          ]
          ++ cfg.extraArgs
        );
        Restart = "on-failure";
        RestartSec = 5;
        UMask = "0077";
        LimitNOFILE = 1048576;
      };
    };
  };
}
