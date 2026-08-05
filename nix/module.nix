{self}: {
  config,
  lib,
  pkgs,
  utils,
  ...
}: let
  cfg = config.services.maestro;
  depotPackage = import ./depot-package.nix {inherit pkgs;};
  userNamespaceHostBase = 1048576;
  userNamespaceRangeSize = 1073741824;
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
      {
        assertion = lib.versionAtLeast pkgs.containerd.version "2.0";
        message = "Maestro workload user namespaces require containerd 2.0 or newer";
      }
      {
        assertion = lib.versionAtLeast pkgs.runc.version "1.2";
        message = "Maestro workload user namespaces require runc 1.2 or newer";
      }
      {
        assertion = lib.versionAtLeast config.boot.kernelPackages.kernel.version "6.3";
        message = "Maestro workload user namespaces require Linux 6.3 or newer";
      }
    ];

    users.groups.maestro-userns = {};
    users.users.maestro-userns = {
      isSystemUser = true;
      group = "maestro-userns";
      subUidRanges = [
        {
          startUid = userNamespaceHostBase;
          count = userNamespaceRangeSize;
        }
      ];
      subGidRanges = [
        {
          startGid = userNamespaceHostBase;
          count = userNamespaceRangeSize;
        }
      ];
    };

    virtualisation.containerd.enable = true;
    virtualisation.containerd.settings = {
      plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options.SystemdCgroup = true;
    };
    # dhcpcd otherwise assigns IPv4LL addresses to Maestro's dynamically
    # created links. Its connected 169.254.0.0/16 route can capture cloud
    # metadata traffic that must continue through the node's primary ENI.
    networking.dhcpcd.denyInterfaces = [
      "maestro0"
      "mh*"
      "mp*"
      "wg0"
    ];
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
            "--containerd-socket"
            "/run/containerd/containerd.sock"
            "--etcd-binary"
            "${cfg.etcdPackage}/bin/etcd"
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
