{self}: {
  config,
  lib,
  pkgs,
  utils,
  ...
}: let
  cfg = config.services.maestro-rewrite;
in {
  options.services.maestro-rewrite = {
    enable = lib.mkEnableOption "rewritten Maestro control plane";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.rewrite;
      description = "Maestro rewrite package to run";
    };

    launchConfig = lib.mkOption {
      type = lib.types.str;
      description = "Absolute path to the owner-only daemon launch document outside the Nix store";
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
        assertion = lib.hasPrefix "/" cfg.launchConfig;
        message = "services.maestro-rewrite.launchConfig must be an absolute runtime path";
      }
    ];

    virtualisation.containerd.enable = true;
    virtualisation.containerd.settings = {
      plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options.SystemdCgroup = true;
    };

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

    environment.systemPackages = [cfg.package cfg.etcdPackage];

    systemd.services.maestro-rewrite = {
      description = "Rewritten Maestro control plane";
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
        ExecStart = utils.escapeSystemdExecArgs [
          "${cfg.package}/bin/maestro-daemon"
          "start"
          cfg.launchConfig
        ];
        Restart = "on-failure";
        RestartSec = 5;
        UMask = "0077";
        LimitNOFILE = 1048576;
      };
    };
  };
}
