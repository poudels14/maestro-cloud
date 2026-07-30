{
  config,
  maestroPackage,
  pkgs,
}: let
  inherit (pkgs) lib;
  buildkit = config.systemd.services.buildkitd;
  daemon = config.systemd.services.maestro;
  daemonCommand = daemon.serviceConfig.ExecStart;
in
  assert config.virtualisation.containerd.enable;
  assert config.virtualisation.containerd.settings.plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options.SystemdCgroup;
  assert config.boot.kernel.sysctl."net.ipv4.ip_forward" == 1;
  assert lib.elem "nix-command" config.nix.settings.experimental-features;
  assert lib.elem "flakes" config.nix.settings.experimental-features;
  assert lib.elem "containerd.service" buildkit.after;
  assert lib.elem "containerd.service" buildkit.requires;
  assert lib.elem "multi-user.target" buildkit.wantedBy;
  assert buildkit.serviceConfig.RuntimeDirectory == "buildkit";
  assert buildkit.serviceConfig.StateDirectory == "buildkit";
  assert lib.hasInfix "--containerd-worker=true" buildkit.serviceConfig.ExecStart;
  assert lib.elem "network-online.target" daemon.after;
  assert lib.elem "containerd.service" daemon.after;
  assert lib.elem "buildkitd.service" daemon.after;
  assert lib.elem "containerd.service" daemon.requires;
  assert lib.elem "buildkitd.service" daemon.requires;
  assert lib.elem "network-online.target" daemon.wants;
  assert lib.elem "multi-user.target" daemon.wantedBy;
  assert daemon.serviceConfig.UMask == "0077";
  assert daemon.serviceConfig.LimitNOFILE == 1048576;
  assert config.services.maestro.package == maestroPackage;
  assert config.services.maestro.source != null;
  assert lib.any (package: lib.getName package == "depot") config.environment.systemPackages;
  assert lib.hasInfix "/bin/maestro-daemon" daemonCommand;
  assert lib.hasInfix "\"start\" \"/run/maestro/launch.json\"" daemonCommand;
    pkgs.runCommand "maestro-module-check" {} ''
      touch "$out"
    ''
