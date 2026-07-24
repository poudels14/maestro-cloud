{
  pkgs,
  rewritePackage,
}: let
  version = rewritePackage.version;
in
  pkgs.dockerTools.buildLayeredImage {
    name = "maestro-daemon";
    tag = version;
    created = "1970-01-01T00:00:01Z";
    includeStorePaths = false;

    extraCommands = ''
      mkdir -p bin share/maestro-panel var/lib/maestro
      install -m0555 ${rewritePackage}/bin/maestro-daemon bin/maestro-daemon
      cp -a ${rewritePackage}/share/maestro-panel/. share/maestro-panel/
      test -f share/maestro-panel/index.html
      chmod 0700 var/lib/maestro
    '';

    config = {
      Entrypoint = ["/bin/maestro-daemon"];
      WorkingDir = "/var/lib/maestro";
      Labels = {
        "org.opencontainers.image.source" = "https://github.com/poudels14/maestro-cloud";
        "org.opencontainers.image.title" = "Maestro daemon";
        "org.opencontainers.image.version" = version;
      };
    };

    meta = {
      description = "Minimal image containing the rewritten daemon and static panel";
    };
  }
