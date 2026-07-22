{
  pkgs,
  rewriteBinaries,
}: let
  version = rewriteBinaries.version;
in
  pkgs.dockerTools.buildLayeredImage {
    name = "maestro-daemon";
    tag = version;
    created = "1970-01-01T00:00:01Z";
    includeStorePaths = false;

    extraCommands = ''
      mkdir -p bin var/lib/maestro
      install -m0555 ${rewriteBinaries}/bin/maestro-daemon bin/maestro-daemon
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
      description = "Minimal image containing the static rewritten Maestro daemon";
    };
  }
