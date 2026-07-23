{
  nodeRuntime,
  panel,
  pkgs,
  version,
}:
  pkgs.dockerTools.buildLayeredImage {
    name = "maestro-admin";
    tag = version;
    created = "1970-01-01T00:00:01Z";

    extraCommands = ''
      mkdir -p app opt/maestro/node
      cp -a ${panel}/share/maestro-panel/. app/
      cp -a ${nodeRuntime}/. opt/maestro/node/
    '';

    config = {
      Entrypoint = [nodeRuntime.nodeBinary];
      Cmd = ["/app/server/index.mjs"];
      WorkingDir = "/app";
      Env = [
        "HOST=0.0.0.0"
        "NODE_ENV=production"
        "PORT=80"
      ];
      ExposedPorts = {
        "80/tcp" = {};
      };
      User = "65532:65532";
      Labels = {
        "org.opencontainers.image.source" = "https://github.com/poudels14/maestro-cloud";
        "org.opencontainers.image.title" = "Maestro admin panel";
        "org.opencontainers.image.version" = version;
      };
    };

    meta = {
      description = "Minimal image containing the rewritten Maestro admin panel";
    };
  }
