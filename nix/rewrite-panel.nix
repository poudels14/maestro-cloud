{
  pkgs,
  rewriteVersion,
}: let
  buildNodejs = pkgs.nodejs_22;
  nodejs = pkgs.nodejs-slim_22;
  pnpm = pkgs.pnpm_10.override {nodejs = buildNodejs;};
  panelVersion =
    (builtins.fromJSON (builtins.readFile ../ui/apps/panel/package.json)).version;
  version =
    assert pkgs.lib.assertMsg
    (panelVersion == rewriteVersion)
    "rewrite panel and Rust release versions must match";
    panelVersion;
  source = pkgs.lib.fileset.toSource {
    root = ../.;
    fileset = pkgs.lib.fileset.intersection
      (pkgs.lib.fileset.gitTracked ../.)
      (pkgs.lib.fileset.unions [
        ../package.json
        ../pnpm-lock.yaml
        ../pnpm-workspace.yaml
        ../ui
      ]);
  };
in
  pkgs.stdenvNoCC.mkDerivation (finalAttrs: {
    pname = "maestro-rewrite-panel";
    inherit version;
    src = source;

    pnpmDeps = pkgs.fetchPnpmDeps {
      inherit (finalAttrs) pname src version;
      inherit pnpm;
      fetcherVersion = 3;
      hash = "sha256-sFTdaO/d4j9PGBwAkz+in4wwIESxqRoj/fTshUBSJgI=";
    };

    nativeBuildInputs = [
      buildNodejs
      pkgs.pnpmConfigHook
      pnpm
    ];

    buildPhase = ''
      runHook preBuild

      export NITRO_PRESET=node-server
      export NODE_ENV=production
      pnpm --dir ui build

      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall

      mkdir -p "$out/share/maestro-panel"
      cp -a ui/apps/panel/.output/. "$out/share/maestro-panel/"
      test -f "$out/share/maestro-panel/server/index.mjs"
      test -f "$out/share/maestro-panel/nitro.json"

      runHook postInstall
    '';

    passthru = {
      inherit nodejs;
    };

    meta = {
      description = "Production output for the rewritten Maestro panel";
      platforms = pkgs.lib.platforms.all;
    };
  })
