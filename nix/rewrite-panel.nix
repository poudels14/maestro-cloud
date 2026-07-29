{
  pkgs,
  rewriteVersion,
}: let
  buildNodejs = pkgs.nodejs_22;
  pnpm = pkgs.pnpm_10.override {nodejs = buildNodejs;};
  version = rewriteVersion;
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
      hash = "sha256-2qdAV5l/9xS+r+73kwjHAxPodMTFEaMV+dleMRXSb74=";
    };

    nativeBuildInputs = [
      buildNodejs
      pkgs.pnpmConfigHook
      pnpm
    ];

    buildPhase = ''
      runHook preBuild

      export NODE_ENV=production
      pnpm --dir ui build

      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall

      mkdir -p "$out/share/maestro-panel"
      cp -a ui/apps/panel/dist/client/. "$out/share/maestro-panel/"
      test -f "$out/share/maestro-panel/index.html"
      test -d "$out/share/maestro-panel/assets"

      runHook postInstall
    '';

    meta = {
      description = "Static production assets for the rewritten Maestro panel";
      platforms = pkgs.lib.platforms.all;
    };
  })
