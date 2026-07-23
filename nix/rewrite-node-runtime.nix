{
  nodejs,
  pkgs,
}: let
  runtimeRoot = "/opt/maestro/node";
  runtimeLib = "${runtimeRoot}/lib";
  interpreterName =
    builtins.baseNameOf pkgs.stdenv.cc.bintools.dynamicLinker;
  runtimeInterpreter = "${runtimeLib}/${interpreterName}";
in
  pkgs.stdenvNoCC.mkDerivation {
    pname = "maestro-node-runtime";
    inherit (nodejs) version;

    dontUnpack = true;
    dontPatchELF = true;
    dontStrip = true;

    nativeBuildInputs = [
      pkgs.nukeReferences
      pkgs.patchelf
      pkgs.pax-utils
    ];

    installPhase = ''
      runHook preInstall

      mkdir -p "$out/bin" "$out/lib"
      install -m0755 ${nodejs}/bin/node "$out/bin/node"

      while IFS= read -r dependency; do
        if [ "$dependency" = "${nodejs}/bin/node" ]; then
          continue
        fi

        destination="$out/lib/$(basename "$dependency")"
        if [ -e "$destination" ]; then
          cmp --silent "$dependency" "$destination"
        else
          install -m0755 "$dependency" "$destination"
        fi
      done < <(lddtree -l ${nodejs}/bin/node)

      for dependency in \
        ${pkgs.stdenv.cc.libc}/lib/libnss_dns.so.2 \
        ${pkgs.stdenv.cc.libc}/lib/libnss_files.so.2 \
        ${pkgs.stdenv.cc.libc}/lib/libresolv.so.2; do
        install -m0755 "$dependency" "$out/lib/$(basename "$dependency")"
      done

      patchelf \
        --set-interpreter ${pkgs.lib.escapeShellArg runtimeInterpreter} \
        --set-rpath ${pkgs.lib.escapeShellArg runtimeLib} \
        "$out/bin/node"
      for library in "$out"/lib/*; do
        if [ "$(basename "$library")" != ${pkgs.lib.escapeShellArg interpreterName} ]; then
          patchelf --set-rpath ${pkgs.lib.escapeShellArg runtimeLib} "$library"
        fi
      done

      find "$out" -type f -exec nuke-refs '{}' +
      "$out/lib/${interpreterName}" \
        --library-path "$out/lib" \
        "$out/bin/node" --version >/dev/null

      runHook postInstall
    '';

    passthru = {
      inherit runtimeInterpreter runtimeLib runtimeRoot;
      nodeBinary = "${runtimeRoot}/bin/node";
    };

    meta = {
      description = "Relocatable Node runtime for the rewritten Maestro panel";
      platforms = pkgs.lib.platforms.linux;
    };
  }
