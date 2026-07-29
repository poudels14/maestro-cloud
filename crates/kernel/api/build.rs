use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("../../apps/cli/Cargo.toml");
    println!("cargo:rerun-if-changed={}", manifest.display());

    let source = fs::read_to_string(&manifest)?;
    let version = package_version(&source).ok_or_else(|| {
        io::Error::other(format!(
            "failed to read the Maestro release version from `{}`",
            manifest.display()
        ))
    })?;
    println!("cargo:rustc-env=MAESTRO_VERSION={version}");
    Ok(())
}

fn package_version(manifest: &str) -> Option<&str> {
    let mut package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            package = line == "[package]";
            continue;
        }
        if !package {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim() == "version" {
            return value.trim().strip_prefix('"')?.strip_suffix('"');
        }
    }
    None
}
