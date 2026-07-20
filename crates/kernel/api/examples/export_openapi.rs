use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: export_openapi <output-path>")?;
    let document = kernel_api::openapi_document();
    let encoded = serde_json::to_vec_pretty(&document)?;
    std::fs::write(output, encoded)?;
    Ok(())
}
