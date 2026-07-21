use std::io::{self, Write};

use clap::Parser;

fn main() {
    let cli = maestro_cli::Cli::parse();
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    if let Err(error) = maestro_cli::run(cli, &mut input, &mut output) {
        let _ = writeln!(io::stderr().lock(), "[maestro]: {error}");
        std::process::exit(1);
    }
}
