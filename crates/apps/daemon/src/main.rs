use std::path::PathBuf;

use daemon::{launch_control_plane, load_launch_config};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("maestro daemon failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let program = arguments
        .next()
        .and_then(|value| PathBuf::from(value).file_name().map(|name| name.to_owned()))
        .and_then(|name| name.to_str().map(str::to_owned))
        .unwrap_or_else(|| "daemon".to_owned());
    let path = arguments.next().ok_or_else(|| usage_error(&program))?;
    if arguments.next().is_some() {
        return Err(usage_error(&program).into());
    }
    let path = PathBuf::from(path);
    let config = load_launch_config(&path)?;
    let running = launch_control_plane(config).await?;
    shutdown_signal().await?;
    running.shutdown().await?;
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            signal = terminate.recv() => signal.map_or_else(
                || Err(std::io::Error::other("SIGTERM listener closed")),
                |_| Ok(()),
            ),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

fn usage_error(program: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("usage: {program} <launch-config.json>"),
    )
}
