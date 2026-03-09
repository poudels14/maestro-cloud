use tokio::{
    sync::broadcast,
    task::JoinHandle,
    time::{Duration, Instant},
};

const SIGNAL_CHANNEL_CAPACITY: usize = 16;
const CTRL_C_CHAIN_WINDOW: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownEvent {
    Graceful,
    Force,
}

pub(crate) fn spawn_shutdown_signal_bus()
-> Result<(broadcast::Sender<ShutdownEvent>, JoinHandle<()>), String> {
    let (tx, _) = broadcast::channel(SIGNAL_CHANNEL_CAPACITY);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|err| format!("failed to install SIGTERM handler: {err}"))?;
    let mut sigquit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit())
        .map_err(|err| format!("failed to install SIGQUIT handler: {err}"))?;
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .map_err(|err| format!("failed to install SIGHUP handler: {err}"))?;

    let signal_tx = tx.clone();
    let signal_task = tokio::spawn(async move {
        let mut ctrl_c_count: u8 = 0;
        let mut last_ctrl_c_at: Option<Instant> = None;
        let mut graceful_requested = false;
        loop {
            tokio::select! {
                ctrl_c = tokio::signal::ctrl_c() => {
                    if ctrl_c.is_err() {
                        break;
                    }
                    let now = Instant::now();
                    ctrl_c_count = match last_ctrl_c_at {
                        Some(last) if now.duration_since(last) <= CTRL_C_CHAIN_WINDOW => {
                            ctrl_c_count.saturating_add(1)
                        }
                        _ => 1,
                    };
                    last_ctrl_c_at = Some(now);

                    match ctrl_c_count {
                        1 => {
                            eprintln!("[maestro]: press ctrl+c again to stop gracefully (third ctrl+c will force shutdown).");
                        }
                        2 => {
                            if !graceful_requested {
                                graceful_requested = true;
                                eprintln!("[maestro]: stopping all jobs gracefully.");
                                let _ = signal_tx.send(ShutdownEvent::Graceful);
                            }
                        }
                        _ => {
                            eprintln!("[maestro]: forcing shutdown.");
                            let _ = signal_tx.send(ShutdownEvent::Force);
                        }
                    }
                }
                _ = sigterm.recv() => {
                    if !graceful_requested {
                        graceful_requested = true;
                        eprintln!("[maestro]: received SIGTERM; stopping all jobs and terminate cleanly.");
                        let _ = signal_tx.send(ShutdownEvent::Graceful);
                    }
                }
                _ = sigquit.recv() => {
                    if !graceful_requested {
                        graceful_requested = true;
                        eprintln!("[maestro]: received SIGQUIT; stopping all jobs and terminate cleanly.");
                        let _ = signal_tx.send(ShutdownEvent::Graceful);
                    }
                }
                _ = sighup.recv() => {
                    if !graceful_requested {
                        graceful_requested = true;
                        eprintln!("[maestro]: received SIGHUP; stopping all jobs and terminate cleanly.");
                        let _ = signal_tx.send(ShutdownEvent::Graceful);
                    }
                }
            }
        }
    });

    Ok((tx, signal_task))
}
