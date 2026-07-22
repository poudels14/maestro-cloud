use crate::{BackupStatsSnapshot, ControllerStatsSnapshot, StatsWarning};

/// Derives stable sink, dead-letter, backup, heartbeat, and version diagnostics.
pub fn derive_stats_warnings(
    controller: Option<&ControllerStatsSnapshot>,
    backup: &BackupStatsSnapshot,
    heartbeat_age_ms: Option<u64>,
    now_ms: i64,
    api_version: &str,
) -> Vec<StatsWarning> {
    let mut warnings = Vec::new();
    match heartbeat_age_ms {
        None => push(
            &mut warnings,
            "controller-heartbeat-missing",
            "error",
            "No stats report is available from the controller".to_owned(),
        ),
        Some(age) if age > 30_000 => push(
            &mut warnings,
            "controller-heartbeat-stale",
            "error",
            format!("Controller stats report is {}s old", age / 1_000),
        ),
        _ => {}
    }
    if let Some(controller) = controller {
        controller_warnings(&mut warnings, controller, now_ms, api_version);
    }
    if backup.configured && backup.last_error_at_ms > backup.last_success_at_ms {
        push(
            &mut warnings,
            "log-backup-failing",
            "error",
            "The latest S3 log-backup attempt failed".to_owned(),
        );
    }
    warnings
}

fn controller_warnings(
    warnings: &mut Vec<StatsWarning>,
    controller: &ControllerStatsSnapshot,
    now_ms: i64,
    api_version: &str,
) {
    if controller.version != api_version {
        push(
            warnings,
            "component-version-mismatch",
            "warning",
            format!(
                "Controller {} and API {} are running different versions",
                controller.version, api_version
            ),
        );
    }
    for sink in &controller.sinks {
        if sink.consecutive_failures > 0 {
            push(
                warnings,
                &format!("sink-{}-failing", sink.id),
                "error",
                format!(
                    "{} log sink has failed {} consecutive time(s)",
                    sink.id, sink.consecutive_failures
                ),
            );
        } else if let Some(oldest) = sink.oldest_pending_at_ms {
            let age = now_ms.saturating_sub(oldest);
            let progressing = sink
                .last_cursor_advance_at_ms
                .is_some_and(|last| now_ms.saturating_sub(last) <= 30_000);
            if sink.pending_entries > 0 && age > 60_000 && !progressing {
                push(
                    warnings,
                    &format!("sink-{}-behind", sink.id),
                    "warning",
                    format!(
                        "{} log sink is {}s behind with {} pending entries",
                        sink.id,
                        age / 1_000,
                        sink.pending_entries
                    ),
                );
            }
        }
    }
    if controller.dead_letters.count > 0 {
        let severity = if controller.dead_letters.count.saturating_mul(10)
            >= controller.dead_letters.capacity.saturating_mul(9)
        {
            "error"
        } else {
            "warning"
        };
        push(
            warnings,
            "datadog-dead-letters",
            severity,
            format!(
                "Datadog has {} quarantined log entries",
                controller.dead_letters.count
            ),
        );
    }
}

fn push(warnings: &mut Vec<StatsWarning>, code: &str, severity: &str, message: String) {
    warnings.push(StatsWarning {
        code: code.to_owned(),
        severity: severity.to_owned(),
        message,
    });
}
