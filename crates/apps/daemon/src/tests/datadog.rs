use kernel_api::SecretValue;
use logs::InMemoryLogStoreRuntime;

use crate::datadog::{
    DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig, build_datadog_sinks,
    configure_datadog,
};

#[test]
fn metric_sink_is_opt_in_and_uses_an_independent_cursor_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let log_store = InMemoryLogStoreRuntime::new();
    let disabled = configure_datadog(Some(&config(false)), "prod", "node-one")?
        .ok_or("configured Datadog missing")?;
    let disabled = build_datadog_sinks(Some(disabled), &log_store);
    assert_eq!(disabled.logs.len(), 1);
    assert!(disabled.metrics.is_empty());
    assert!(disabled.host_metrics.is_empty());

    let enabled = configure_datadog(Some(&config(true)), "prod", "node-one")?
        .ok_or("configured Datadog missing")?;
    let enabled = build_datadog_sinks(Some(enabled), &log_store);
    assert_eq!(enabled.logs.len(), 1);
    assert_eq!(enabled.metrics.len(), 1);
    assert_eq!(enabled.host_metrics.len(), 1);
    assert_eq!(
        enabled
            .metrics
            .first()
            .ok_or("Datadog metric sink missing")?
            .id()
            .as_str(),
        "datadog"
    );
    assert_eq!(
        enabled
            .host_metrics
            .first()
            .ok_or("Datadog host metric sink missing")?
            .id()
            .as_str(),
        "datadog"
    );
    Ok(())
}

fn config(enabled: bool) -> DatadogLaunchConfig {
    DatadogLaunchConfig {
        api_key: SecretValue::new("secret"),
        site: "datadoghq.com".to_owned(),
        include_ingress_logs: true,
        include_tailscale_logs: true,
        logs: DatadogLogsLaunchConfig::default(),
        metrics: DatadogMetricsLaunchConfig {
            enabled,
            tags: vec!["env:test".to_owned()],
        },
    }
}
