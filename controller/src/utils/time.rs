use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};

pub fn current_time_millis() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| anyhow!("system clock is before unix epoch: {err}"))?;
    Ok(u64::try_from(now.as_millis())?)
}
