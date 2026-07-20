use std::future::pending;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime};

pub(super) struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        pending::<()>().await;
    }
}
