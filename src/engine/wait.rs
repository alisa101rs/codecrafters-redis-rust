use eyre::eyre;
use tokio::sync::broadcast;

use crate::error::RedisError;

pub struct WaitBuilder {
    receiver: broadcast::Receiver<String>,
}

impl WaitBuilder {
    pub(super) fn new(receiver: broadcast::Receiver<String>) -> Self {
        Self { receiver }
    }
    pub async fn for_keys(&mut self, keys: &[String]) -> Result<(), RedisError> {
        loop {
            let ev = self
                .receiver
                .recv()
                .await
                .map_err(|_| eyre!("Sender is closed"))?;
            if keys.contains(&ev) {
                break;
            }
        }

        Ok(())
    }
}
