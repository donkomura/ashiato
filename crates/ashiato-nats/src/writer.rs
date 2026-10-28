use ashiato_pipeline::{PartitionMessage, PartitionWriter, TransportError};
use async_nats::jetstream::Context;
use async_trait::async_trait;

use crate::broker::{NatsError, RetryPolicy, checked_run_id};

// Sends each message after the PubAck of the one before it, so that a failed send never lets a
// later position into the stream first (6.3 節).
pub struct NatsWriter {
    context: Context,
    run_id: String,
    max_message_size: usize,
    retry: RetryPolicy,
}

impl NatsWriter {
    pub fn new(
        client: async_nats::Client,
        run_id: &str,
        max_message_size: usize,
    ) -> Result<Self, NatsError> {
        Ok(Self {
            context: async_nats::jetstream::new(client),
            run_id: checked_run_id(run_id)?,
            max_message_size,
            retry: RetryPolicy::default(),
        })
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }
}

#[async_trait]
impl PartitionWriter for NatsWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        let _ = (
            &self.context,
            &self.run_id,
            self.max_message_size,
            self.retry,
        );
        todo!("{message:?}")
    }
}
