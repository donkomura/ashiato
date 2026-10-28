use ashiato_pipeline::{
    BrokerSettings, Delivery, DeliveryHandle, PartitionId, PartitionReader, TransportError,
};
use async_trait::async_trait;

use crate::broker::{EventStream, NatsError, RetryPolicy};

pub struct NatsReader {
    retry: RetryPolicy,
}

impl EventStream {
    // Creates the consumers of the partitions anew, so that a restarted analyzer receives the
    // unacknowledged messages in the order of the stream (10 節).
    pub async fn reader(
        &self,
        partitions: Vec<PartitionId>,
        broker: &BrokerSettings,
        max_ack_pending: i64,
    ) -> Result<NatsReader, NatsError> {
        todo!("{partitions:?}{broker:?}{max_ack_pending}")
    }
}

impl NatsReader {
    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry }
    }
}

#[async_trait]
impl PartitionReader for NatsReader {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        todo!("{partition}{max}{:?}", self.retry)
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        todo!("{partitions:?}{max}")
    }

    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        todo!("{deliveries:?}")
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        todo!("{partition}")
    }
}
