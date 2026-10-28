use std::time::Duration;

use async_trait::async_trait;

use crate::{PartitionId, PartitionMessage};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("partition {0} is not part of this run")]
    UnknownPartition(PartitionId),
    #[error("the reader of partition {0} is closed")]
    Closed(PartitionId),
    #[error("redelivering the messages of partition {0} is not supported in a single process")]
    RedeliveryUnsupported(PartitionId),
    #[error("the message broker failed")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryHandle(u64);

impl DeliveryHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub message: PartitionMessage,
    pub handle: DeliveryHandle,
}

#[async_trait]
pub trait PartitionWriter: Send + Sync {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError>;
}

#[async_trait]
pub trait PartitionReader: Send {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError>;
    // Fetches from the first of the partitions, in the given order, that has a message ready, or
    // waits until one of them has. Returns the index of that partition among the given ones. An
    // empty list of deliveries means that the partition is closed and drained.
    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError>;
    // Like fetch_ready, but returns None once wait has passed with no message ready. The default
    // waits as long as fetch_ready, which suits a transport that frees the room of a message as
    // soon as it is fetched rather than when it is acknowledged.
    async fn fetch_ready_within(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
        wait: Duration,
    ) -> Result<Option<(usize, Vec<Delivery>)>, TransportError> {
        let _ = wait;
        self.fetch_ready(partitions, max).await.map(Some)
    }
    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError>;
    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError>;
}
