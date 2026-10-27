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
    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError>;
    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError>;
}
