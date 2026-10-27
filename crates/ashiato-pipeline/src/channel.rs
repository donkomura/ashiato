use async_trait::async_trait;

use crate::{
    Delivery, DeliveryHandle, PartitionId, PartitionMessage, PartitionReader, PartitionWriter,
    TransportError,
};

#[derive(Clone)]
pub struct ChannelWriter {}

pub struct ChannelReader {}

pub fn partition_channels(
    _partitions: impl IntoIterator<Item = PartitionId>,
    _capacity: usize,
) -> (ChannelWriter, ChannelReader) {
    todo!()
}

#[async_trait]
impl PartitionWriter for ChannelWriter {
    async fn put(&self, _message: PartitionMessage) -> Result<(), TransportError> {
        todo!()
    }
}

#[async_trait]
impl PartitionReader for ChannelReader {
    async fn fetch(
        &mut self,
        _partition: &PartitionId,
        _max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        todo!()
    }

    async fn ack(&self, _deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        todo!()
    }

    async fn reset(&mut self, _partition: &PartitionId) -> Result<(), TransportError> {
        todo!()
    }
}
