use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::{
    Delivery, DeliveryHandle, PartitionId, PartitionMessage, PartitionReader, PartitionWriter,
    SendTimes, TransportError,
};

#[derive(Clone)]
pub struct ChannelWriter {
    senders: Arc<HashMap<PartitionId, mpsc::Sender<PartitionMessage>>>,
}

pub struct ChannelReader {
    receivers: HashMap<PartitionId, mpsc::Receiver<PartitionMessage>>,
    next_handle: u64,
}

pub fn partition_channels(
    partitions: impl IntoIterator<Item = PartitionId>,
    capacity: usize,
) -> (ChannelWriter, ChannelReader) {
    let (senders, receivers) = partitions
        .into_iter()
        .map(|partition| {
            let (sender, receiver) = mpsc::channel(capacity);
            ((partition.clone(), sender), (partition, receiver))
        })
        .unzip();
    (
        ChannelWriter {
            senders: Arc::new(senders),
        },
        ChannelReader {
            receivers,
            next_handle: 0,
        },
    )
}

#[async_trait]
impl PartitionWriter for ChannelWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        self.put_timed(message, &SendTimes::default()).await
    }

    async fn put_timed(
        &self,
        message: PartitionMessage,
        times: &SendTimes,
    ) -> Result<(), TransportError> {
        let partition = message.partition().clone();
        let sender = self
            .senders
            .get(&partition)
            .ok_or_else(|| TransportError::UnknownPartition(partition.clone()))?;
        let started = Instant::now();
        match sender.try_send(message) {
            Ok(()) => {
                times.publish_wait.since(started);
                Ok(())
            }
            Err(TrySendError::Full(message)) => {
                let sent = sender.send(message).await;
                times.rejected_wait.since(started);
                sent.map_err(|_| TransportError::Closed(partition))
            }
            Err(TrySendError::Closed(_)) => Err(TransportError::Closed(partition)),
        }
    }
}

impl ChannelReader {
    fn deliver(&mut self, message: PartitionMessage) -> Delivery {
        let handle = DeliveryHandle::new(self.next_handle);
        self.next_handle += 1;
        Delivery { message, handle }
    }
}

#[async_trait]
impl PartitionReader for ChannelReader {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        let receiver = self
            .receivers
            .get_mut(partition)
            .ok_or_else(|| TransportError::UnknownPartition(partition.clone()))?;
        if max == 0 {
            return Ok(Vec::new());
        }
        let Some(first) = receiver.recv().await else {
            return Ok(Vec::new());
        };
        let mut messages = vec![first];
        while messages.len() < max {
            let Ok(message) = receiver.try_recv() else {
                break;
            };
            messages.push(message);
        }
        Ok(messages
            .into_iter()
            .map(|message| self.deliver(message))
            .collect())
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        if let Some(unknown) = partitions
            .iter()
            .find(|partition| !self.receivers.contains_key(*partition))
        {
            return Err(TransportError::UnknownPartition(unknown.clone()));
        }
        if partitions.is_empty() || max == 0 {
            return Ok((0, Vec::new()));
        }
        let receivers = &mut self.receivers;
        let (index, first) = std::future::poll_fn(|cx| {
            for (index, partition) in partitions.iter().enumerate() {
                let receiver = receivers
                    .get_mut(partition)
                    .expect("every given partition was found above");
                if let Poll::Ready(message) = receiver.poll_recv(cx) {
                    return Poll::Ready((index, message));
                }
            }
            Poll::Pending
        })
        .await;
        let Some(first) = first else {
            return Ok((index, Vec::new()));
        };
        let receiver = self
            .receivers
            .get_mut(&partitions[index])
            .expect("every given partition was found above");
        let mut messages = vec![first];
        while messages.len() < max {
            let Ok(message) = receiver.try_recv() else {
                break;
            };
            messages.push(message);
        }
        Ok((
            index,
            messages
                .into_iter()
                .map(|message| self.deliver(message))
                .collect(),
        ))
    }

    // A channel hands each message out once and keeps no copy, so there is nothing to acknowledge.
    async fn ack(&self, _deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        Ok(())
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        Err(TransportError::RedeliveryUnsupported(partition.clone()))
    }
}
