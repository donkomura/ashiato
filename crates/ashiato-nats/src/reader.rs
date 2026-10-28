use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use ashiato_pipeline::{
    BrokerSettings, Delivery, DeliveryHandle, PartitionId, PartitionReader, TransportError,
};
use async_nats::HeaderMap;
use async_nats::jetstream::message::Acker;
use async_nats::jetstream::stream::{ConsumerErrorKind, Stream};
use async_nats::jetstream::{self, consumer};
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::broker::{EventStream, NatsError, RetryPolicy, retrying};
use crate::codec::{consumer_name, decode};
use crate::config::consumer_config;

type PullConsumer = consumer::Consumer<consumer::pull::Config>;

// A fetch asks each consumer only for the messages that it has now, so that waiting on one
// partition never holds back the others (9 節). The idle polls back off up to this interval.
const IDLE_POLL_INITIAL: Duration = Duration::from_millis(1);
const IDLE_POLL_MAX: Duration = Duration::from_millis(100);

pub struct NatsReader {
    client: async_nats::Client,
    stream: Stream,
    run_id: String,
    broker: BrokerSettings,
    max_ack_pending: i64,
    consumers: HashMap<PartitionId, PullConsumer>,
    ackers: Mutex<HashMap<u64, Acker>>,
    next_handle: u64,
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
        let mut reader = NatsReader {
            client: self.client.clone(),
            stream: self.stream.clone(),
            run_id: self.run_id.clone(),
            broker: *broker,
            max_ack_pending,
            consumers: HashMap::new(),
            ackers: Mutex::new(HashMap::new()),
            next_handle: 0,
            retry: self.retry,
        };
        for partition in partitions {
            reader.renew(&partition).await?;
        }
        Ok(reader)
    }
}

impl NatsReader {
    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    async fn renew(&mut self, partition: &PartitionId) -> Result<(), NatsError> {
        let name = consumer_name(partition);
        let config = consumer_config(&self.run_id, partition, &self.broker, self.max_ack_pending);
        let stream = &self.stream;
        let consumer = retrying(self.retry, || async {
            match stream.delete_consumer(&name).await {
                Ok(_) => {}
                Err(error)
                    if matches!(error.kind(), ConsumerErrorKind::JetStream(ref failure)
                        if failure.error_code() == jetstream::ErrorCode::CONSUMER_NOT_FOUND) => {}
                Err(error) => {
                    return Err(NatsError::Consumer {
                        consumer: name.clone(),
                        source: error.into(),
                    });
                }
            }
            stream
                .create_consumer(config.clone())
                .await
                .map_err(|error| NatsError::Consumer {
                    consumer: name.clone(),
                    source: error.into(),
                })
        })
        .await?;
        self.consumers.insert(partition.clone(), consumer);
        Ok(())
    }

    async fn fetch_now(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        let consumer = self
            .consumers
            .get(partition)
            .ok_or_else(|| TransportError::UnknownPartition(partition.clone()))?;
        let fetch_error = |source| NatsError::Fetch {
            partition: partition.clone(),
            source,
        };
        let messages = retrying(self.retry, || async {
            let mut batch = consumer
                .fetch()
                .max_messages(max)
                .messages()
                .await
                .map_err(|error| fetch_error(error.into()))?;
            let mut messages = Vec::new();
            while let Some(message) = batch.next().await {
                match message {
                    Ok(message) => messages.push(message),
                    // The messages received before the failure are returned; the rest stay in
                    // the stream for a later fetch.
                    Err(_) if !messages.is_empty() => break,
                    Err(error) => return Err(fetch_error(error)),
                }
            }
            Ok(messages)
        })
        .await?;
        let mut deliveries = Vec::with_capacity(messages.len());
        for message in messages {
            let (message, acker) = message.split();
            let headers = message.headers.unwrap_or_else(HeaderMap::new);
            let message = decode(&self.run_id, &message.subject, &headers, &message.payload)
                .map_err(NatsError::from)?;
            let handle = DeliveryHandle::new(self.next_handle);
            self.next_handle += 1;
            self.ackers
                .lock()
                .expect("no thread panics while holding the ackers")
                .insert(handle.id(), acker);
            deliveries.push(Delivery { message, handle });
        }
        Ok(deliveries)
    }
}

#[async_trait]
impl PartitionReader for NatsReader {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        let mut idle = IDLE_POLL_INITIAL;
        loop {
            let deliveries = self.fetch_now(partition, max).await?;
            if !deliveries.is_empty() || max == 0 {
                return Ok(deliveries);
            }
            tokio::time::sleep(idle).await;
            idle = (idle * 2).min(IDLE_POLL_MAX);
        }
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        if partitions.is_empty() || max == 0 {
            return Ok((0, Vec::new()));
        }
        let mut idle = IDLE_POLL_INITIAL;
        loop {
            for (index, partition) in partitions.iter().enumerate() {
                let deliveries = self.fetch_now(partition, max).await?;
                if !deliveries.is_empty() {
                    return Ok((index, deliveries));
                }
            }
            tokio::time::sleep(idle).await;
            idle = (idle * 2).min(IDLE_POLL_MAX);
        }
    }

    // An acknowledgement that does not reach the server only leaves the message to be delivered
    // again, which the analyzer drops by its position (10 節).
    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        let ackers: Vec<Acker> = {
            let mut ackers = self
                .ackers
                .lock()
                .expect("no thread panics while holding the ackers");
            deliveries
                .iter()
                .filter_map(|handle| ackers.remove(&handle.id()))
                .collect()
        };
        for acker in &ackers {
            retrying(self.retry, || async {
                acker.ack().await.map_err(NatsError::Ack)
            })
            .await?;
        }
        retrying(self.retry, || async {
            self.client
                .flush()
                .await
                .map_err(|error| NatsError::Ack(error.into()))
        })
        .await?;
        Ok(())
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        if !self.consumers.contains_key(partition) {
            return Err(TransportError::UnknownPartition(partition.clone()));
        }
        self.renew(partition).await?;
        Ok(())
    }
}
