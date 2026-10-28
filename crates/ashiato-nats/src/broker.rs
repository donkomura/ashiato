use std::time::Duration;

use ashiato_pipeline::{BrokerSettings, PartitionId, Position, TransportError};
use async_nats::jetstream::Context;
use async_nats::jetstream::context::PublishError;
use async_nats::jetstream::stream::Stream;

use crate::codec::{CodecError, validate_run_id};
use crate::config::CapacityError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    // How long requests may keep failing before the error stops the role. The refusals of a full
    // stream do not count, because they are the back pressure of 7.2 節 rather than a failure.
    pub give_up_after: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            give_up_after: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NatsError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Capacity(#[from] CapacityError),
    #[error("failed to create stream {stream}")]
    CreateStream {
        stream: String,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to open stream {stream}")]
    OpenStream {
        stream: String,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to delete stream {stream}")]
    DeleteStream {
        stream: String,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to create consumer {consumer}")]
    Consumer {
        consumer: String,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to publish the message of partition {partition} at {position:?}")]
    Publish {
        partition: PartitionId,
        position: Position,
        #[source]
        source: PublishError,
    },
    #[error("failed to fetch the messages of partition {partition}")]
    Fetch {
        partition: PartitionId,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to read the last message of partition {partition}")]
    LastMessage {
        partition: PartitionId,
        #[source]
        source: async_nats::Error,
    },
    #[error("failed to acknowledge the committed messages")]
    Ack(#[source] async_nats::Error),
}

impl From<NatsError> for TransportError {
    fn from(error: NatsError) -> Self {
        TransportError::Broker(Box::new(error))
    }
}

// The stream of one run (7.1 節). The analyzer creates it at the start of the run and deletes it
// at the end; the workers open the existing one.
pub struct EventStream {
    pub(crate) context: Context,
    pub(crate) stream: Stream,
    pub(crate) run_id: String,
}

impl EventStream {
    pub async fn create(
        client: async_nats::Client,
        run_id: &str,
        partitions: usize,
        broker: &BrokerSettings,
    ) -> Result<Self, NatsError> {
        let _ = client;
        todo!("{run_id}{partitions}{broker:?}")
    }

    pub async fn open(client: async_nats::Client, run_id: &str) -> Result<Self, NatsError> {
        let _ = client;
        todo!("{run_id}")
    }

    pub async fn delete(self) -> Result<(), NatsError> {
        let _ = (&self.stream, &self.run_id);
        todo!()
    }

    // The position after the last message of the partition still in the stream, with which a
    // restarted worker skips the messages that the analyzer has not received yet (6.4 節).
    pub async fn last_published(
        &self,
        partition: &PartitionId,
    ) -> Result<Option<Position>, NatsError> {
        todo!("{partition}")
    }

    pub fn jetstream(&self) -> &Context {
        &self.context
    }
}

pub(crate) fn checked_run_id(run_id: &str) -> Result<String, NatsError> {
    validate_run_id(run_id)?;
    Ok(run_id.to_owned())
}
