use std::time::{Duration, Instant};

use ashiato_pipeline::{BrokerSettings, PartitionId, Position, TransportError};
use async_nats::jetstream::context::PublishError;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use async_nats::jetstream::{self, Context};

use crate::codec::{CodecError, decode, events_subject, stream_name, validate_run_id};
use crate::config::{CapacityError, check_capacity, stream_config};

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
    pub(crate) retry: RetryPolicy,
}

impl EventStream {
    // A resumed analyzer finds the stream of its run with the messages that it has not
    // acknowledged yet, so an existing stream is opened rather than created anew.
    pub async fn create(
        client: async_nats::Client,
        run_id: &str,
        partitions: usize,
        broker: &BrokerSettings,
    ) -> Result<Self, NatsError> {
        let run_id = checked_run_id(run_id)?;
        check_capacity(partitions, broker)?;
        let context = jetstream::new(client);
        let stream = context
            .get_or_create_stream(stream_config(&run_id, broker))
            .await
            .map_err(|source| NatsError::CreateStream {
                stream: stream_name(&run_id),
                source: source.into(),
            })?;
        Ok(Self {
            context,
            stream,
            run_id,
            retry: RetryPolicy::default(),
        })
    }

    pub async fn open(client: async_nats::Client, run_id: &str) -> Result<Self, NatsError> {
        let run_id = checked_run_id(run_id)?;
        let context = jetstream::new(client);
        let stream = context
            .get_stream(stream_name(&run_id))
            .await
            .map_err(|source| NatsError::OpenStream {
                stream: stream_name(&run_id),
                source: source.into(),
            })?;
        Ok(Self {
            context,
            stream,
            run_id,
            retry: RetryPolicy::default(),
        })
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    pub async fn delete(self) -> Result<(), NatsError> {
        let stream = stream_name(&self.run_id);
        self.context
            .delete_stream(&stream)
            .await
            .map_err(|source| NatsError::DeleteStream {
                stream,
                source: source.into(),
            })?;
        Ok(())
    }

    // The position after the last message of the partition still in the stream, with which a
    // restarted worker skips the messages that the analyzer has not received yet (6.4 節).
    pub async fn last_published(
        &self,
        partition: &PartitionId,
    ) -> Result<Option<Position>, NatsError> {
        let subject = events_subject(&self.run_id, partition);
        let last = retrying(self.retry, || async {
            match self.stream.get_last_raw_message_by_subject(&subject).await {
                Ok(message) => Ok(Some(message)),
                Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
                Err(error) => Err(NatsError::LastMessage {
                    partition: partition.clone(),
                    source: error.into(),
                }),
            }
        })
        .await?;
        let Some(message) = last else {
            return Ok(None);
        };
        let message = decode(
            &self.run_id,
            &message.subject,
            &message.headers,
            &message.payload,
        )?;
        Ok(Some(message.end_position()))
    }

    pub fn jetstream(&self) -> &Context {
        &self.context
    }
}

pub(crate) fn checked_run_id(run_id: &str) -> Result<String, NatsError> {
    validate_run_id(run_id)?;
    Ok(run_id.to_owned())
}

// Waits longer after each failure, and gives up once the failures have lasted longer than
// give_up_after since the last success.
pub(crate) struct Backoff {
    policy: RetryPolicy,
    delay: Duration,
    failing_since: Option<Instant>,
}

impl Backoff {
    pub(crate) fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            delay: policy.initial_backoff,
            failing_since: None,
        }
    }

    pub(crate) async fn after_failure(&mut self) -> bool {
        let since = *self.failing_since.get_or_insert_with(Instant::now);
        if since.elapsed() >= self.policy.give_up_after {
            return false;
        }
        self.wait().await;
        true
    }

    pub(crate) async fn while_refused(&mut self) {
        self.failing_since = None;
        self.wait().await;
    }

    async fn wait(&mut self) {
        tokio::time::sleep(self.delay).await;
        self.delay = (self.delay * 2).min(self.policy.max_backoff);
    }
}

// Every failure of a request to the server is retried, because the server comes back with the
// same streams and consumers after a restart (10 節).
pub(crate) async fn retrying<T, F, Fut>(policy: RetryPolicy, mut attempt: F) -> Result<T, NatsError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NatsError>>,
{
    let mut backoff = Backoff::new(policy);
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if !backoff.after_failure().await {
                    return Err(error);
                }
            }
        }
    }
}
