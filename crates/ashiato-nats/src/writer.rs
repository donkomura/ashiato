use ashiato_pipeline::{PartitionMessage, PartitionWriter, TransportError};
use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::{self, Context};
use async_trait::async_trait;

use crate::broker::{Backoff, NatsError, RetryPolicy, checked_run_id};
use crate::codec::{EncodedMessage, encode_within};

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
            context: jetstream::new(client),
            run_id: checked_run_id(run_id)?,
            max_message_size,
            retry: RetryPolicy::default(),
        })
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    async fn publish(
        &self,
        message: &PartitionMessage,
        part: EncodedMessage,
    ) -> Result<(), NatsError> {
        let mut backoff = Backoff::new(self.retry);
        loop {
            let sent = self
                .context
                .publish_with_headers(
                    part.subject.clone(),
                    part.headers.clone(),
                    part.payload.clone(),
                )
                .await;
            let acknowledged = match sent {
                Ok(ack) => ack.await.map(drop),
                Err(error) => Err(error),
            };
            let Err(error) = acknowledged else {
                return Ok(());
            };
            match refusal(&error) {
                Refusal::Full => backoff.while_refused().await,
                Refusal::Transient if backoff.after_failure().await => {}
                Refusal::Transient | Refusal::Permanent => {
                    return Err(NatsError::Publish {
                        partition: message.partition().clone(),
                        position: message.position(),
                        source: error,
                    });
                }
            }
        }
    }
}

#[async_trait]
impl PartitionWriter for NatsWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        let parts = encode_within(&self.run_id, &message, self.max_message_size)
            .map_err(NatsError::from)?;
        for part in parts {
            self.publish(&message, part).await?;
        }
        Ok(())
    }
}

enum Refusal {
    Full,
    Transient,
    Permanent,
}

// A full stream refuses a message with a store error that names the limit (7.2 節). A message
// that is sent again after a lost PubAck is dropped as a duplicate by its Nats-Msg-Id (5.3 節),
// so a timeout or a broken connection is retried. No responders means that JetStream is not up
// yet, as during a restart of the server.
fn refusal(error: &PublishError) -> Refusal {
    match error.kind() {
        PublishErrorKind::TimedOut
        | PublishErrorKind::BrokenPipe
        | PublishErrorKind::StreamNotFound => Refusal::Transient,
        PublishErrorKind::Other => match std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<jetstream::Error>())
        {
            Some(refused)
                if refused.error_code() == jetstream::ErrorCode::STREAM_STORE_FAILED
                    && refused.to_string().starts_with("maximum") =>
            {
                Refusal::Full
            }
            Some(_) => Refusal::Permanent,
            None => Refusal::Transient,
        },
        _ => Refusal::Permanent,
    }
}
