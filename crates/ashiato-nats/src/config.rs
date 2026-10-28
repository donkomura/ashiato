use ashiato_pipeline::{BrokerSettings, PartitionId};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::{DiscardPolicy, RetentionPolicy, StorageType};
use async_nats::jetstream::{consumer, stream};

use crate::codec::{consumer_name, events_subject, events_subjects, stream_name};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "broker.max_bytes {max_bytes} is less than the {required} bytes that {partitions} partitions \
     may hold with max_messages_per_partition and max_message_size; the stream could fill up with \
     the partitions that the analyzer does not read and stop the run (7.2 節)"
)]
pub struct CapacityError {
    pub max_bytes: u64,
    pub required: u128,
    pub partitions: usize,
}

pub fn check_capacity(partitions: usize, broker: &BrokerSettings) -> Result<(), CapacityError> {
    let required = partitions as u128
        * u128::from(broker.max_messages_per_partition)
        * u128::from(broker.max_message_size);
    if u128::from(broker.max_bytes) >= required {
        Ok(())
    } else {
        Err(CapacityError {
            max_bytes: broker.max_bytes,
            required,
            partitions,
        })
    }
}

// The analyzer holds the messages since its last commit, and the messages of one more fetch,
// without acknowledging them (8.2 節). A lower limit would stop the deliveries before the commit
// that acknowledges them.
pub fn max_ack_pending(commit_every: usize, fetch_max: usize) -> i64 {
    i64::try_from(commit_every + fetch_max).unwrap_or(i64::MAX)
}

pub fn stream_config(run_id: &str, broker: &BrokerSettings) -> stream::Config {
    stream::Config {
        name: stream_name(run_id),
        subjects: vec![events_subjects(run_id)],
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        max_bytes: i64::try_from(broker.max_bytes).unwrap_or(i64::MAX),
        max_messages_per_subject: i64::from(broker.max_messages_per_partition),
        discard: DiscardPolicy::New,
        discard_new_per_subject: true,
        max_message_size: i32::try_from(broker.max_message_size).unwrap_or(i32::MAX),
        duplicate_window: std_duration(broker.duplicate_window),
        ..Default::default()
    }
}

pub fn consumer_config(
    run_id: &str,
    partition: &PartitionId,
    broker: &BrokerSettings,
    max_ack_pending: i64,
) -> consumer::pull::Config {
    consumer::pull::Config {
        durable_name: Some(consumer_name(partition)),
        filter_subject: events_subject(run_id, partition),
        ack_policy: AckPolicy::Explicit,
        deliver_policy: DeliverPolicy::All,
        ack_wait: std_duration(broker.ack_wait),
        max_ack_pending,
        ..Default::default()
    }
}

fn std_duration(duration: ashiato_core::Duration) -> std::time::Duration {
    duration
        .to_time_delta()
        .to_std()
        .expect("a duration of the run plan is positive")
}
