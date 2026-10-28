use ashiato_pipeline::{BrokerSettings, PartitionId};
use async_nats::jetstream::{consumer, stream};

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
    todo!("{partitions}{broker:?}")
}

pub fn max_ack_pending(commit_every: usize, fetch_max: usize) -> i64 {
    todo!("{commit_every}{fetch_max}")
}

pub fn stream_config(run_id: &str, broker: &BrokerSettings) -> stream::Config {
    todo!("{run_id}{broker:?}")
}

pub fn consumer_config(
    run_id: &str,
    partition: &PartitionId,
    broker: &BrokerSettings,
    max_ack_pending: i64,
) -> consumer::pull::Config {
    todo!("{run_id}{partition}{broker:?}{max_ack_pending}")
}
