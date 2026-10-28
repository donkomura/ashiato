use std::time::Duration;

use ashiato_nats::{
    CapacityError, check_capacity, consumer_config, max_ack_pending, stream_config,
};
use ashiato_pipeline::{BrokerSettings, PartitionId};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::{DiscardPolicy, RetentionPolicy, StorageType};

const RUN_ID: &str = "case-1";

fn broker() -> BrokerSettings {
    BrokerSettings {
        max_bytes: 50 << 30,
        max_messages_per_partition: 64,
        max_message_size: 1 << 20,
        duplicate_window: "10m".parse().unwrap(),
        ack_wait: "15m".parse().unwrap(),
    }
}

#[test]
fn the_stream_refuses_new_messages_of_a_full_partition_and_removes_acknowledged_ones() {
    let config = stream_config(RUN_ID, &broker());

    assert_eq!(config.name, "ASHIATO_case-1");
    assert_eq!(config.subjects, vec!["ashiato.case-1.events.>".to_owned()]);
    assert_eq!(config.retention, RetentionPolicy::WorkQueue);
    assert_eq!(config.storage, StorageType::File);
    assert_eq!(config.max_bytes, 50 << 30);
    assert_eq!(config.max_messages_per_subject, 64);
    assert_eq!(config.discard, DiscardPolicy::New);
    assert!(config.discard_new_per_subject);
    assert_eq!(config.max_message_size, 1 << 20);
    assert_eq!(config.duplicate_window, Duration::from_secs(600));
}

#[test]
fn each_partition_has_a_durable_pull_consumer_of_its_own_subject() {
    let partition = PartitionId::new("111111111111", "us-east-1");
    let config = consumer_config(RUN_ID, &partition, &broker(), 80);

    assert_eq!(
        config.durable_name.as_deref(),
        Some("analyzer-111111111111-us-east-1")
    );
    assert_eq!(
        config.filter_subject,
        "ashiato.case-1.events.111111111111.us-east-1"
    );
    assert_eq!(config.ack_policy, AckPolicy::Explicit);
    assert_eq!(config.deliver_policy, DeliverPolicy::All);
    assert_eq!(config.ack_wait, Duration::from_secs(900));
    assert_eq!(config.max_ack_pending, 80);
}

#[test]
fn the_messages_held_before_a_commit_stay_under_the_limit_of_pending_acknowledgements() {
    for (commit_every, fetch_max) in [(1, 1), (64, 16), (64, 1), (1000, 64)] {
        let limit = max_ack_pending(commit_every, fetch_max);
        // The analyzer holds the messages since the last commit and one more fetch before it
        // commits, all without acknowledging them.
        assert!(
            limit >= i64::try_from(commit_every + fetch_max).unwrap(),
            "{commit_every} {fetch_max} {limit}"
        );
    }
}

#[test]
fn max_bytes_must_hold_every_partition_full() {
    let mib = 1_u64 << 20;
    let cases: [(usize, u64, u32, u64, bool); 7] = [
        (1, 64 * mib, 64, mib, true),
        (1, 64 * mib - 1, 64, mib, false),
        (24, 24 * 64 * mib, 64, mib, true),
        (24, 23 * 64 * mib, 64, mib, false),
        (0, 0, 64, mib, true),
        (3, 3 * 10 * 100, 10, 100, true),
        (u32::MAX as usize, u64::MAX, u32::MAX, u64::MAX, false),
    ];
    for (partitions, max_bytes, per_partition, message_size, ok) in cases {
        let settings = BrokerSettings {
            max_bytes,
            max_messages_per_partition: per_partition,
            max_message_size: message_size,
            ..broker()
        };
        let result = check_capacity(partitions, &settings);
        assert_eq!(result.is_ok(), ok, "{partitions} {max_bytes} {result:?}");
        if let Err(CapacityError {
            max_bytes: reported,
            required,
            partitions: reported_partitions,
        }) = result
        {
            assert_eq!(reported, max_bytes);
            assert_eq!(reported_partitions, partitions);
            assert_eq!(
                required,
                partitions as u128 * u128::from(per_partition) * u128::from(message_size)
            );
        }
    }
}
