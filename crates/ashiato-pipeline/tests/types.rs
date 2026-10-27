use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, TimeUnit};
use ashiato_pipeline::{
    EVENT_COLUMNS, IngestedFile, PartitionId, PartitionMessage, Position, events_schema, file_id,
};
use chrono::{TimeZone, Utc};

fn ingested_file(record_count: u64) -> IngestedFile {
    IngestedFile {
        uri: "file:///logs/111111111111_CloudTrail_us-east-1_20261001T0000Z_a.json.gz".to_owned(),
        version_id: None,
        etag: None,
        size: 100,
        sha256: "00".repeat(32),
        file_time: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
        record_count,
    }
}

#[test]
fn positions_are_ordered_by_file_then_record() {
    let mut positions = vec![
        Position::new(1, 0),
        Position::new(0, 5),
        Position::new(0, 0),
        Position::new(1, 3),
    ];
    positions.sort();
    assert_eq!(
        positions,
        vec![
            Position::new(0, 0),
            Position::new(0, 5),
            Position::new(1, 0),
            Position::new(1, 3),
        ]
    );
    assert_eq!(Position::START, Position::new(0, 0));
}

#[test]
fn next_file_is_the_start_of_the_following_file() {
    assert_eq!(Position::new(3, 42).next_file(), Position::new(4, 0));
}

#[test]
fn partitions_are_ordered_by_account_then_region() {
    let mut partitions = vec![
        PartitionId::new("222222222222", "ap-northeast-1"),
        PartitionId::new("111111111111", "us-east-1"),
        PartitionId::new("111111111111", "ap-northeast-1"),
    ];
    partitions.sort();
    assert_eq!(
        partitions,
        vec![
            PartitionId::new("111111111111", "ap-northeast-1"),
            PartitionId::new("111111111111", "us-east-1"),
            PartitionId::new("222222222222", "ap-northeast-1"),
        ]
    );
    assert_eq!(
        PartitionId::new("111111111111", "us-east-1").to_string(),
        "111111111111/us-east-1"
    );
}

#[test]
fn file_ids_differ_across_partitions_and_files() {
    let ids: BTreeSet<i64> = (0..3)
        .flat_map(|partition| (0..1000).map(move |seq| file_id(partition, seq)))
        .collect();
    assert_eq!(ids.len(), 3000);
    assert!(file_id(0, 1) < file_id(0, 2));
    assert!(file_id(0, u64::from(u32::MAX)) < file_id(1, 0));
    assert!(ids.iter().all(|id| *id >= 0));
}

#[test]
fn events_schema_has_the_normalized_columns_and_the_origin_of_each_event() {
    let schema = events_schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, EVENT_COLUMNS);
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    for (name, data_type) in [
        ("event_id", DataType::Utf8),
        ("event_time", timestamp.clone()),
        ("read_only", DataType::Boolean),
        ("session_created_at", timestamp),
        ("mfa_authenticated", DataType::Boolean),
        ("credential_from_console", DataType::Boolean),
        ("request_parameters", DataType::Utf8),
        ("file_id", DataType::Int64),
        ("record_index", DataType::Int64),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &data_type,
            "{name}"
        );
    }
    for name in ["event_id", "event_time", "file_id", "record_index"] {
        assert!(
            !schema.field_with_name(name).unwrap().is_nullable(),
            "{name}"
        );
    }
}

#[test]
fn messages_report_their_partition_and_the_range_of_records() {
    let partition = PartitionId::new("111111111111", "us-east-1");
    let watermark = Utc.with_ymd_and_hms(2026, 9, 30, 23, 30, 0).unwrap();
    let chunk = PartitionMessage::FileChunk {
        partition: partition.clone(),
        position: Position::new(2, 10),
        record_count: 5,
        file: ingested_file(20),
        events: RecordBatch::new_empty(Arc::clone(&events_schema())),
        watermark,
    };
    assert_eq!(chunk.partition(), &partition);
    assert_eq!(chunk.position(), Position::new(2, 10));
    assert_eq!(chunk.end_position(), Position::new(2, 15));

    let error = PartitionMessage::FileError {
        partition: partition.clone(),
        position: Position::new(3, 0),
        file: ingested_file(0),
        reason: "invalid gzip header".to_owned(),
        watermark,
    };
    assert_eq!(error.position(), Position::new(3, 0));
    assert_eq!(error.end_position(), Position::new(4, 0));

    let end = PartitionMessage::EndOfPartition {
        partition: partition.clone(),
        position: Position::new(4, 0),
    };
    assert_eq!(end.partition(), &partition);
    assert_eq!(end.position(), Position::new(4, 0));
    assert_eq!(end.end_position(), Position::new(4, 0));
}

#[test]
fn ingested_file_round_trips_through_json() {
    let file = IngestedFile {
        version_id: Some("3HL4kqtJlcpXroDTDmJ".to_owned()),
        etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned()),
        ..ingested_file(7)
    };
    let json = serde_json::to_string(&file).unwrap();
    assert_eq!(serde_json::from_str::<IngestedFile>(&json).unwrap(), file);
}
