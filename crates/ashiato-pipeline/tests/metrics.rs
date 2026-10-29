use std::sync::Arc;
use std::time::Duration;

use ashiato_pipeline::{
    AnalyzerCounts, AnalyzerMetrics, Moment, PartitionId, PartitionMessage, PartitionReader,
    PartitionWriter, Position, SendTimes, WorkerMetrics, partition_channels, unix_ms,
};
use chrono::{TimeZone, Utc};
use serde_json::json;

fn partition(account: &str) -> PartitionId {
    PartitionId::new(account, "us-east-1")
}

fn end(partition: &PartitionId, file_seq: u64) -> PartitionMessage {
    PartitionMessage::EndOfPartition {
        partition: partition.clone(),
        position: Position::new(file_seq, 0),
    }
}

#[test]
fn the_analyzer_line_has_the_counts_the_times_in_whole_milliseconds_and_the_phases() {
    let metrics = AnalyzerMetrics::default();
    metrics.set_counts(AnalyzerCounts {
        manifest_files: 1234,
        ingested_files: 321,
        global_watermark: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
        pending_rows: 100,
        pending_max_rows: 200,
    });
    metrics.fetch_wait.add(Duration::from_micros(1600));
    metrics.fetch_wait.add(Duration::from_micros(1600));
    metrics.commit.add(Duration::from_millis(20));
    metrics.batch.add(Duration::from_micros(30_999));
    metrics.first_plan_response.mark_at(1_730_000_000_000);
    metrics.last_end_committed.mark_at(1_730_000_000_500);

    assert_eq!(
        metrics.line(1_730_000_001_000, false),
        json!({
            "metrics": "analyzer",
            "unix_ms": 1_730_000_001_000_u64,
            "final": false,
            "manifest_files": 1234,
            "ingested_files": 321,
            "global_watermark": "2026-01-01T00:00:00Z",
            "pending_rows": 100,
            "pending_max_rows": 200,
            "time_ms": { "fetch_wait": 3, "commit": 20, "batch": 30 },
            "phases_unix_ms": {
                "first_plan_response": 1_730_000_000_000_u64,
                "last_end_committed": 1_730_000_000_500_u64,
                "correlated": null,
            },
        })
    );
}

#[test]
fn the_analyzer_line_before_anything_is_known_has_nulls_and_zeros() {
    let metrics = AnalyzerMetrics::default();

    assert_eq!(
        metrics.line(5, true),
        json!({
            "metrics": "analyzer",
            "unix_ms": 5,
            "final": true,
            "manifest_files": 0,
            "ingested_files": 0,
            "global_watermark": null,
            "pending_rows": 0,
            "pending_max_rows": 0,
            "time_ms": { "fetch_wait": 0, "commit": 0, "batch": 0 },
            "phases_unix_ms": {
                "first_plan_response": null,
                "last_end_committed": null,
                "correlated": null,
            },
        })
    );
}

#[test]
fn the_counts_set_last_are_read_back() {
    let metrics = AnalyzerMetrics::default();
    let counts = AnalyzerCounts {
        manifest_files: 3,
        ingested_files: 2,
        global_watermark: None,
        pending_rows: 1,
        pending_max_rows: 4,
    };

    metrics.set_counts(AnalyzerCounts {
        pending_rows: 9,
        ..counts
    });
    metrics.set_counts(counts);

    assert_eq!(metrics.counts(), counts);
}

#[test]
fn a_moment_keeps_its_first_mark() {
    let moment = Moment::default();
    assert_eq!(moment.get(), None);

    moment.mark_at(10);
    moment.mark_at(20);
    let before = unix_ms();
    let marked = Moment::default();
    marked.mark();
    let after = unix_ms();

    assert_eq!(moment.get(), Some(10));
    let marked = marked.get().unwrap();
    assert!(
        before <= marked && marked <= after,
        "{before} {marked} {after}"
    );
    assert!(before > 1_700_000_000_000, "{before}");
}

#[test]
fn the_worker_line_sums_the_files_of_each_partition_and_lists_partitions_without_files() {
    let metrics = WorkerMetrics::new(3);
    let (north, south, idle) = (
        partition("111111111111"),
        partition("222222222222"),
        partition("333333333333"),
    );
    for partition in [&south, &idle, &north] {
        metrics.add_partition(partition);
    }
    metrics.record_file(&south, 100, 7);
    metrics.record_file(&north, 10, 1);
    metrics.record_file(&south, 50, 0);
    metrics.fetch_wait.add(Duration::from_millis(10));
    metrics.decode.add(Duration::from_micros(20_500));
    metrics.send.publish_wait.add(Duration::from_millis(30));
    metrics.send.rejected_wait.add(Duration::from_millis(40));

    assert_eq!(
        metrics.line(1_730_000_000_000, true),
        json!({
            "metrics": "worker",
            "worker_id": 3,
            "unix_ms": 1_730_000_000_000_u64,
            "final": true,
            "partitions": [
                { "partition": "111111111111/us-east-1", "files": 1, "bytes": 10, "events": 1 },
                { "partition": "222222222222/us-east-1", "files": 2, "bytes": 150, "events": 7 },
                { "partition": "333333333333/us-east-1", "files": 0, "bytes": 0, "events": 0 },
            ],
            "time_ms": { "fetch_wait": 10, "decode": 20, "publish_wait": 30, "rejected_wait": 40 },
        })
    );
}

#[tokio::test]
async fn a_channel_writer_counts_the_wait_for_a_full_channel_as_rejected() {
    let partition = partition("111111111111");
    let (writer, mut reader) = partition_channels([partition.clone()], 1);
    let times = Arc::new(SendTimes::default());

    writer.put_timed(end(&partition, 0), &times).await.unwrap();
    assert_eq!(times.rejected_wait.get(), Duration::ZERO);
    let waiting = tokio::spawn({
        let (writer, times, partition) = (writer.clone(), Arc::clone(&times), partition.clone());
        async move { writer.put_timed(end(&partition, 1), &times).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    reader.fetch(&partition, 1).await.unwrap();
    waiting.await.unwrap().unwrap();

    assert!(
        times.rejected_wait.get() >= Duration::from_millis(150),
        "{times:?}"
    );
    assert!(
        times.publish_wait.get() < Duration::from_millis(150),
        "{times:?}"
    );
}
