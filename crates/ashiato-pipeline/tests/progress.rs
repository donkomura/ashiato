use std::sync::Arc;

use arrow_array::RecordBatch;
use ashiato_pipeline::{
    AckTiming, Admission, GlobalWatermark, IngestedFile, PartitionId, PartitionMessage,
    PartitionProgress, PartitionWatermark, Position, admit, batch_start, events_schema,
    file_watermark, global_watermark, is_batch_complete, is_late, readable_partitions,
    recent_event_expiry,
};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};

const FILE_COUNT: u64 = 3;
const RECORDS_PER_FILE: u64 = 10;

fn time(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, hour, minute, 0).unwrap()
}

fn partition() -> PartitionId {
    PartitionId::new("111111111111", "us-east-1")
}

fn file(record_count: u64) -> IngestedFile {
    IngestedFile {
        uri: "/logs/a.json.gz".to_owned(),
        version_id: None,
        etag: None,
        size: 1,
        sha256: String::new(),
        file_time: time(0, 0),
        record_count,
    }
}

fn chunk_of(file_seq: u64, offset: u64, count: u32, record_count: u64) -> PartitionMessage {
    PartitionMessage::FileChunk {
        partition: partition(),
        position: Position::new(file_seq, offset),
        record_count: count,
        file: file(record_count),
        events: RecordBatch::new_empty(Arc::clone(&events_schema())),
        watermark: time(0, 0),
    }
}

fn chunk(file_seq: u64, offset: u64, count: u32) -> PartitionMessage {
    chunk_of(file_seq, offset, count, RECORDS_PER_FILE)
}

fn file_error(file_seq: u64) -> PartitionMessage {
    PartitionMessage::FileError {
        partition: partition(),
        position: Position::new(file_seq, 0),
        file: file(0),
        reason: "invalid gzip header".to_owned(),
        watermark: time(0, 0),
    }
}

fn end(file_seq: u64) -> PartitionMessage {
    PartitionMessage::EndOfPartition {
        partition: partition(),
        position: Position::new(file_seq, 0),
    }
}

fn progress(next: (u64, u64), committed: (u64, u64)) -> PartitionProgress {
    PartitionProgress {
        next: Position::new(next.0, next.1),
        committed: Position::new(committed.0, committed.1),
        ended: false,
        end_committed: false,
    }
}

fn accept(skip_records: u64, next: (u64, u64)) -> Admission {
    Admission::Accept {
        skip_records,
        next: Position::new(next.0, next.1),
        ends_partition: false,
    }
}

fn gap(expected: (u64, u64), received: (u64, u64)) -> Admission {
    Admission::Gap {
        expected: Position::new(expected.0, expected.1),
        received: Position::new(received.0, received.1),
    }
}

#[test]
fn admission_of_messages_compared_with_the_next_and_committed_positions() {
    let cases = [
        (
            "all records are before the committed position",
            chunk(0, 0, 10),
            progress((1, 5), (1, 5)),
            Admission::Duplicate {
                ack: AckTiming::Now,
            },
        ),
        (
            "the records end exactly at the committed position",
            chunk(1, 0, 5),
            progress((1, 5), (1, 5)),
            Admission::Duplicate {
                ack: AckTiming::Now,
            },
        ),
        (
            "all records are before the next position but after the committed position",
            chunk(1, 0, 5),
            progress((1, 5), (0, 0)),
            Admission::Duplicate {
                ack: AckTiming::AfterNextCommit,
            },
        ),
        (
            "the message starts before the next position and ends after it",
            chunk(1, 3, 5),
            progress((1, 5), (1, 0)),
            accept(2, (1, 8)),
        ),
        (
            "the message starts at the next position",
            chunk(1, 5, 2),
            progress((1, 5), (1, 0)),
            accept(0, (1, 7)),
        ),
        (
            "the message reaches the end of the file",
            chunk(1, 5, 5),
            progress((1, 5), (1, 0)),
            accept(0, (2, 0)),
        ),
        (
            "a chunk of an empty file moves on to the next file",
            chunk_of(1, 0, 0, 0),
            progress((1, 0), (1, 0)),
            accept(0, (2, 0)),
        ),
        (
            "a chunk of an empty file that was already received",
            chunk_of(1, 0, 0, 0),
            progress((2, 0), (2, 0)),
            Admission::Duplicate {
                ack: AckTiming::Now,
            },
        ),
        (
            "the message starts after the next position in the same file",
            chunk(1, 7, 2),
            progress((1, 5), (1, 0)),
            gap((1, 5), (1, 7)),
        ),
        (
            "the message starts in a later file",
            chunk(2, 0, 10),
            progress((1, 5), (1, 0)),
            gap((1, 5), (2, 0)),
        ),
        (
            "a file error at the start of the next file",
            file_error(1),
            progress((1, 0), (1, 0)),
            accept(0, (2, 0)),
        ),
        (
            "a file error in the middle of a file",
            file_error(1),
            progress((1, 5), (1, 0)),
            gap((1, 5), (1, 0)),
        ),
        (
            "a file error that was already received",
            file_error(0),
            progress((1, 5), (1, 0)),
            Admission::Duplicate {
                ack: AckTiming::Now,
            },
        ),
        (
            "a file error for a later file",
            file_error(2),
            progress((1, 0), (1, 0)),
            gap((1, 0), (2, 0)),
        ),
        (
            "the end of the partition before the last file has been received",
            end(FILE_COUNT),
            progress((2, 0), (2, 0)),
            gap((2, 0), (3, 0)),
        ),
        (
            "the end of the partition at a position other than after the last file",
            end(FILE_COUNT - 1),
            progress((2, 0), (2, 0)),
            gap((2, 0), (2, 0)),
        ),
    ];
    for (name, message, progress, expected) in cases {
        assert_eq!(admit(&message, &progress, FILE_COUNT), expected, "{name}");
    }
}

#[test]
fn the_end_of_the_partition_after_the_last_file_ends_the_partition() {
    assert_eq!(
        admit(&end(FILE_COUNT), &progress((3, 0), (2, 4)), FILE_COUNT),
        Admission::Accept {
            skip_records: 0,
            next: Position::new(3, 0),
            ends_partition: true,
        }
    );
}

#[test]
fn an_empty_partition_ends_at_its_first_position() {
    assert_eq!(
        admit(&end(0), &progress((0, 0), (0, 0)), 0),
        Admission::Accept {
            skip_records: 0,
            next: Position::new(0, 0),
            ends_partition: true,
        }
    );
}

#[test]
fn a_repeated_end_of_the_partition_is_a_duplicate_acknowledged_once_the_end_is_committed() {
    let ended = PartitionProgress {
        ended: true,
        ..progress((3, 0), (3, 0))
    };
    assert_eq!(
        admit(&end(FILE_COUNT), &ended, FILE_COUNT),
        Admission::Duplicate {
            ack: AckTiming::AfterNextCommit,
        }
    );
    let committed = PartitionProgress {
        end_committed: true,
        ..ended
    };
    assert_eq!(
        admit(&end(FILE_COUNT), &committed, FILE_COUNT),
        Admission::Duplicate {
            ack: AckTiming::Now,
        }
    );
}

#[test]
fn a_partition_starts_with_nothing_received_after_its_resume_position() {
    assert_eq!(
        PartitionProgress::starting_at(Position::new(2, 3)),
        progress((2, 3), (2, 3))
    );
}

#[test]
fn the_watermark_of_a_file_allows_for_late_delivery() {
    assert_eq!(
        file_watermark(time(1, 0), TimeDelta::minutes(30)),
        time(0, 30)
    );
}

#[test]
fn only_events_before_the_watermark_are_late() {
    assert!(is_late(time(0, 29), time(0, 30)));
    assert!(!is_late(time(0, 30), time(0, 30)));
    assert!(!is_late(time(0, 31), time(0, 30)));
}

#[test]
fn the_global_watermark_is_the_minimum_over_the_partitions_that_have_not_ended() {
    let lateness = TimeDelta::minutes(30);
    let read_since = time(0, 0);
    assert_eq!(
        global_watermark(
            [
                PartitionWatermark::At(time(5, 0)),
                PartitionWatermark::Ended,
                PartitionWatermark::At(time(3, 0)),
            ],
            read_since,
            lateness,
        ),
        GlobalWatermark::At(time(3, 0))
    );
    assert_eq!(
        global_watermark(
            [
                PartitionWatermark::At(time(5, 0)),
                PartitionWatermark::NotStarted,
            ],
            time(1, 0),
            lateness,
        ),
        GlobalWatermark::At(time(0, 30))
    );
    assert_eq!(
        global_watermark(
            [PartitionWatermark::Ended, PartitionWatermark::Ended],
            read_since,
            lateness,
        ),
        GlobalWatermark::Complete
    );
    assert_eq!(
        global_watermark([], read_since, lateness),
        GlobalWatermark::Complete
    );
}

#[test]
fn partitions_ahead_of_the_global_watermark_by_max_skew_or_more_are_not_read() {
    use PartitionWatermark::{At, Ended, NotStarted};
    let lateness = TimeDelta::minutes(30);
    let read_since = time(1, 0);
    let skew = Some(TimeDelta::hours(2));
    let cases = [
        (
            "without max_skew every partition that has not ended is read",
            vec![At(time(1, 0)), At(time(20, 0)), NotStarted, Ended],
            None,
            vec![true, true, true, false],
        ),
        (
            "the partition that holds the global watermark is read",
            vec![At(time(3, 0)), At(time(4, 0))],
            skew,
            vec![true, true],
        ),
        (
            "a partition just under max_skew ahead is read",
            vec![At(time(3, 0)), At(time(4, 59))],
            skew,
            vec![true, true],
        ),
        (
            "a partition exactly max_skew ahead is not read",
            vec![At(time(3, 0)), At(time(5, 0))],
            skew,
            vec![true, false],
        ),
        (
            "a partition more than max_skew ahead is not read",
            vec![At(time(3, 0)), At(time(9, 0))],
            skew,
            vec![true, false],
        ),
        (
            "a partition that has received nothing holds the global watermark and is read",
            vec![NotStarted, At(time(2, 29)), At(time(2, 30))],
            skew,
            vec![true, true, false],
        ),
        (
            "an ended partition is not read and does not hold the others back",
            vec![Ended, At(time(9, 0)), At(time(10, 0))],
            skew,
            vec![false, true, true],
        ),
        (
            "nothing is read once every partition has ended",
            vec![Ended, Ended],
            skew,
            vec![false, false],
        ),
        ("no partitions", vec![], skew, vec![]),
    ];
    for (name, partitions, max_skew, expected) in cases {
        assert_eq!(
            readable_partitions(&partitions, read_since, lateness, max_skew),
            expected,
            "{name}"
        );
    }
}

// The analyzer waits only on the partitions it may read, so a set of partitions that has not
// ended must always leave one of them readable; otherwise the run stops for good (9 節).
#[test]
fn some_partition_is_readable_while_any_partition_has_not_ended() {
    let lateness = TimeDelta::minutes(30);
    let read_since = time(1, 0);
    let states: Vec<PartitionWatermark> =
        [PartitionWatermark::NotStarted, PartitionWatermark::Ended]
            .into_iter()
            .chain(
                (0..8).map(|step| {
                    PartitionWatermark::At(time(0, 30) + TimeDelta::minutes(25 * step))
                }),
            )
            .collect();
    for max_skew in [
        TimeDelta::minutes(1),
        TimeDelta::minutes(50),
        TimeDelta::hours(3),
    ] {
        for first in &states {
            for second in &states {
                for third in &states {
                    let partitions = [*first, *second, *third];
                    let readable =
                        readable_partitions(&partitions, read_since, lateness, Some(max_skew));
                    let open = partitions
                        .iter()
                        .any(|partition| *partition != PartitionWatermark::Ended);
                    assert_eq!(
                        readable.iter().any(|readable| *readable),
                        open,
                        "{partitions:?} with max_skew {max_skew}"
                    );
                }
            }
        }
    }
}

#[test]
fn batches_are_aligned_to_the_unix_epoch() {
    let hour = TimeDelta::hours(1);
    assert_eq!(batch_start(time(5, 59), hour), time(5, 0));
    assert_eq!(batch_start(time(5, 0), hour), time(5, 0));
    let seven_hours = TimeDelta::hours(7);
    let epoch = DateTime::UNIX_EPOCH;
    assert_eq!(
        batch_start(epoch + TimeDelta::hours(15), seven_hours),
        epoch + TimeDelta::hours(14)
    );
}

#[test]
fn a_batch_is_complete_once_the_global_watermark_reaches_its_end() {
    assert!(!is_batch_complete(
        time(2, 0),
        GlobalWatermark::At(time(1, 59))
    ));
    assert!(is_batch_complete(
        time(2, 0),
        GlobalWatermark::At(time(2, 0))
    ));
    assert!(is_batch_complete(
        time(2, 0),
        GlobalWatermark::At(time(2, 1))
    ));
    assert!(is_batch_complete(time(2, 0), GlobalWatermark::Complete));
}

#[test]
fn recent_event_ids_expire_twice_the_allowed_lateness_before_the_partition_watermark() {
    assert_eq!(
        recent_event_expiry(time(3, 0), TimeDelta::minutes(30)),
        time(2, 0)
    );
}
