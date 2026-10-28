use chrono::TimeDelta;

use crate::{EventTime, PartitionMessage, Position};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionProgress {
    pub next: Position,
    pub committed: Position,
    pub ended: bool,
    pub end_committed: bool,
}

impl PartitionProgress {
    pub fn starting_at(position: Position) -> Self {
        Self {
            next: position,
            committed: position,
            ended: false,
            end_committed: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckTiming {
    Now,
    AfterNextCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Accept {
        skip_records: u64,
        next: Position,
        ends_partition: bool,
    },
    Duplicate {
        ack: AckTiming,
    },
    Gap {
        expected: Position,
        received: Position,
    },
}

pub fn admit(
    message: &PartitionMessage,
    progress: &PartitionProgress,
    file_count: u64,
) -> Admission {
    match message {
        PartitionMessage::FileChunk {
            position,
            record_count,
            file,
            ..
        } => {
            let end_offset = position.record_offset + u64::from(*record_count);
            let after = if end_offset >= file.record_count {
                position.next_file()
            } else {
                Position::new(position.file_seq, end_offset)
            };
            if let Some(duplicate) = duplicate(after, progress) {
                return duplicate;
            }
            if *position <= progress.next && position.file_seq == progress.next.file_seq {
                Admission::Accept {
                    skip_records: progress.next.record_offset - position.record_offset,
                    next: after,
                    ends_partition: false,
                }
            } else {
                gap(progress, *position)
            }
        }
        PartitionMessage::FileError { position, .. } => {
            let start = Position::new(position.file_seq, 0);
            if let Some(duplicate) = duplicate(start.next_file(), progress) {
                return duplicate;
            }
            if start == progress.next {
                Admission::Accept {
                    skip_records: 0,
                    next: start.next_file(),
                    ends_partition: false,
                }
            } else {
                gap(progress, start)
            }
        }
        PartitionMessage::EndOfPartition { position, .. } => {
            let last = Position::new(file_count, 0);
            if progress.ended && *position == last {
                let ack = if progress.end_committed {
                    AckTiming::Now
                } else {
                    AckTiming::AfterNextCommit
                };
                return Admission::Duplicate { ack };
            }
            if *position == last && progress.next == last && !progress.ended {
                Admission::Accept {
                    skip_records: 0,
                    next: last,
                    ends_partition: true,
                }
            } else {
                gap(progress, *position)
            }
        }
    }
}

fn duplicate(after: Position, progress: &PartitionProgress) -> Option<Admission> {
    if after > progress.next {
        return None;
    }
    let ack = if after <= progress.committed {
        AckTiming::Now
    } else {
        AckTiming::AfterNextCommit
    };
    Some(Admission::Duplicate { ack })
}

fn gap(progress: &PartitionProgress, received: Position) -> Admission {
    Admission::Gap {
        expected: progress.next,
        received,
    }
}

pub fn file_watermark(file_time: EventTime, allowed_lateness: TimeDelta) -> EventTime {
    file_time - allowed_lateness
}

pub fn is_late(event_time: EventTime, watermark: EventTime) -> bool {
    event_time < watermark
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionWatermark {
    NotStarted,
    At(EventTime),
    Ended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GlobalWatermark {
    At(EventTime),
    Complete,
}

pub fn global_watermark(
    partitions: impl IntoIterator<Item = PartitionWatermark>,
    read_since: EventTime,
    allowed_lateness: TimeDelta,
) -> GlobalWatermark {
    partitions
        .into_iter()
        .filter_map(|partition| match partition {
            PartitionWatermark::NotStarted => Some(read_since - allowed_lateness),
            PartitionWatermark::At(watermark) => Some(watermark),
            PartitionWatermark::Ended => None,
        })
        .min()
        .map_or(GlobalWatermark::Complete, GlobalWatermark::At)
}

// A partition whose watermark is max_skew or more ahead of the global watermark is not read, so
// that pending holds events of about max_skew + allowed_lateness + batch (9 節). The partition
// that holds the global watermark is never max_skew ahead of it, so while some partition has not
// ended, at least one partition stays readable and the run cannot stall.
pub fn readable_partitions(
    partitions: &[PartitionWatermark],
    read_since: EventTime,
    allowed_lateness: TimeDelta,
    max_skew: Option<TimeDelta>,
) -> Vec<bool> {
    let global = global_watermark(partitions.iter().copied(), read_since, allowed_lateness);
    partitions
        .iter()
        .map(|partition| match (*partition, global, max_skew) {
            (PartitionWatermark::Ended, _, _) => false,
            (PartitionWatermark::At(watermark), GlobalWatermark::At(global), Some(max_skew)) => {
                watermark - global < max_skew
            }
            _ => true,
        })
        .collect()
}

pub fn batch_start(event_time: EventTime, batch: TimeDelta) -> EventTime {
    let width = batch
        .num_microseconds()
        .filter(|width| *width > 0)
        .expect("the batch length is a positive number of microseconds");
    let micros = event_time.timestamp_micros();
    EventTime::from_timestamp_micros(micros - micros.rem_euclid(width))
        .expect("the start of a batch is not later than a representable event time")
}

// Every event accepted from now on is at or after the global watermark, because it is not
// before the watermark of its file, which is not before the global watermark. A batch that
// ends at or before the global watermark therefore receives no more events.
pub fn is_batch_complete(batch_end: EventTime, global: GlobalWatermark) -> bool {
    match global {
        GlobalWatermark::At(watermark) => batch_end <= watermark,
        GlobalWatermark::Complete => true,
    }
}

pub fn recent_event_expiry(
    partition_watermark: EventTime,
    allowed_lateness: TimeDelta,
) -> EventTime {
    partition_watermark - allowed_lateness * 2
}
