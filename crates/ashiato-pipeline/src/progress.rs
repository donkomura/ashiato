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
    _message: &PartitionMessage,
    _progress: &PartitionProgress,
    _file_count: u64,
) -> Admission {
    todo!()
}

pub fn file_watermark(_file_time: EventTime, _allowed_lateness: TimeDelta) -> EventTime {
    todo!()
}

pub fn is_late(_event_time: EventTime, _watermark: EventTime) -> bool {
    todo!()
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
    _partitions: impl IntoIterator<Item = PartitionWatermark>,
    _read_since: EventTime,
    _allowed_lateness: TimeDelta,
) -> GlobalWatermark {
    todo!()
}

pub fn batch_start(_event_time: EventTime, _batch: TimeDelta) -> EventTime {
    todo!()
}

pub fn is_batch_complete(_batch_end: EventTime, _global: GlobalWatermark) -> bool {
    todo!()
}

pub fn recent_event_expiry(
    _partition_watermark: EventTime,
    _allowed_lateness: TimeDelta,
) -> EventTime {
    todo!()
}
