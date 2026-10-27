use std::fmt;

use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type EventTime = DateTime<Utc>;

const FILE_SEQ_BITS: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartitionId {
    pub account: String,
    pub region: String,
}

impl PartitionId {
    pub fn new(account: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            region: region.into(),
        }
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.account, self.region)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position {
    pub file_seq: u64,
    pub record_offset: u64,
}

impl Position {
    pub const START: Position = Position {
        file_seq: 0,
        record_offset: 0,
    };

    pub fn new(file_seq: u64, record_offset: u64) -> Self {
        Self {
            file_seq,
            record_offset,
        }
    }

    pub fn next_file(self) -> Self {
        Self::new(self.file_seq + 1, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestedFile {
    pub uri: String,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub sha256: String,
    pub file_time: EventTime,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionMessage {
    FileChunk {
        partition: PartitionId,
        position: Position,
        record_count: u32,
        file: IngestedFile,
        events: RecordBatch,
        watermark: EventTime,
    },
    FileError {
        partition: PartitionId,
        position: Position,
        file: IngestedFile,
        reason: String,
        watermark: EventTime,
    },
    EndOfPartition {
        partition: PartitionId,
        position: Position,
    },
}

impl PartitionMessage {
    pub fn partition(&self) -> &PartitionId {
        match self {
            PartitionMessage::FileChunk { partition, .. }
            | PartitionMessage::FileError { partition, .. }
            | PartitionMessage::EndOfPartition { partition, .. } => partition,
        }
    }

    pub fn position(&self) -> Position {
        match self {
            PartitionMessage::FileChunk { position, .. }
            | PartitionMessage::FileError { position, .. }
            | PartitionMessage::EndOfPartition { position, .. } => *position,
        }
    }

    pub fn end_position(&self) -> Position {
        match self {
            PartitionMessage::FileChunk {
                position,
                record_count,
                ..
            } => Position::new(
                position.file_seq,
                position.record_offset + u64::from(*record_count),
            ),
            PartitionMessage::FileError { position, .. } => position.next_file(),
            PartitionMessage::EndOfPartition { position, .. } => *position,
        }
    }
}

// file_id is derived from positions fixed by the saved manifest rather than from the order in
// which files arrive, so that the same input gives the same file_id regardless of the placement
// and the speed of the workers. The upper bits hold the partition and the lower bits the file.
pub fn file_id(partition_index: u32, file_seq: u64) -> i64 {
    assert!(
        file_seq < 1 << FILE_SEQ_BITS,
        "a partition holds fewer than 2^{FILE_SEQ_BITS} files"
    );
    let id = (u64::from(partition_index) << FILE_SEQ_BITS) | file_seq;
    i64::try_from(id).expect("partition_index takes at most 31 bits above the file sequence")
}
