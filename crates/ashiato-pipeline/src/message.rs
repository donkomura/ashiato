use std::fmt;

use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type EventTime = DateTime<Utc>;

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
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
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
        todo!()
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
        todo!()
    }

    pub fn position(&self) -> Position {
        todo!()
    }

    pub fn end_position(&self) -> Position {
        todo!()
    }
}

pub fn file_id(_partition_index: u32, _file_seq: u64) -> i64 {
    todo!()
}
