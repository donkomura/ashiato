use ashiato_core::Duration;
use serde::{Deserialize, Serialize};

use crate::{ManifestEntry, PartitionId, Position, RunPlan};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error(
        "placement.mode: distributed needs source.kind: s3, because the workers on other machines \
         cannot read the local directory"
    )]
    LocalSourceInDistributed,
    #[error("placement.mode: distributed needs placement.nats_url")]
    MissingNatsUrl,
    #[error("the run plan is not distributed, so it has no ingest workers to plan for")]
    NotDistributed,
    #[error("worker {worker_id} is not one of the {workers} workers of the run (ids start at 0)")]
    UnknownWorker { worker_id: u32, workers: u32 },
    #[error("partition {0} is not part of this run")]
    UnknownPartition(PartitionId),
    #[error(
        "the manifest entry of file {file_seq} takes {size} bytes and does not fit in a reply of \
         {limit} bytes"
    )]
    EntryTooLarge {
        file_seq: u64,
        size: usize,
        limit: usize,
    },
    #[error("failed to encode the reply")]
    Encode(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerPlan {
    pub run_id: String,
    pub worker_id: u32,
    pub source: WorkerSource,
    #[serde(serialize_with = "serialize_display")]
    pub allowed_lateness: Duration,
    pub max_message_size: u64,
    pub partitions: Vec<AssignedPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSource {
    pub uri: String,
    pub endpoint_url: Option<String>,
}

// index is the place of the partition in the saved manifest, which file_id is derived from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignedPartition {
    pub index: u32,
    pub partition: PartitionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRequest {
    pub partition: PartitionId,
    pub from_file_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestPage {
    pub files: Vec<ManifestEntry>,
    pub next_file_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeRequest {
    pub partitions: Vec<PartitionId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeResponse {
    pub positions: Vec<PartitionPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPosition {
    pub partition: PartitionId,
    pub position: Position,
}

fn serialize_display<S: serde::Serializer>(
    value: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

pub fn validate_distributed(plan: &RunPlan) -> Result<(), ControlError> {
    todo!("{plan:?}")
}

impl WorkerPlan {
    pub fn new(
        plan: &RunPlan,
        partitions: &[PartitionId],
        worker_id: u32,
    ) -> Result<Self, ControlError> {
        todo!("{plan:?} {partitions:?} {worker_id}")
    }
}

pub fn manifest_page(
    files: &[ManifestEntry],
    from_file_seq: u64,
    limit: usize,
) -> Result<ManifestPage, ControlError> {
    todo!("{files:?} {from_file_seq} {limit}")
}

pub fn resume_response(
    request: &ResumeRequest,
    partitions: &[PartitionId],
    committed: &[Position],
) -> Result<ResumeResponse, ControlError> {
    todo!("{request:?} {partitions:?} {committed:?}")
}

pub fn resume_from(committed: Position, last_published: Option<Position>) -> Position {
    todo!("{committed:?} {last_published:?}")
}
