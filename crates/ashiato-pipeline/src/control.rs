use ashiato_core::Duration;
use serde::{Deserialize, Serialize};

use crate::{
    ManifestEntry, PartitionId, PlacementMode, Position, RunPlan, SourceLocation, assign_partitions,
};

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

// Checked when the run starts, so that a plan the workers cannot follow fails before the
// workspace is made.
pub fn validate_distributed(plan: &RunPlan) -> Result<(), ControlError> {
    if plan.placement.mode != PlacementMode::Distributed {
        return Ok(());
    }
    if matches!(plan.source.location, SourceLocation::Local { .. }) {
        return Err(ControlError::LocalSourceInDistributed);
    }
    if plan.placement.nats_url.is_none() {
        return Err(ControlError::MissingNatsUrl);
    }
    Ok(())
}

impl WorkerPlan {
    // partitions are in the order of the saved manifest. Worker ids start at 0 and index the
    // assignment of assign_partitions.
    pub fn new(
        plan: &RunPlan,
        partitions: &[PartitionId],
        worker_id: u32,
    ) -> Result<Self, ControlError> {
        if plan.placement.mode != PlacementMode::Distributed {
            return Err(ControlError::NotDistributed);
        }
        let SourceLocation::S3 { uri, endpoint_url } = &plan.source.location else {
            return Err(ControlError::LocalSourceInDistributed);
        };
        let workers = plan.placement.ingest_workers;
        let unknown = || ControlError::UnknownWorker {
            worker_id,
            workers: workers.get(),
        };
        let slot = usize::try_from(worker_id).map_err(|_| unknown())?;
        let assigned = assign_partitions(partitions, workers)
            .into_iter()
            .nth(slot)
            .ok_or_else(unknown)?;
        Ok(Self {
            run_id: plan.run_id.clone(),
            worker_id,
            source: WorkerSource {
                uri: uri.clone(),
                endpoint_url: endpoint_url.clone(),
            },
            allowed_lateness: plan.pipeline.allowed_lateness,
            max_message_size: plan.broker.max_message_size,
            partitions: assigned
                .into_iter()
                .map(|index| AssignedPartition {
                    index: u32::try_from(index)
                        .expect("the manifest holds fewer than 2^32 partitions"),
                    partition: partitions[index].clone(),
                })
                .collect(),
        })
    }
}

// Takes as many entries from from_file_seq on as fit in a reply of limit bytes, so that a list
// larger than the max_payload of NATS is returned over several requests (7.4 節).
pub fn manifest_page(
    files: &[ManifestEntry],
    from_file_seq: u64,
    limit: usize,
) -> Result<ManifestPage, ControlError> {
    // The size of the reply is counted as the empty reply plus the entries and their commas,
    // which is exact for compact JSON. The empty reply is measured with the longest
    // next_file_seq, so that the reply fits whichever value it ends up with.
    let envelope = serde_json::to_vec(&ManifestPage {
        files: Vec::new(),
        next_file_seq: Some(u64::MAX),
    })?
    .len();
    let remaining = files.iter().filter(|entry| entry.file_seq >= from_file_seq);
    let mut page = Vec::new();
    let mut size = envelope;
    for entry in remaining {
        let entry_size = serde_json::to_vec(entry)?.len() + usize::from(!page.is_empty());
        if size + entry_size > limit {
            if page.is_empty() {
                return Err(ControlError::EntryTooLarge {
                    file_seq: entry.file_seq,
                    size: entry_size,
                    limit,
                });
            }
            return Ok(ManifestPage {
                files: page,
                next_file_seq: Some(entry.file_seq),
            });
        }
        size += entry_size;
        page.push(entry.clone());
    }
    Ok(ManifestPage {
        files: page,
        next_file_seq: None,
    })
}

// partitions and committed are in the order of the saved manifest.
pub fn resume_response(
    request: &ResumeRequest,
    partitions: &[PartitionId],
    committed: &[Position],
) -> Result<ResumeResponse, ControlError> {
    let positions = request
        .partitions
        .iter()
        .map(|partition| {
            let index = partitions
                .iter()
                .position(|candidate| candidate == partition)
                .ok_or_else(|| ControlError::UnknownPartition(partition.clone()))?;
            Ok(PartitionPosition {
                partition: partition.clone(),
                position: committed[index],
            })
        })
        .collect::<Result<_, ControlError>>()?;
    Ok(ResumeResponse { positions })
}

// last_published is the end of the last message of the partition left in the stream. Starting
// from it saves sending again what the analyzer has yet to read; starting from committed alone
// would also be correct, since the analyzer drops the duplicates by their position (6.4 節).
pub fn resume_from(committed: Position, last_published: Option<Position>) -> Position {
    last_published.map_or(committed, |published| committed.max(published))
}
