use std::collections::HashMap;

use ashiato_pipeline::{
    CommittedPositions, ControlClient, ControlError, Manifest, ManifestEntry, PartitionId,
    Position, RunPlan, TransportError, WorkerPlan,
};
use async_trait::async_trait;

use crate::broker::NatsError;

// The answers of the analyzer to the requests of the ingest workers (7.4 節).
pub struct ControlService {}

impl ControlService {
    pub fn new(
        plan: &RunPlan,
        manifest: Manifest,
        committed: CommittedPositions,
    ) -> Result<Self, ControlError> {
        let _ = (plan, manifest, committed);
        todo!()
    }
}

pub struct ControlServer {}

pub async fn serve_control(
    client: async_nats::Client,
    run_id: &str,
    service: ControlService,
) -> Result<ControlServer, NatsError> {
    let _ = (client, run_id, service);
    todo!()
}

pub struct NatsControl {}

impl NatsControl {
    pub fn new(client: async_nats::Client, run_id: &str) -> Result<Self, NatsError> {
        let _ = (client, run_id);
        todo!()
    }
}

#[async_trait]
impl ControlClient for NatsControl {
    async fn plan(&self, worker_id: u32) -> Result<WorkerPlan, TransportError> {
        let _ = worker_id;
        todo!()
    }

    async fn manifest(
        &self,
        partition: &PartitionId,
        from_file_seq: u64,
    ) -> Result<Vec<ManifestEntry>, TransportError> {
        let _ = (partition, from_file_seq);
        todo!()
    }

    async fn resume_positions(
        &self,
        partitions: &[PartitionId],
    ) -> Result<HashMap<PartitionId, Position>, TransportError> {
        let _ = partitions;
        todo!()
    }

    async fn last_published(
        &self,
        partition: &PartitionId,
    ) -> Result<Option<Position>, TransportError> {
        let _ = partition;
        todo!()
    }
}
