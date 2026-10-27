use ashiato_pipeline::{
    DeliveryHandle, EventTime, Manifest, PartitionId, PartitionReader, Position, TransportError,
};
use chrono::TimeDelta;

use crate::{PipelineStore, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyzerSettings {
    pub batch: TimeDelta,
    pub allowed_lateness: TimeDelta,
    pub read_since: EventTime,
    pub fetch_max: usize,
    pub commit_every: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("failed to receive the messages of partition {partition}")]
    Transport {
        partition: PartitionId,
        #[source]
        source: TransportError,
    },
    #[error(
        "partition {partition} skipped from {expected:?} to {received:?}; \
         a message was lost or sent out of order"
    )]
    Gap {
        partition: PartitionId,
        expected: Position,
        received: Position,
    },
    #[error("partition {0} stopped sending before its end")]
    Closed(PartitionId),
}

pub struct Analyzer {
    _store: PipelineStore,
}

impl Analyzer {
    pub fn open(
        _store: PipelineStore,
        _manifest: Manifest,
        _settings: AnalyzerSettings,
    ) -> Result<Self, AnalyzerError> {
        todo!()
    }

    pub fn resume_positions(&self) -> Vec<Position> {
        todo!()
    }

    pub fn all_partitions_ended(&self) -> bool {
        todo!()
    }

    pub async fn receive<R: PartitionReader + ?Sized>(
        &mut self,
        _reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        todo!()
    }

    pub fn commit(&mut self) -> Result<Vec<DeliveryHandle>, AnalyzerError> {
        todo!()
    }

    pub async fn run<R: PartitionReader + ?Sized>(
        &mut self,
        _reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        todo!()
    }
}
