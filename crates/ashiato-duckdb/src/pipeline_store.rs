use std::path::Path;

use ashiato_pipeline::Manifest;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(
        "the run plan digest {given} differs from {stored} saved in the workspace; \
         resume with the run plan the run was started with"
    )]
    DigestMismatch { stored: String, given: String },
    #[error("the saved manifest is inconsistent: {0}")]
    InconsistentManifest(String),
}

pub struct PipelineStore {
    _connection: duckdb::Connection,
}

impl PipelineStore {
    pub fn open(_path: &Path) -> Result<Self, StoreError> {
        todo!()
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        todo!()
    }

    pub fn start_run<E: From<StoreError>>(
        &mut self,
        _digest: &str,
        _list: impl FnOnce() -> Result<Manifest, E>,
    ) -> Result<Manifest, E> {
        todo!()
    }
}
