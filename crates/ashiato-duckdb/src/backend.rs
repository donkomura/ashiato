use std::path::Path;
use std::sync::{Arc, Mutex};

use ashiato_core::SignalRow;
use ashiato_core::macros::Dialect;
use ashiato_core::query::{BackendError, QueryBackend};
use async_trait::async_trait;

pub struct DuckDbBackend {
    connection: Arc<Mutex<duckdb::Connection>>,
}

impl DuckDbBackend {
    pub fn open(path: &Path) -> Result<Self, duckdb::Error> {
        todo!("open {}", path.display())
    }
}

#[async_trait]
impl QueryBackend for DuckDbBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError> {
        let _ = &self.connection;
        todo!("run {sql}")
    }
}
