use std::path::Path;

use ashiato_pipeline::{Manifest, ManifestEntry, PartitionId, PartitionManifest};
use chrono::DateTime;
use duckdb::OptionalExt;
use duckdb::types::{TimeUnit, Value};

const PIPELINE_SCHEMA_SQL: &str = include_str!("../sql/pipeline_schema.sql");
const SELECT_MANIFEST_SQL: &str = "SELECT partition_index, account, region, file_seq, uri, \
     version_id, etag, size, epoch_us(file_time) \
     FROM manifest ORDER BY partition_index, file_seq NULLS FIRST";

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
    pub(crate) connection: duckdb::Connection,
}

struct ManifestRow {
    partition_index: u32,
    account: String,
    region: String,
    file_seq: Option<u64>,
    uri: Option<String>,
    version_id: Option<String>,
    etag: Option<String>,
    size: Option<u64>,
    file_time_us: Option<i64>,
}

impl PipelineStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::initialize(duckdb::Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::initialize(duckdb::Connection::open_in_memory()?)
    }

    fn initialize(connection: duckdb::Connection) -> Result<Self, StoreError> {
        connection.execute_batch(PIPELINE_SCHEMA_SQL)?;
        Ok(Self { connection })
    }

    // The manifest is listed only when the run starts for the first time. Listing again on a
    // resumed run could reorder the files and make the same file_seq point at another file.
    pub fn start_run<E: From<StoreError>>(
        &mut self,
        digest: &str,
        list: impl FnOnce() -> Result<Manifest, E>,
    ) -> Result<Manifest, E> {
        if let Some(stored) = self.stored_digest()? {
            if stored != digest {
                return Err(StoreError::DigestMismatch {
                    stored,
                    given: digest.to_owned(),
                }
                .into());
            }
            return Ok(self.saved_manifest()?);
        }
        let manifest = list()?;
        self.save(digest, &manifest)?;
        Ok(manifest)
    }

    fn stored_digest(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT digest FROM run_meta", [], |row| row.get(0))
            .optional()?)
    }

    fn save(&mut self, digest: &str, manifest: &Manifest) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        {
            let mut appender = transaction.appender("manifest")?;
            for (index, partition) in manifest.partitions.iter().enumerate() {
                let index = u32::try_from(index).map_err(|_| {
                    StoreError::InconsistentManifest(format!("too many partitions: {index}"))
                })?;
                let PartitionId { account, region } = &partition.partition;
                if partition.files.is_empty() {
                    appender.append_row((
                        index,
                        account,
                        region,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ))?;
                }
                for file in &partition.files {
                    appender.append_row((
                        index,
                        account,
                        region,
                        file.file_seq,
                        &file.uri,
                        &file.version_id,
                        &file.etag,
                        file.size,
                        Value::Timestamp(TimeUnit::Microsecond, file.file_time.timestamp_micros()),
                    ))?;
                }
            }
            appender.flush()?;
        }
        transaction.execute("INSERT INTO run_meta VALUES (?)", [digest])?;
        transaction.commit()?;
        Ok(())
    }

    fn saved_manifest(&self) -> Result<Manifest, StoreError> {
        let mut statement = self.connection.prepare(SELECT_MANIFEST_SQL)?;
        let rows = statement.query_map([], |row| {
            Ok(ManifestRow {
                partition_index: row.get(0)?,
                account: row.get(1)?,
                region: row.get(2)?,
                file_seq: row.get(3)?,
                uri: row.get(4)?,
                version_id: row.get(5)?,
                etag: row.get(6)?,
                size: row.get(7)?,
                file_time_us: row.get(8)?,
            })
        })?;
        let mut manifest = Manifest::default();
        for row in rows {
            let row = row?;
            let index = usize::try_from(row.partition_index).ok();
            if index == Some(manifest.partitions.len()) {
                manifest.partitions.push(PartitionManifest {
                    partition: PartitionId::new(row.account, row.region),
                    files: Vec::new(),
                });
            }
            let last_index = manifest.partitions.len().checked_sub(1);
            let partition = match manifest.partitions.last_mut() {
                Some(partition) if index == last_index => partition,
                _ => {
                    return Err(inconsistent(format!(
                        "partition {} is out of sequence",
                        row.partition_index
                    )));
                }
            };
            if let Some(file_seq) = row.file_seq {
                if usize::try_from(file_seq).ok() != Some(partition.files.len()) {
                    return Err(inconsistent(format!(
                        "file {file_seq} of partition {} is out of sequence",
                        partition.partition
                    )));
                }
                partition.files.push(ManifestEntry {
                    file_seq,
                    uri: row.uri.ok_or_else(|| missing("uri", file_seq))?,
                    version_id: row.version_id,
                    etag: row.etag,
                    size: row.size.ok_or_else(|| missing("size", file_seq))?,
                    file_time: row
                        .file_time_us
                        .and_then(DateTime::from_timestamp_micros)
                        .ok_or_else(|| missing("file_time", file_seq))?,
                });
            }
        }
        Ok(manifest)
    }
}

fn inconsistent(message: String) -> StoreError {
    StoreError::InconsistentManifest(message)
}

fn missing(column: &str, file_seq: u64) -> StoreError {
    inconsistent(format!("{column} of file {file_seq} is missing"))
}
