use std::path::Path;

use ashiato_core::SignalRow;
use ashiato_core::lineage::{Caller, Federation, LineageEdge};
use ashiato_core::query::DataAvailability;
use ashiato_core::sequence::ObservedSignal;
use ashiato_pipeline::{Manifest, ManifestEntry, PartitionId, PartitionManifest};
use chrono::{DateTime, Utc};
use duckdb::OptionalExt;
use duckdb::types::{TimeUnit, Value};

const PIPELINE_SCHEMA_SQL: &str = include_str!("../sql/pipeline_schema.sql");
const SELECT_MANIFEST_SQL: &str = "SELECT partition_index, account, region, file_seq, uri, \
     version_id, etag, size, epoch_us(file_time) \
     FROM manifest ORDER BY partition_index, file_seq NULLS FIRST";

const SELECT_SIGNALS_SQL: &str = "SELECT rule_id, epoch_us(start_time), epoch_us(end_time), \
     event_ids, access_key_id, principal_key, source_ip, account_id, region, attrs \
     FROM signals \
     ORDER BY rule_id, start_time, end_time, event_ids, access_key_id NULLS FIRST, \
     principal_key, source_ip, account_id, region, attrs";
const SELECT_LINEAGE_EDGES_SQL: &str = "SELECT child_access_key_id, parent_access_key_id, \
     parent_principal_key, parent_principal_arn, parent_identity_type, parent_account_id, \
     parent_invoked_by, role_arn, issued_by_event_id, cross_account, federation_provider, \
     federation_subject \
     FROM lineage_edges \
     QUALIFY row_number() OVER (PARTITION BY child_access_key_id ORDER BY batch_start, seq) = 1 \
     ORDER BY batch_start, seq";

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
    #[error("a saved signal is not valid JSON")]
    Json(#[source] serde_json::Error),
    #[error("a saved time is out of range: {0} microseconds")]
    TimeOutOfRange(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestCounts {
    pub ingested_files: u64,
    pub file_errors: u64,
    pub late_events: u64,
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

    pub fn set_memory_limit(&self, limit: &str) -> Result<(), StoreError> {
        self.connection.execute("SET memory_limit = ?", [limit])?;
        Ok(())
    }

    pub fn memory_limit(&self) -> Result<String, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT current_setting('memory_limit')", [], |row| {
                row.get(0)
            })?)
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

    // Signals are read in a fixed order, so that the output does not depend on the order in
    // which DuckDB returned the rows of a rule (streaming-pipeline.md 11).
    pub fn observed_signals(&self) -> Result<Vec<ObservedSignal>, StoreError> {
        let mut statement = self.connection.prepare(SELECT_SIGNALS_SQL)?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
            ))
        })?;
        let mut signals = Vec::new();
        for row in rows {
            let (
                signal_id,
                start,
                end,
                event_ids,
                access_key_id,
                principal_key,
                source_ip,
                account_id,
                region,
                attrs,
            ) = row?;
            signals.push(ObservedSignal {
                signal_id,
                row: SignalRow {
                    start_time: micros(start)?,
                    end_time: micros(end)?,
                    event_ids: serde_json::from_str(&event_ids).map_err(StoreError::Json)?,
                    access_key_id,
                    principal_key,
                    source_ip,
                    account_id,
                    region,
                    attrs: serde_json::from_str(&attrs).map_err(StoreError::Json)?,
                },
            });
        }
        Ok(signals)
    }

    // Edges for the same child access key are kept once, in the order of event_time and
    // event_id, which is the same as the lineage SQL over all events.
    pub fn lineage_edges(&self) -> Result<Vec<LineageEdge>, StoreError> {
        let mut statement = self.connection.prepare(SELECT_LINEAGE_EDGES_SQL)?;
        let edges = statement.query_map([], |row| {
            let federation = match (row.get(10)?, row.get(11)?) {
                (Some(provider), Some(subject)) => Some(Federation { provider, subject }),
                _ => None,
            };
            Ok(LineageEdge {
                child_access_key_id: row.get(0)?,
                parent: Caller {
                    access_key_id: row.get(1)?,
                    principal_key: row.get(2)?,
                    principal_arn: row.get(3)?,
                    identity_type: row.get(4)?,
                    account_id: row.get(5)?,
                    invoked_by: row.get(6)?,
                },
                role_arn: row.get(7)?,
                issued_by_event_id: row.get(8)?,
                cross_account: row.get(9)?,
                federation,
            })
        })?;
        Ok(edges.collect::<Result<_, _>>()?)
    }

    pub fn data_availability(&self) -> Result<DataAvailability, StoreError> {
        Ok(self.connection.query_row(
            "SELECT events, s3_data_events > 0 FROM data_counters",
            [],
            |row| {
                Ok(DataAvailability {
                    events: row.get(0)?,
                    s3_data_events: row.get(1)?,
                })
            },
        )?)
    }

    pub fn ingest_counts(&self) -> Result<IngestCounts, StoreError> {
        Ok(self.connection.query_row(
            "SELECT \
                 (SELECT count(*) FROM ingested_files WHERE error IS NULL), \
                 (SELECT count(*) FROM ingested_files WHERE error IS NOT NULL), \
                 (SELECT count(*) FROM late_events)",
            [],
            |row| {
                Ok(IngestCounts {
                    ingested_files: row.get(0)?,
                    file_errors: row.get(1)?,
                    late_events: row.get(2)?,
                })
            },
        )?)
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

fn micros(value: i64) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp_micros(value).ok_or(StoreError::TimeOutOfRange(value))
}
