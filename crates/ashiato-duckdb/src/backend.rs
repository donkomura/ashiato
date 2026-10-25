use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use ashiato_core::lineage::{Caller, Federation, LineageEdge};
use ashiato_core::macros::Dialect;
use ashiato_core::query::{BackendError, QueryBackend};
use ashiato_core::{AnalysisWindow, SignalRow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use duckdb::arrow::datatypes::DataType;
use duckdb::types::Value;

const ATTR_PREFIX: &str = "attr_";
const LINEAGE_EDGES_SQL: &str = include_str!("../sql/lineage_edges.sql");

pub struct DuckDbBackend {
    connection: Arc<Mutex<duckdb::Connection>>,
}

impl DuckDbBackend {
    pub fn open(path: &Path) -> Result<Self, duckdb::Error> {
        let connection = duckdb::Connection::open(path)?;
        connection.execute_batch(crate::SCHEMA_SQL)?;
        Ok(Self::from_connection(connection))
    }

    pub(crate) fn from_connection(connection: duckdb::Connection) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }
}

#[async_trait]
impl QueryBackend for DuckDbBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError> {
        let connection = Arc::clone(&self.connection);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().unwrap_or_else(PoisonError::into_inner);
            query_signals(&connection, &sql)
        })
        .await
        .map_err(|error| BackendError::Query(Box::new(error)))?
    }

    async fn query_lineage(
        &self,
        window: &AnalysisWindow,
    ) -> Result<Vec<LineageEdge>, BackendError> {
        let connection = Arc::clone(&self.connection);
        let since = sql_timestamp(window.baseline_since());
        let until = sql_timestamp(window.until());
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().unwrap_or_else(PoisonError::into_inner);
            query_lineage(&connection, &since, &until)
        })
        .await
        .map_err(|error| BackendError::Query(Box::new(error)))?
    }
}

fn query_signals(
    connection: &duckdb::Connection,
    sql: &str,
) -> Result<Vec<SignalRow>, BackendError> {
    let mut statement = connection.prepare(sql).map_err(query_error)?;
    let mut rows = statement.query([]).map_err(query_error)?;
    let columns = SignalColumns::locate(
        rows.as_ref()
            .expect("rows returned by Statement::query refer to their statement"),
    )?;
    let mut signals = Vec::new();
    while let Some(row) = rows.next().map_err(query_error)? {
        signals.push(columns.read(row)?);
    }
    Ok(signals)
}

// Sessions used in the analysis period may have been issued during the baseline period,
// so edges are taken from the start of the baseline period.
fn query_lineage(
    connection: &duckdb::Connection,
    since: &str,
    until: &str,
) -> Result<Vec<LineageEdge>, BackendError> {
    let mut statement = connection.prepare(LINEAGE_EDGES_SQL).map_err(query_error)?;
    let edges = statement
        .query_map([since, until], |row| {
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
        })
        .map_err(query_error)?;
    edges.collect::<Result<_, _>>().map_err(query_error)
}

fn sql_timestamp(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

struct Column {
    name: String,
    index: usize,
    untyped_null: bool,
}

impl Column {
    fn new(statement: &duckdb::Statement<'_>, name: &str, index: usize) -> Self {
        Self {
            name: name.to_owned(),
            index,
            untyped_null: statement.column_type(index) == DataType::Null,
        }
    }

    fn value(&self, row: &duckdb::Row<'_>) -> Result<Value, BackendError> {
        // duckdb 1.20000.0 panics instead of returning Value::Null when reading a column
        // whose type is the untyped NULL, such as `NULL AS attr_x` in a rule's SQL.
        if self.untyped_null {
            return Ok(Value::Null);
        }
        row.get(self.index).map_err(query_error)
    }

    fn invalid(&self, expected: &str, value: &Value) -> BackendError {
        BackendError::InvalidValue {
            column: self.name.clone(),
            message: format!("expected {expected}, but got {value:?}"),
        }
    }

    fn timestamp(&self, row: &duckdb::Row<'_>) -> Result<DateTime<Utc>, BackendError> {
        match self.value(row)? {
            Value::Timestamp(unit, value) => DateTime::from_timestamp_micros(unit.to_micros(value))
                .ok_or_else(|| {
                    self.invalid("a representable timestamp", &Value::Timestamp(unit, value))
                }),
            other => Err(self.invalid("a timestamp", &other)),
        }
    }

    fn text(&self, row: &duckdb::Row<'_>) -> Result<String, BackendError> {
        match self.value(row)? {
            Value::Text(text) => Ok(text),
            other => Err(self.invalid("a non-NULL string", &other)),
        }
    }

    fn optional_text(&self, row: &duckdb::Row<'_>) -> Result<Option<String>, BackendError> {
        match self.value(row)? {
            Value::Null => Ok(None),
            Value::Text(text) => Ok(Some(text)),
            other => Err(self.invalid("a string or NULL", &other)),
        }
    }

    fn text_list(&self, row: &duckdb::Row<'_>) -> Result<Vec<String>, BackendError> {
        match self.value(row)? {
            Value::List(items) | Value::Array(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::Text(text) => Ok(text),
                    other => Err(self.invalid("a list of strings", &other)),
                })
                .collect(),
            other => Err(self.invalid("a list of strings", &other)),
        }
    }
}

struct SignalColumns {
    start_time: Column,
    end_time: Column,
    event_ids: Column,
    access_key_id: Column,
    principal_key: Column,
    source_ip: Column,
    account_id: Column,
    region: Column,
    attrs: Vec<(String, Column)>,
}

impl SignalColumns {
    fn locate(statement: &duckdb::Statement<'_>) -> Result<Self, BackendError> {
        let names = statement.column_names();
        let required = |name: &str| {
            names
                .iter()
                .position(|candidate| candidate == name)
                .map(|index| Column::new(statement, name, index))
                .ok_or_else(|| BackendError::MissingColumn {
                    column: name.to_owned(),
                })
        };
        let attrs = names
            .iter()
            .enumerate()
            .filter_map(|(index, name)| {
                let attr = name.strip_prefix(ATTR_PREFIX)?;
                Some((attr.to_owned(), Column::new(statement, name, index)))
            })
            .collect();
        Ok(Self {
            start_time: required("start_time")?,
            end_time: required("end_time")?,
            event_ids: required("event_ids")?,
            access_key_id: required("access_key_id")?,
            principal_key: required("principal_key")?,
            source_ip: required("source_ip")?,
            account_id: required("account_id")?,
            region: required("region")?,
            attrs,
        })
    }

    fn read(&self, row: &duckdb::Row<'_>) -> Result<SignalRow, BackendError> {
        let mut attrs = BTreeMap::new();
        for (attr, column) in &self.attrs {
            if let Some(value) = column.optional_text(row)? {
                attrs.insert(attr.clone(), value);
            }
        }
        Ok(SignalRow {
            start_time: self.start_time.timestamp(row)?,
            end_time: self.end_time.timestamp(row)?,
            event_ids: self.event_ids.text_list(row)?,
            access_key_id: self.access_key_id.optional_text(row)?,
            principal_key: self.principal_key.text(row)?,
            source_ip: self.source_ip.text(row)?,
            account_id: self.account_id.text(row)?,
            region: self.region.text(row)?,
            attrs,
        })
    }
}

fn query_error(error: duckdb::Error) -> BackendError {
    BackendError::Query(Box::new(error))
}
