use ashiato_core::AnalysisWindow;
use ashiato_core::macros::Dialect;
use ashiato_core::query::{SignalQueryError, batch_signal_sql};
use ashiato_core::rule::SignalRule;
use ashiato_pipeline::EventTime;
use duckdb::types::{TimeUnit, Value};

use crate::backend::query_signals;

const CREATE_BATCH_EVENTS_SQL: &str = "CREATE OR REPLACE TEMP TABLE events AS \
     SELECT * REPLACE ( \
         CAST(request_parameters AS JSON) AS request_parameters, \
         CAST(response_elements AS JSON) AS response_elements, \
         CAST(additional_event_data AS JSON) AS additional_event_data, \
         CAST(resources AS JSON) AS resources) \
     FROM pending WHERE event_time >= ? AND event_time < ?";

#[derive(Debug, thiserror::Error)]
pub enum BatchQueryError {
    #[error(transparent)]
    Signal(#[from] SignalQueryError),
    #[error("failed to store the results of a batch")]
    DuckDb(#[from] duckdb::Error),
    #[error("failed to encode a signal")]
    Json(#[from] serde_json::Error),
}

pub(crate) struct BatchQueries {
    rules: Vec<SignalRule>,
    window: AnalysisWindow,
}

impl BatchQueries {
    pub(crate) fn new(rules: Vec<SignalRule>, window: AnalysisWindow) -> Self {
        Self { rules, window }
    }

    pub(crate) fn run(
        &self,
        connection: &duckdb::Connection,
        start: EventTime,
        end: EventTime,
    ) -> Result<(), BatchQueryError> {
        connection.execute(CREATE_BATCH_EVENTS_SQL, [timestamp(start), timestamp(end)])?;
        if start < self.window.until() && self.window.since() < end {
            self.query_signals(connection, start, end)?;
        }
        connection.execute_batch("DROP TABLE events;")?;
        Ok(())
    }

    fn query_signals(
        &self,
        connection: &duckdb::Connection,
        start: EventTime,
        end: EventTime,
    ) -> Result<(), BatchQueryError> {
        for rule in &self.rules {
            let sql = batch_signal_sql(rule, Dialect::DuckDb, &self.window, start, end)?;
            let rows =
                query_signals(connection, &sql).map_err(|source| SignalQueryError::Backend {
                    rule_id: rule.id.clone(),
                    source,
                })?;
            for mut row in rows {
                // array_agg returns the event IDs in no particular order, so they are sorted to
                // keep the output the same from run to run (streaming-pipeline.md 11).
                row.event_ids.sort();
                connection.execute(
                    "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        rule.id,
                        timestamp(start),
                        timestamp(row.start_time),
                        timestamp(row.end_time),
                        serde_json::to_string(&row.event_ids)?,
                        row.access_key_id,
                        row.principal_key,
                        row.source_ip,
                        row.account_id,
                        row.region,
                        serde_json::to_string(&row.attrs)?,
                    ],
                )?;
            }
        }
        Ok(())
    }
}

pub(crate) fn timestamp(time: EventTime) -> Value {
    Value::Timestamp(TimeUnit::Microsecond, time.timestamp_micros())
}
