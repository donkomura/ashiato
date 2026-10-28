use ashiato_core::AnalysisWindow;
use ashiato_core::macros::Dialect;
use ashiato_core::query::{BackendError, SignalQueryError, batch_signal_sql};
use ashiato_core::rule::SignalRule;
use ashiato_pipeline::EventTime;
use duckdb::types::{TimeUnit, Value};

use crate::backend::{query_lineage, query_signals, sql_timestamp};

const CREATE_BATCH_EVENTS_SQL: &str = "CREATE OR REPLACE TEMP TABLE events AS \
     SELECT * REPLACE ( \
         CAST(request_parameters AS JSON) AS request_parameters, \
         CAST(response_elements AS JSON) AS response_elements, \
         CAST(additional_event_data AS JSON) AS additional_event_data, \
         CAST(resources AS JSON) AS resources) \
     FROM pending WHERE event_time >= ? AND event_time < ?";

// The same rows as the baseline CTE of the batch processing (design.md 6.4). A row already
// added from an earlier batch is not added again, so the table has no duplicates either.
const INSERT_BASELINE_SQL: &str = "INSERT INTO baseline \
     WITH period AS (SELECT * FROM events WHERE event_time >= $1 AND event_time < $2) \
     SELECT principal_key, 'source_ip', source_ip FROM period \
     UNION SELECT principal_key, 'user_agent', user_agent FROM period \
     UNION SELECT principal_key, 'region', aws_region FROM period \
     UNION SELECT principal_key, 'event_name', event_name FROM period \
     EXCEPT SELECT principal_key, dim, value FROM baseline";
const INSERT_BASELINE_PRINCIPALS_SQL: &str = "INSERT INTO baseline_principals \
     SELECT principal_key FROM events WHERE event_time >= $1 AND event_time < $2 \
     EXCEPT SELECT principal_key FROM baseline_principals";

#[derive(Debug, thiserror::Error)]
pub enum BatchQueryError {
    #[error(transparent)]
    Signal(#[from] SignalQueryError),
    #[error("failed to query the session lineage")]
    Lineage(#[source] BackendError),
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
        // The baseline period comes before the analysis period, so the events are streamed in
        // time order and the baseline is complete before the first batch of the analysis period
        // is queried (streaming-pipeline.md 8.5).
        let baseline_since = start.max(self.window.baseline_since());
        let baseline_until = end.min(self.window.baseline_until());
        if baseline_since < baseline_until {
            let range = [timestamp(baseline_since), timestamp(baseline_until)];
            connection.execute(INSERT_BASELINE_SQL, range.clone())?;
            connection.execute(INSERT_BASELINE_PRINCIPALS_SQL, range)?;
        }
        if start < self.window.until() && self.window.since() < end {
            self.query_signals(connection, start, end)?;
        }
        // Lineage is taken from the baseline period as well, so that a session issued before
        // the analysis period can still be traced to its origin (streaming-pipeline.md 8.6).
        let since = start.max(self.window.baseline_since());
        let until = end.min(self.window.until());
        if since < until {
            self.query_lineage(connection, start, since, until)?;
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

    fn query_lineage(
        &self,
        connection: &duckdb::Connection,
        start: EventTime,
        since: EventTime,
        until: EventTime,
    ) -> Result<(), BatchQueryError> {
        let edges = query_lineage(connection, &sql_timestamp(since), &sql_timestamp(until))
            .map_err(BatchQueryError::Lineage)?;
        for (seq, edge) in edges.into_iter().enumerate() {
            let (provider, subject) = edge
                .federation
                .map(|federation| (federation.provider, federation.subject))
                .unzip();
            connection.execute(
                "INSERT INTO lineage_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    timestamp(start),
                    seq as u64,
                    edge.child_access_key_id,
                    edge.parent.access_key_id,
                    edge.parent.principal_key,
                    edge.parent.principal_arn,
                    edge.parent.identity_type,
                    edge.parent.account_id,
                    edge.parent.invoked_by,
                    edge.role_arn,
                    edge.issued_by_event_id,
                    edge.cross_account,
                    provider,
                    subject,
                ],
            )?;
        }
        Ok(())
    }
}

pub(crate) fn timestamp(time: EventTime) -> Value {
    Value::Timestamp(TimeUnit::Microsecond, time.timestamp_micros())
}
