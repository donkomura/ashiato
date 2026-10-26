use std::collections::BTreeMap;
use std::io::Write;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::lineage::{LineageStep, Origin};
use crate::query::DataAvailability;
use crate::rule::{DataRequirement, RuleSourceDigest};
use crate::ruleset::{RuleRefs, RulesetDigest};
use crate::{Severity, SignalRow, Tactic};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputRecord {
    Run(RunRecord),
    RuleSkipped(RuleSkippedRecord),
    Signal(SignalRecord),
    ScenarioMatch(ScenarioMatchRecord),
    Summary(SummaryRecord),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunRecord {
    pub tool_version: String,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub rule_sources: Vec<RuleSourceDigest>,
    pub rulesets: Vec<RulesetDigest>,
    pub data: DataAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuleSkippedRecord {
    pub rule_id: String,
    pub reason: SkipReason,
    pub requires: Vec<DataRequirement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    MissingData,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SignalRecord {
    pub signal_id: String,
    pub severity: Severity,
    pub tactics: Vec<Tactic>,
    pub origin: Origin,
    pub principal_key: String,
    pub account_id: String,
    pub region: String,
    #[serde(flatten)]
    pub evidence: SignalEvidence,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<RuleRefs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SignalEvidence {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub event_ids: Vec<String>,
    pub access_key_id: Option<String>,
    pub source_ip: String,
    pub attrs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScenarioMatchRecord {
    pub scenario_id: String,
    pub severity: Severity,
    pub origin: Origin,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub steps: Vec<StepRecord>,
    pub lineage: Vec<LineageStep>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<RuleRefs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepRecord {
    pub signal_id: String,
    pub signals: Vec<SignalEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    ScenarioMatch,
    Signal,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SummaryRecord {
    pub kind: RecordKind,
    pub rule_id: String,
    pub severity: Severity,
    pub origin: Origin,
    pub count: usize,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub source_ips: usize,
    pub events: usize,
}

pub trait Sink {
    fn write(&mut self, record: &OutputRecord) -> Result<(), SinkError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("failed to write an output record")]
    Io(#[from] std::io::Error),
}

pub struct JsonLinesSink<W: Write> {
    writer: W,
}

impl<W: Write> JsonLinesSink<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> Sink for JsonLinesSink<W> {
    fn write(&mut self, record: &OutputRecord) -> Result<(), SinkError> {
        serde_json::to_writer(&mut self.writer, record).map_err(std::io::Error::from)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }
}

impl From<&SignalRow> for SignalEvidence {
    fn from(row: &SignalRow) -> Self {
        Self {
            start_time: row.start_time,
            end_time: row.end_time,
            event_ids: row.event_ids.clone(),
            access_key_id: row.access_key_id.clone(),
            source_ip: row.source_ip.clone(),
            attrs: row.attrs.clone(),
        }
    }
}
