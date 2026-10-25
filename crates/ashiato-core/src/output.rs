use std::collections::BTreeMap;
use std::io::Write;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::lineage::{LineageStep, Origin};
use crate::rule::RuleSetDigest;
use crate::{Severity, Tactic};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputRecord {
    Run(RunRecord),
    Signal(SignalRecord),
    ScenarioMatch(ScenarioMatchRecord),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunRecord {
    pub tool_version: String,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub rulesets: Vec<RuleSetDigest>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepRecord {
    pub signal_id: String,
    pub signals: Vec<SignalEvidence>,
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
    fn write(&mut self, _record: &OutputRecord) -> Result<(), SinkError> {
        todo!()
    }
}
