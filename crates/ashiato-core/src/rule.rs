use std::collections::BTreeMap;

use serde::Deserialize;

use crate::{Severity, Tactic};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Rule {
    Signal(SignalRule),
    Scenario(ScenarioRule),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRule {
    pub id: String,
    pub title: String,
    pub severity: Severity,
    pub tactics: Vec<Tactic>,
    #[serde(default)]
    pub requires: Vec<DataRequirement>,
    #[serde(default)]
    pub params: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub attrs: Vec<String>,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataRequirement {
    S3DataEvents,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioRule {
    pub id: String,
    pub title: String,
    pub severity: Severity,
    pub tactics: Vec<Tactic>,
    pub window: String,
    pub steps: Vec<ScenarioStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioStep {
    pub signal: String,
    #[serde(default)]
    pub optional: bool,
    #[serde(default = "default_min_count")]
    pub min_count: u32,
    #[serde(default)]
    pub distinct_by: Option<String>,
    #[serde(default)]
    pub max_gap: Option<String>,
    #[serde(default)]
    pub same: Vec<String>,
}

fn default_min_count() -> u32 {
    1
}

impl Rule {
    pub fn id(&self) -> &str {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuleError {
    #[error("failed to parse rule file {path}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("rule id {id} is defined more than once in rule set {ruleset}")]
    DuplicateId { id: String, ruleset: String },
    #[error("scenario {scenario} refers to {signal}, which is not a signal rule")]
    UnknownSignal { scenario: String, signal: String },
    #[error("scenario {scenario} uses attribute {attr}, which signal {signal} does not declare")]
    UndeclaredAttribute {
        scenario: String,
        signal: String,
        attr: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct RuleSet {
    id: String,
    digest: String,
    rules: Vec<Rule>,
}

impl RuleSet {
    pub fn parse(_id: impl Into<String>, _files: Vec<RuleFile>) -> Result<Self, RuleError> {
        todo!()
    }

    pub fn builtin() -> Result<Self, RuleError> {
        todo!()
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSetDigest {
    pub id: String,
    pub digest: String,
}

#[derive(Debug, Clone)]
pub struct Rules {
    rules: BTreeMap<String, Rule>,
    rulesets: Vec<RuleSetDigest>,
}

impl Rules {
    pub fn new(_sets: impl IntoIterator<Item = RuleSet>) -> Result<Self, RuleError> {
        todo!()
    }

    pub fn get(&self, id: &str) -> Option<&Rule> {
        self.rules.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Rule> {
        self.rules.values()
    }

    pub fn rulesets(&self) -> &[RuleSetDigest] {
        &self.rulesets
    }
}
