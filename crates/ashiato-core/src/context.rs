use std::collections::BTreeMap;

use ipnet::IpNet;
use serde::Deserialize;
use serde_json::Value;

use crate::Duration;
use crate::rule::Rules;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Context {
    pub organization: Organization,
    pub suppress: Vec<Suppression>,
    pub sensitive: Sensitive,
    pub baseline: Baseline,
    pub scoring: Scoring,
    pub params: BTreeMap<String, Value>,
    #[serde(skip)]
    digest: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Organization {
    pub internal_cidrs: Vec<IpNet>,
    pub trusted_accounts: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Suppression {
    pub origin: Option<String>,
    pub signals: Option<Vec<String>>,
    pub signal: Option<String>,
    pub principal_key: Option<String>,
    pub attrs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Sensitive {
    pub s3_buckets: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Baseline {
    pub lookback: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Scoring {
    pub min_tactics: usize,
    pub threshold: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("failed to parse the context file")]
    Parse(#[from] serde_yaml::Error),
    #[error("suppress entry {index} has no condition, so it would suppress every signal")]
    EmptySuppression { index: usize },
    #[error("param {key} must be written as <rule_id>.<param>")]
    MalformedParamKey { key: String },
    #[error("param {key} refers to {rule_id}, which is not a signal rule")]
    UnknownRule { key: String, rule_id: String },
    #[error("param {key} is not declared in the params of signal rule {rule_id}")]
    UndeclaredParam { key: String, rule_id: String },
}

impl Default for Context {
    fn default() -> Self {
        todo!()
    }
}

impl Default for Baseline {
    fn default() -> Self {
        todo!()
    }
}

impl Default for Scoring {
    fn default() -> Self {
        todo!()
    }
}

impl Context {
    pub fn parse(_text: &str) -> Result<Self, ContextError> {
        todo!()
    }

    pub fn digest(&self) -> Option<&str> {
        todo!()
    }

    pub fn apply_params(&self, _rules: &Rules) -> Result<Rules, ContextError> {
        todo!()
    }
}
