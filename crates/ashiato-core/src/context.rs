use std::collections::BTreeMap;
use std::net::IpAddr;

use ipnet::IpNet;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::lineage::Origin;
use crate::rule::Rules;
use crate::{Duration, SignalRow};

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
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

const DEFAULT_LOOKBACK: &str = "30d";
const DEFAULT_MIN_TACTICS: usize = 3;
const DEFAULT_THRESHOLD: u32 = 10;

impl Default for Baseline {
    fn default() -> Self {
        Self {
            lookback: DEFAULT_LOOKBACK
                .parse()
                .expect("the default lookback is written as a valid duration"),
        }
    }
}

impl Default for Scoring {
    fn default() -> Self {
        Self {
            min_tactics: DEFAULT_MIN_TACTICS,
            threshold: DEFAULT_THRESHOLD,
        }
    }
}

impl Context {
    pub fn parse(text: &str) -> Result<Self, ContextError> {
        // A file with only comments is a YAML null, which is read as a file without settings.
        let value: serde_yaml::Value = serde_yaml::from_str(text)?;
        let mut context: Self = if value.is_null() {
            Self::default()
        } else {
            serde_yaml::from_value(value)?
        };
        if let Some(index) = context.suppress.iter().position(Suppression::is_empty) {
            return Err(ContextError::EmptySuppression { index });
        }
        if let Some(key) = context.params.keys().find(|key| split_param(key).is_none()) {
            return Err(ContextError::MalformedParamKey { key: key.clone() });
        }
        context.digest = Some(sha256(text));
        Ok(context)
    }

    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    pub fn apply_params(&self, rules: &Rules) -> Result<Rules, ContextError> {
        let mut rules = rules.clone();
        for (key, value) in &self.params {
            let (rule_id, param) = split_param(key)
                .ok_or_else(|| ContextError::MalformedParamKey { key: key.clone() })?;
            let rule = rules
                .signal_rule_mut(rule_id)
                .ok_or_else(|| ContextError::UnknownRule {
                    key: key.clone(),
                    rule_id: rule_id.to_owned(),
                })?;
            let declared =
                rule.params
                    .get_mut(param)
                    .ok_or_else(|| ContextError::UndeclaredParam {
                        key: key.clone(),
                        rule_id: rule_id.to_owned(),
                    })?;
            *declared = value.clone();
        }
        Ok(rules)
    }

    pub(crate) fn suppresses(&self, signal_id: &str, origin: &Origin, row: &SignalRow) -> bool {
        self.suppress
            .iter()
            .any(|entry| entry.matches(signal_id, origin, row))
    }

    // A source that is not an IP address, such as the host name of an AWS service, is never
    // inside the organization's networks.
    pub(crate) fn is_internal_source(&self, source_ip: &str) -> bool {
        source_ip.parse::<IpAddr>().is_ok_and(|ip| {
            self.organization
                .internal_cidrs
                .iter()
                .any(|cidr| cidr.contains(&ip))
        })
    }
}

impl Suppression {
    fn matches(&self, signal_id: &str, origin: &Origin, row: &SignalRow) -> bool {
        self.origin
            .as_deref()
            .is_none_or(|pattern| wildcard_match(pattern, &origin.id))
            && self
                .signals
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == signal_id))
            && self.signal.as_deref().is_none_or(|id| id == signal_id)
            && self
                .principal_key
                .as_deref()
                .is_none_or(|key| key == row.principal_key)
            && self.attrs.iter().all(|(name, pattern)| {
                row.attrs
                    .get(name)
                    .is_some_and(|value| wildcard_match(pattern, value))
            })
    }

    fn is_empty(&self) -> bool {
        self.origin.is_none()
            && self.signals.is_none()
            && self.signal.is_none()
            && self.principal_key.is_none()
            && self.attrs.is_empty()
    }
}

fn split_param(key: &str) -> Option<(&str, &str)> {
    key.split_once('.')
        .filter(|(rule_id, param)| !rule_id.is_empty() && !param.is_empty())
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    let mut middle: Vec<&str> = parts.collect();
    let Some(last) = middle.pop() else {
        return rest.is_empty();
    };
    for part in middle {
        let Some(start) = rest.find(part) else {
            return false;
        };
        rest = &rest[start + part.len()..];
    }
    rest.ends_with(last)
}

fn sha256(text: &str) -> String {
    let hex: String = Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
}
