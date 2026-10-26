use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::rule::{RuleFile, Rules};

pub const DEFAULT_RULESET_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ruleset {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub framework: Option<Framework>,
    pub rules: Vec<RulesetEntry>,
    #[serde(skip)]
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Framework {
    pub kind: FrameworkKind,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameworkKind {
    MitreAttack,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RulesetEntry {
    pub id: String,
    #[serde(default)]
    pub refs: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RulesetError {
    #[error("failed to parse ruleset file {path}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("ruleset {ruleset} is defined more than once")]
    DuplicateRuleset { ruleset: String },
    #[error("ruleset {ruleset} lists rule {rule} more than once")]
    DuplicateRule { ruleset: String, rule: String },
    #[error("ruleset {ruleset} gives refs to rule {rule}, but declares no framework")]
    RefsWithoutFramework { ruleset: String, rule: String },
    #[error(
        "ruleset {ruleset} gives rule {rule} the ref {reference}, which is not valid for its framework"
    )]
    InvalidRef {
        ruleset: String,
        rule: String,
        reference: String,
    },
    #[error("ruleset {ruleset} lists rule {rule}, which does not exist")]
    UnknownRule { ruleset: String, rule: String },
    #[error(
        "ruleset {ruleset} lists scenario {scenario}, but not signal {signal} that it refers to"
    )]
    MissingScenarioSignal {
        ruleset: String,
        scenario: String,
        signal: String,
    },
    #[error("ruleset {ruleset} does not exist")]
    UnknownRuleset { ruleset: String },
    #[error("rule {rule} is not in the selected rulesets {}", rulesets.join(", "))]
    RuleNotSelected { rule: String, rulesets: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RulesetDigest {
    pub id: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuleRefs {
    pub ruleset: String,
    pub ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub rules: Rules,
    pub rulesets: Vec<RulesetDigest>,
    pub refs: BTreeMap<String, Vec<RuleRefs>>,
}

impl Ruleset {
    pub fn parse(_file: &RuleFile) -> Result<Self, RulesetError> {
        todo!()
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }
}

#[derive(Debug, Clone, Default)]
pub struct Rulesets {
    sets: BTreeMap<String, Ruleset>,
}

impl Rulesets {
    pub fn parse(_files: Vec<RuleFile>) -> Result<Self, RulesetError> {
        todo!()
    }

    pub fn builtin() -> Result<Self, RulesetError> {
        todo!()
    }

    pub fn extend(&mut self, _other: Rulesets) {
        todo!()
    }

    pub fn add_to_default(&mut self, _rule_ids: impl IntoIterator<Item = String>) {
        todo!()
    }

    pub fn get(&self, id: &str) -> Option<&Ruleset> {
        self.sets.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Ruleset> {
        self.sets.values()
    }

    pub fn check(&self, _rules: &Rules) -> Result<(), RulesetError> {
        todo!()
    }

    pub fn unlisted<'a>(&self, _rules: &'a Rules) -> Vec<&'a str> {
        todo!()
    }

    pub fn select(
        &self,
        _rules: &Rules,
        _rulesets: &[String],
        _rule_ids: &[String],
    ) -> Result<Selection, RulesetError> {
        todo!()
    }
}
