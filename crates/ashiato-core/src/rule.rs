use std::collections::BTreeMap;

use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Duration, Severity, Tactic};

pub(crate) static BUILTIN_RULES: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../rules");

const BUILTIN_RULE_SET_ID: &str = "builtin";

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
    pub window: Duration,
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
    pub max_gap: Option<Duration>,
    #[serde(default)]
    pub same: Vec<String>,
}

fn default_min_count() -> u32 {
    1
}

impl Rule {
    pub fn id(&self) -> &str {
        match self {
            Rule::Signal(rule) => &rule.id,
            Rule::Scenario(rule) => &rule.id,
        }
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
    pub fn parse(id: impl Into<String>, mut files: Vec<RuleFile>) -> Result<Self, RuleError> {
        let id = id.into();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let mut rules: Vec<Rule> = Vec::with_capacity(files.len());
        for file in &files {
            let rule: Rule =
                serde_yaml::from_str(&file.content).map_err(|source| RuleError::Parse {
                    path: file.path.clone(),
                    source,
                })?;
            if rules.iter().any(|existing| existing.id() == rule.id()) {
                return Err(RuleError::DuplicateId {
                    id: rule.id().to_owned(),
                    ruleset: id,
                });
            }
            rules.push(rule);
        }
        Ok(Self {
            id,
            digest: digest(&files),
            rules,
        })
    }

    pub fn builtin() -> Result<Self, RuleError> {
        let mut files = Vec::new();
        collect_builtin_rule_files(&BUILTIN_RULES, &mut files);
        Self::parse(BUILTIN_RULE_SET_ID, files)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    pub fn new(sets: impl IntoIterator<Item = RuleSet>) -> Result<Self, RuleError> {
        let mut rules = BTreeMap::new();
        let mut rulesets = Vec::new();
        for set in sets {
            rulesets.push(RuleSetDigest {
                id: set.id,
                digest: set.digest,
            });
            for rule in set.rules {
                rules.insert(rule.id().to_owned(), rule);
            }
        }
        let rules = Self { rules, rulesets };
        rules.check_scenario_references()?;
        Ok(rules)
    }

    fn check_scenario_references(&self) -> Result<(), RuleError> {
        for rule in self.rules.values() {
            let Rule::Scenario(scenario) = rule else {
                continue;
            };
            for step in &scenario.steps {
                let Some(Rule::Signal(signal)) = self.rules.get(&step.signal) else {
                    return Err(RuleError::UnknownSignal {
                        scenario: scenario.id.clone(),
                        signal: step.signal.clone(),
                    });
                };
                let used_attrs = step.same.iter().chain(step.distinct_by.iter());
                for attr in used_attrs {
                    if !signal.attrs.contains(attr) {
                        return Err(RuleError::UndeclaredAttribute {
                            scenario: scenario.id.clone(),
                            signal: signal.id.clone(),
                            attr: attr.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
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

fn collect_builtin_rule_files(dir: &Dir<'_>, files: &mut Vec<RuleFile>) {
    for file in dir.files() {
        let path = file.path().to_string_lossy();
        if !path.ends_with(".yaml") {
            continue;
        }
        if let Some(content) = file.contents_utf8() {
            files.push(RuleFile {
                path: path.into_owned(),
                content: content.to_owned(),
            });
        }
    }
    for child in dir.dirs() {
        collect_builtin_rule_files(child, files);
    }
}

fn digest(sorted_files: &[RuleFile]) -> String {
    let mut hasher = Sha256::new();
    for file in sorted_files {
        for part in [&file.path, &file.content] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
    }
    let hex: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
}
