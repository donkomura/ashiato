use std::collections::{BTreeMap, BTreeSet};

use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};

use crate::rule::{Rule, RuleFile, Rules, collect_yaml_files, digest};

static BUILTIN_RULESETS: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../rulesets");

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
    pub fn parse(file: &RuleFile) -> Result<Self, RulesetError> {
        let mut ruleset: Ruleset =
            serde_yaml::from_str(&file.content).map_err(|source| RulesetError::Parse {
                path: file.path.clone(),
                source,
            })?;
        let mut seen = BTreeSet::new();
        for entry in &ruleset.rules {
            if !seen.insert(entry.id.as_str()) {
                return Err(RulesetError::DuplicateRule {
                    ruleset: ruleset.id.clone(),
                    rule: entry.id.clone(),
                });
            }
            ruleset.check_refs(entry)?;
        }
        ruleset.digest = digest(std::slice::from_ref(file));
        Ok(ruleset)
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    fn check_refs(&self, entry: &RulesetEntry) -> Result<(), RulesetError> {
        if entry.refs.is_empty() {
            return Ok(());
        }
        let Some(framework) = &self.framework else {
            return Err(RulesetError::RefsWithoutFramework {
                ruleset: self.id.clone(),
                rule: entry.id.clone(),
            });
        };
        match entry
            .refs
            .iter()
            .find(|reference| !framework.kind.accepts(reference))
        {
            Some(reference) => Err(RulesetError::InvalidRef {
                ruleset: self.id.clone(),
                rule: entry.id.clone(),
                reference: reference.clone(),
            }),
            None => Ok(()),
        }
    }

    fn contains(&self, rule_id: &str) -> bool {
        self.rules.iter().any(|entry| entry.id == rule_id)
    }
}

impl FrameworkKind {
    fn accepts(self, reference: &str) -> bool {
        match self {
            FrameworkKind::MitreAttack => is_attack_technique_id(reference),
        }
    }
}

fn is_attack_technique_id(reference: &str) -> bool {
    let Some(number) = reference.strip_prefix('T') else {
        return false;
    };
    let (technique, sub_technique) = match number.split_once('.') {
        Some((technique, sub_technique)) => (technique, Some(sub_technique)),
        None => (number, None),
    };
    let is_digits = |text: &str, len: usize| {
        text.len() == len && text.bytes().all(|byte| byte.is_ascii_digit())
    };
    is_digits(technique, 4) && sub_technique.is_none_or(|sub| is_digits(sub, 3))
}

#[derive(Debug, Clone, Default)]
pub struct Rulesets {
    sets: BTreeMap<String, Ruleset>,
}

impl Rulesets {
    pub fn parse(files: Vec<RuleFile>) -> Result<Self, RulesetError> {
        let mut sets = BTreeMap::new();
        for file in &files {
            let ruleset = Ruleset::parse(file)?;
            if sets.contains_key(&ruleset.id) {
                return Err(RulesetError::DuplicateRuleset {
                    ruleset: ruleset.id,
                });
            }
            sets.insert(ruleset.id.clone(), ruleset);
        }
        Ok(Self { sets })
    }

    pub fn builtin() -> Result<Self, RulesetError> {
        let mut files = Vec::new();
        collect_yaml_files(&BUILTIN_RULESETS, &mut files);
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Self::parse(files)
    }

    pub fn extend(&mut self, other: Rulesets) {
        self.sets.extend(other.sets);
    }

    pub fn add_to_default(&mut self, rule_ids: impl IntoIterator<Item = String>) {
        let default = self
            .sets
            .entry(DEFAULT_RULESET_ID.to_owned())
            .or_insert_with(|| Ruleset {
                id: DEFAULT_RULESET_ID.to_owned(),
                title: DEFAULT_RULESET_ID.to_owned(),
                framework: None,
                rules: Vec::new(),
                digest: digest(&[]),
            });
        for id in rule_ids {
            if !default.contains(&id) {
                default.rules.push(RulesetEntry {
                    id,
                    refs: Vec::new(),
                });
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<&Ruleset> {
        self.sets.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Ruleset> {
        self.sets.values()
    }

    pub fn check(&self, rules: &Rules) -> Result<(), RulesetError> {
        for set in self.sets.values() {
            for entry in &set.rules {
                let Some(rule) = rules.get(&entry.id) else {
                    return Err(RulesetError::UnknownRule {
                        ruleset: set.id.clone(),
                        rule: entry.id.clone(),
                    });
                };
                let Rule::Scenario(scenario) = rule else {
                    continue;
                };
                if let Some(step) = scenario
                    .steps
                    .iter()
                    .find(|step| !set.contains(&step.signal))
                {
                    return Err(RulesetError::MissingScenarioSignal {
                        ruleset: set.id.clone(),
                        scenario: scenario.id.clone(),
                        signal: step.signal.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn unlisted<'a>(&self, rules: &'a Rules) -> Vec<&'a str> {
        rules
            .iter()
            .map(Rule::id)
            .filter(|id| !self.sets.values().any(|set| set.contains(id)))
            .collect()
    }

    pub fn select(
        &self,
        rules: &Rules,
        rulesets: &[String],
        rule_ids: &[String],
    ) -> Result<Selection, RulesetError> {
        let default = [DEFAULT_RULESET_ID.to_owned()];
        let requested = if rulesets.is_empty() {
            &default[..]
        } else {
            rulesets
        };
        let mut selected_sets: Vec<&Ruleset> = Vec::new();
        for id in requested {
            let set = self
                .sets
                .get(id)
                .ok_or_else(|| RulesetError::UnknownRuleset {
                    ruleset: id.clone(),
                })?;
            if !selected_sets.iter().any(|selected| selected.id == set.id) {
                selected_sets.push(set);
            }
        }

        let available: BTreeSet<String> = selected_sets
            .iter()
            .flat_map(|set| set.rules.iter().map(|entry| entry.id.clone()))
            .collect();
        let selected = if rule_ids.is_empty() {
            available
        } else {
            let mut selected = BTreeSet::new();
            for id in rule_ids {
                if !available.contains(id) {
                    return Err(RulesetError::RuleNotSelected {
                        rule: id.clone(),
                        rulesets: selected_sets.iter().map(|set| set.id.clone()).collect(),
                    });
                }
                selected.insert(id.clone());
                if let Some(Rule::Scenario(scenario)) = rules.get(id) {
                    selected.extend(scenario.steps.iter().map(|step| step.signal.clone()));
                }
            }
            selected
        };

        let mut refs: BTreeMap<String, Vec<RuleRefs>> = BTreeMap::new();
        for set in &selected_sets {
            for entry in &set.rules {
                if entry.refs.is_empty() || !selected.contains(&entry.id) {
                    continue;
                }
                refs.entry(entry.id.clone()).or_default().push(RuleRefs {
                    ruleset: set.id.clone(),
                    ids: entry.refs.clone(),
                });
            }
        }
        Ok(Selection {
            rules: rules.subset(&selected),
            rulesets: selected_sets
                .iter()
                .map(|set| RulesetDigest {
                    id: set.id.clone(),
                    digest: set.digest.clone(),
                })
                .collect(),
            refs,
        })
    }
}
