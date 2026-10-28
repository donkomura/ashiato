use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::context::Scoring;
use crate::lineage::Origin;
use crate::output::{OriginScoreRecord, SignalRecord};
use crate::{Severity, Tactic};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScoringSettings {
    pub min_tactics: usize,
    pub threshold: u32,
}

impl From<&Scoring> for ScoringSettings {
    fn from(scoring: &Scoring) -> Self {
        Self {
            min_tactics: scoring.min_tactics,
            threshold: scoring.threshold,
        }
    }
}

impl Default for ScoringSettings {
    fn default() -> Self {
        Self {
            min_tactics: 3,
            threshold: 10,
        }
    }
}

pub fn score(signals: &[SignalRecord], settings: ScoringSettings) -> Vec<OriginScoreRecord> {
    let mut origins: BTreeMap<&str, OriginSignals<'_>> = BTreeMap::new();
    for signal in signals {
        let found = origins
            .entry(&signal.origin.id)
            .or_insert_with(|| OriginSignals {
                origin: &signal.origin,
                severities: BTreeMap::new(),
                tactics: BTreeSet::new(),
            });
        found
            .severities
            .entry(&signal.signal_id)
            .or_insert(signal.severity);
        found.tactics.extend(signal.tactics.iter().copied());
    }
    let mut scores: Vec<OriginScoreRecord> = origins
        .into_values()
        .map(|found| found.into_record(settings))
        .collect();
    scores.sort_by_key(|found| Reverse(found.score));
    scores
}

struct OriginSignals<'a> {
    origin: &'a Origin,
    severities: BTreeMap<&'a str, Severity>,
    tactics: BTreeSet<Tactic>,
}

impl OriginSignals<'_> {
    fn into_record(self, settings: ScoringSettings) -> OriginScoreRecord {
        let score = self.severities.values().copied().map(points).sum();
        OriginScoreRecord {
            origin: self.origin.clone(),
            score,
            above_threshold: self.tactics.len() >= settings.min_tactics
                && score >= settings.threshold,
            tactics: self.tactics.into_iter().collect(),
            signal_ids: self.severities.into_keys().map(str::to_owned).collect(),
        }
    }
}

fn points(severity: Severity) -> u32 {
    match severity {
        Severity::Low => 1,
        Severity::Medium => 3,
        Severity::High => 5,
        Severity::Critical => 8,
    }
}
