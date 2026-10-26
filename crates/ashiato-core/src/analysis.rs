use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::lineage::{LineageGraph, LineageStep, LineageWarning, Origin};
use crate::output::{
    RecordKind, ScenarioMatchRecord, SignalEvidence, SignalRecord, StepRecord, SummaryRecord,
};
use crate::query::{BackendError, QueryBackend, SignalQueryError, query_signal_rule};
use crate::rule::{Rule, Rules, ScenarioRule, SignalRule};
use crate::ruleset::RuleRefs;
use crate::sequence::{ObservedSignal, ScenarioMatch, match_scenario};
use crate::{AnalysisWindow, Severity};

#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    pub signals: Vec<SignalRecord>,
    pub matches: Vec<ScenarioMatchRecord>,
    pub warnings: Vec<LineageWarning>,
}

impl Analysis {
    pub fn retain_severities(&mut self, severities: &[Severity]) {
        self.signals
            .retain(|signal| severities.contains(&signal.severity));
        self.matches
            .retain(|found| severities.contains(&found.severity));
    }

    pub fn attach_refs(&mut self, refs: &BTreeMap<String, Vec<RuleRefs>>) {
        for signal in &mut self.signals {
            signal.refs = refs.get(&signal.signal_id).cloned().unwrap_or_default();
        }
        for found in &mut self.matches {
            found.refs = refs.get(&found.scenario_id).cloned().unwrap_or_default();
        }
    }

    pub fn summarize(&self) -> Vec<SummaryRecord> {
        let mut groups: BTreeMap<(RecordKind, &str, &str), SummaryGroup<'_>> = BTreeMap::new();
        for signal in &self.signals {
            groups
                .entry((RecordKind::Signal, &signal.signal_id, &signal.origin.id))
                .or_insert_with(|| SummaryGroup::new(signal.severity, &signal.origin))
                .add([&signal.evidence]);
        }
        for found in &self.matches {
            groups
                .entry((
                    RecordKind::ScenarioMatch,
                    &found.scenario_id,
                    &found.origin.id,
                ))
                .or_insert_with(|| SummaryGroup::new(found.severity, &found.origin))
                .add(found.steps.iter().flat_map(|step| &step.signals));
        }
        let mut summaries: Vec<SummaryRecord> = groups
            .into_iter()
            .map(|((kind, rule_id, _), group)| group.into_record(kind, rule_id))
            .collect();
        summaries.sort_by_key(|summary| (Reverse(summary.severity), Reverse(summary.count)));
        summaries
    }
}

struct SummaryGroup<'a> {
    severity: Severity,
    origin: &'a Origin,
    count: usize,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    source_ips: BTreeSet<&'a str>,
    events: BTreeSet<&'a str>,
}

impl<'a> SummaryGroup<'a> {
    fn new(severity: Severity, origin: &'a Origin) -> Self {
        Self {
            severity,
            origin,
            count: 0,
            first_seen: None,
            last_seen: None,
            source_ips: BTreeSet::new(),
            events: BTreeSet::new(),
        }
    }

    fn add(&mut self, evidence: impl IntoIterator<Item = &'a SignalEvidence>) {
        self.count += 1;
        for evidence in evidence {
            self.first_seen = Some(
                self.first_seen
                    .map_or(evidence.start_time, |seen| seen.min(evidence.start_time)),
            );
            self.last_seen = self.last_seen.max(Some(evidence.end_time));
            self.source_ips.insert(&evidence.source_ip);
            self.events
                .extend(evidence.event_ids.iter().map(String::as_str));
        }
    }

    fn into_record(self, kind: RecordKind, rule_id: &str) -> SummaryRecord {
        SummaryRecord {
            kind,
            rule_id: rule_id.to_owned(),
            severity: self.severity,
            origin: self.origin.clone(),
            count: self.count,
            first_seen: self
                .first_seen
                .expect("a summary group has at least one signal"),
            last_seen: self
                .last_seen
                .expect("a summary group has at least one signal"),
            source_ips: self.source_ips.len(),
            events: self.events.len(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AnalysisError {
    #[error(transparent)]
    Signal(#[from] SignalQueryError),
    #[error("failed to query the session lineage")]
    Lineage(#[source] BackendError),
}

struct OriginSignals {
    origin: Origin,
    signals: Vec<ObservedSignal>,
}

pub async fn analyze<B: QueryBackend + ?Sized>(
    backend: &B,
    rules: &Rules,
    window: &AnalysisWindow,
) -> Result<Analysis, AnalysisError> {
    let mut observed = Vec::new();
    for rule in signal_rules(rules) {
        for row in query_signal_rule(backend, rule, window).await? {
            observed.push(ObservedSignal {
                signal_id: rule.id.clone(),
                row,
            });
        }
    }
    let edges = backend
        .query_lineage(window)
        .await
        .map_err(AnalysisError::Lineage)?;
    let graph = LineageGraph::new(edges);

    observed.sort_by_key(|signal| signal.row.start_time);
    let mut origins: BTreeMap<String, OriginSignals> = BTreeMap::new();
    let mut warnings = Vec::new();
    for signal in observed {
        let resolution = graph.resolve(
            signal.row.access_key_id.as_deref(),
            &signal.row.principal_key,
        );
        if let Some(warning) = resolution.warning
            && !warnings.contains(&warning)
        {
            warnings.push(warning);
        }
        origins
            .entry(resolution.origin.id.clone())
            .or_insert_with(|| OriginSignals {
                origin: resolution.origin,
                signals: Vec::new(),
            })
            .signals
            .push(signal);
    }

    let mut analysis = Analysis {
        signals: Vec::new(),
        matches: Vec::new(),
        warnings,
    };
    for OriginSignals { origin, signals } in origins.into_values() {
        for scenario in scenario_rules(rules) {
            for found in match_scenario(scenario, &signals) {
                analysis
                    .matches
                    .push(scenario_match_record(scenario, &origin, &found, &graph));
            }
        }
        for signal in signals {
            analysis.signals.push(signal_record(rules, &origin, signal));
        }
    }
    Ok(analysis)
}

fn signal_rules(rules: &Rules) -> impl Iterator<Item = &SignalRule> {
    rules.iter().filter_map(|rule| match rule {
        Rule::Signal(rule) => Some(rule),
        Rule::Scenario(_) => None,
    })
}

fn scenario_rules(rules: &Rules) -> impl Iterator<Item = &ScenarioRule> {
    rules.iter().filter_map(|rule| match rule {
        Rule::Scenario(rule) => Some(rule),
        Rule::Signal(_) => None,
    })
}

fn signal_record(rules: &Rules, origin: &Origin, signal: ObservedSignal) -> SignalRecord {
    let Some(Rule::Signal(rule)) = rules.get(&signal.signal_id) else {
        unreachable!("observed signals are produced by the signal rules of the same rule sources")
    };
    SignalRecord {
        signal_id: signal.signal_id,
        severity: rule.severity,
        tactics: rule.tactics.clone(),
        origin: origin.clone(),
        evidence: SignalEvidence::from(&signal.row),
        principal_key: signal.row.principal_key,
        account_id: signal.row.account_id,
        region: signal.row.region,
        refs: Vec::new(),
    }
}

fn scenario_match_record(
    scenario: &ScenarioRule,
    origin: &Origin,
    found: &ScenarioMatch<'_>,
    graph: &LineageGraph,
) -> ScenarioMatchRecord {
    let mut lineage: Vec<LineageStep> = Vec::new();
    let steps = found
        .steps
        .iter()
        .map(|step| StepRecord {
            signal_id: step.signal_id.to_owned(),
            signals: step
                .signals
                .iter()
                .map(|signal| {
                    let resolution = graph.resolve(
                        signal.row.access_key_id.as_deref(),
                        &signal.row.principal_key,
                    );
                    for session in resolution.lineage {
                        if !lineage.contains(&session) {
                            lineage.push(session);
                        }
                    }
                    SignalEvidence::from(&signal.row)
                })
                .collect(),
        })
        .collect();
    ScenarioMatchRecord {
        scenario_id: scenario.id.clone(),
        severity: scenario.severity,
        origin: origin.clone(),
        start_time: found.start_time(),
        end_time: found.end_time(),
        steps,
        lineage,
        refs: Vec::new(),
    }
}
