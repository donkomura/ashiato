use chrono::{DateTime, Utc};

use crate::SignalRow;
use crate::rule::ScenarioRule;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedSignal {
    pub signal_id: String,
    pub row: SignalRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioMatch<'a> {
    pub steps: Vec<StepMatch<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepMatch<'a> {
    pub signal_id: &'a str,
    pub signals: Vec<&'a ObservedSignal>,
}

impl ScenarioMatch<'_> {
    pub fn start_time(&self) -> DateTime<Utc> {
        todo!()
    }

    pub fn end_time(&self) -> DateTime<Utc> {
        todo!()
    }
}

pub fn match_scenario<'a>(
    scenario: &'a ScenarioRule,
    signals: &'a [ObservedSignal],
) -> Vec<ScenarioMatch<'a>> {
    let _ = (scenario, signals);
    todo!()
}
