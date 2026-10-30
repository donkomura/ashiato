use std::path::Path;

use ashiato_testkit::dataset::ExpectedScenario;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioComparison {
    pub missing: Vec<ExpectedScenario>,
    pub unexpected: Vec<ExpectedScenario>,
}

impl ScenarioComparison {
    pub fn matches(&self) -> bool {
        todo!()
    }
}

pub fn compare_scenarios(
    output: &[u8],
    expected: &[ExpectedScenario],
) -> Result<ScenarioComparison, serde_json::Error> {
    let _ = (output, expected);
    todo!()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleDifference {
    pub rule_id: String,
    pub only_in_reference: u64,
    pub only_in_batch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalComparison {
    pub batch_signals: u64,
    pub differences: Vec<RuleDifference>,
}

impl SignalComparison {
    pub fn matches(&self) -> bool {
        todo!()
    }
}

pub fn compare_signals(
    reference: &Path,
    dataset: &Path,
    work_dir: &Path,
) -> anyhow::Result<SignalComparison> {
    let _ = (reference, dataset, work_dir);
    todo!()
}
