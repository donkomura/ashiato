mod common;

use ashiato_core::analysis::correlate;
use ashiato_core::fixture::fixture_window;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore};
use ashiato_testkit::fixture::{Expectation, Fixture, builtin_fixtures};
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, start_worker, write_records};

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn signal_rules(rules: &Rules) -> Vec<SignalRule> {
    rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .collect()
}

// Runs a rule fixture through the pipeline and counts what the rule detected: the signals of
// a signal rule, or the matches of a scenario.
async fn streamed_detections(rules: &Rules, fixture: &Fixture) -> usize {
    let window = fixture_window();
    let workspace = Workspace::new();
    write_records(&workspace, fixture.records.clone(), &window);
    let manifest = workspace.manifest();
    let settings = AnalyzerSettings {
        batch: TimeDelta::hours(1),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: window.baseline_since(),
        fetch_max: 16,
        commit_every: 16,
    };
    let mut analyzer = Analyzer::open(
        PipelineStore::open(&workspace.database("pipeline")).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
    .with_queries(signal_rules(rules), window);
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
    analyzer.run(&mut reader).await.unwrap();
    worker.await.unwrap().unwrap();
    let store = analyzer.into_store();
    let observed = store.observed_signals().unwrap();
    match rules.get(&fixture.rule_id) {
        Some(Rule::Signal(rule)) => observed
            .iter()
            .filter(|signal| signal.signal_id == rule.id)
            .count(),
        Some(Rule::Scenario(scenario)) => {
            correlate(rules, observed, store.lineage_edges().unwrap())
                .matches
                .iter()
                .filter(|found| found.scenario_id == scenario.id)
                .count()
        }
        None => panic!("fixture {} refers to an unknown rule", fixture.path),
    }
}

// The builtin fixtures are written for the batch processing. The pipeline must reach the same
// verdict on each of them.
async fn assert_pipeline_verdicts(rule_ids: &[&str]) {
    let rules = builtin_rules();
    let fixtures: Vec<Fixture> = builtin_fixtures()
        .unwrap()
        .into_iter()
        .filter(|fixture| rule_ids.contains(&fixture.rule_id.as_str()))
        .collect();
    assert!(!fixtures.is_empty(), "no fixtures for {rule_ids:?}");
    let mut failures = Vec::new();
    for fixture in &fixtures {
        let detections = streamed_detections(&rules, fixture).await;
        let passed = match fixture.expectation {
            Expectation::Positive => detections > 0,
            Expectation::Negative => detections == 0,
        };
        if !passed {
            failures.push(format!("{} ({detections} detections)", fixture.path));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn console_takeover_mining_gives_the_same_verdicts_in_the_pipeline() {
    assert_pipeline_verdicts(&[
        "console_takeover_mining",
        "console_login_without_mfa",
        "cloudshell_activity",
        "compute_resource_launch",
    ])
    .await;
}
