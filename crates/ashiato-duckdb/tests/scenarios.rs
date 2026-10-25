use std::fs::File;
use std::io::Write;

use ashiato_core::analysis::{Analysis, analyze};
use ashiato_core::fixture::{builtin_fixtures, fixture_window};
use ashiato_core::lineage::{Origin, OriginKind};
use ashiato_core::rule::{RuleSet, Rules};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use tempfile::TempDir;

const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";

async fn analyze_builtin_fixture(path: &str) -> Analysis {
    let fixture = builtin_fixtures()
        .unwrap()
        .into_iter()
        .find(|fixture| fixture.path == path)
        .unwrap_or_else(|| panic!("no builtin fixture at {path}"));
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    let mut encoder = GzEncoder::new(
        File::create(source.join("fixture.json.gz")).unwrap(),
        Compression::default(),
    );
    encoder
        .write_all(json!({ "Records": fixture.records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
    let database = dir.path().join("ashiato.duckdb");
    EventStore::open(&database)
        .unwrap()
        .ingest_directory(&source)
        .unwrap();

    let backend = DuckDbBackend::open(&database).unwrap();
    let rules = Rules::new([RuleSet::builtin().unwrap()]).unwrap();
    analyze(&backend, &rules, &fixture_window()).await.unwrap()
}

fn step_signal_counts(analysis: &Analysis, scenario_id: &str) -> Vec<Vec<(String, usize)>> {
    analysis
        .matches
        .iter()
        .filter(|found| found.scenario_id == scenario_id)
        .map(|found| {
            found
                .steps
                .iter()
                .map(|step| (step.signal_id.clone(), step.signals.len()))
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn console_takeover_mining_links_the_console_session_to_the_iam_user() {
    let analysis = analyze_builtin_fixture(
        "console_takeover_mining/fixtures/positive/mining_from_cloudshell.json",
    )
    .await;

    let matches: Vec<_> = analysis
        .matches
        .iter()
        .filter(|found| found.scenario_id == "console_takeover_mining")
        .collect();
    assert_eq!(matches.len(), 1, "{:#?}", analysis.matches);
    assert_eq!(
        matches[0].origin,
        Origin {
            id: format!("iam_user:{ALICE_ARN}"),
            kind: OriginKind::IamUser,
        }
    );
    assert!(matches[0].lineage.is_empty(), "{:#?}", matches[0].lineage);
    let login = &matches[0].steps[0].signals[0];
    assert_eq!(login.access_key_id, None);
    let launch = &matches[0].steps[2].signals[0];
    assert_eq!(launch.access_key_id.as_deref(), Some("ASIACONSOLEALICE"));
}

#[tokio::test]
async fn console_takeover_mining_includes_the_optional_cloudshell_step_when_present() {
    let with_cloudshell = analyze_builtin_fixture(
        "console_takeover_mining/fixtures/positive/mining_from_cloudshell.json",
    )
    .await;
    let without_cloudshell = analyze_builtin_fixture(
        "console_takeover_mining/fixtures/positive/mining_without_cloudshell.json",
    )
    .await;

    let expected = |cloudshell: usize| {
        vec![vec![
            ("console_login_without_mfa".to_owned(), 1),
            ("cloudshell_activity".to_owned(), cloudshell),
            ("compute_resource_launch".to_owned(), 1),
        ]]
    };
    assert_eq!(
        step_signal_counts(&with_cloudshell, "console_takeover_mining"),
        expected(1)
    );
    assert_eq!(
        step_signal_counts(&without_cloudshell, "console_takeover_mining"),
        expected(0)
    );
}
