use std::fs;
use std::path::Path;
use std::process::Command;

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec, DatasetSummary, generate};
use serde_json::{Value, json};
use tempfile::TempDir;

// Small enough for cargo test, while every scenario and special file is still in the dataset.
const SCALE: f64 = 0.005;
const SEED: u64 = 31;
const SKIPPED_FOR_DATA_EVENTS: [&str; 3] = [
    "cross_account_s3_destruction",
    "s3_delete_burst",
    "s3_object_read_or_copy",
];

fn list(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|item| format!("\"{item}\"")).collect();
    format!("[{}]", quoted.join(", "))
}

fn write_plan(workspace: &Path, data: &Path, summary: &DatasetSummary) -> std::path::PathBuf {
    fs::create_dir_all(workspace).unwrap();
    fs::write(
        workspace.join("context.yaml"),
        format!("baseline:\n  lookback: {}\n", summary.lookback),
    )
    .unwrap();
    let plan = workspace.join("run.yaml");
    fs::write(
        &plan,
        format!(
            "run_id: dataset\n\
             source:\n  kind: local\n  uri: {}\n  accounts: {}\n  regions: {}\n\
             analysis_window:\n  since: {}\n  until: {}\n\
             rules: [builtin]\n\
             context: ./context.yaml\n\
             pipeline:\n  batch: {}\n  allowed_lateness: {}\n  max_skew: {}\n\
             placement:\n  mode: single_process\n",
            data.display(),
            list(&summary.accounts),
            list(&summary.regions),
            summary.since.to_rfc3339(),
            summary.until.to_rfc3339(),
            summary.batch,
            summary.allowed_lateness,
            summary.max_skew,
        ),
    )
    .unwrap();
    plan
}

fn run_dataset(kind: DatasetKind) -> (DatasetSummary, Vec<Value>) {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("data");
    let summary = generate(
        &DatasetSpec {
            kind,
            seed: SEED,
            scale: SCALE,
        },
        &data,
    )
    .unwrap();
    let plan = write_plan(&dir.path().join("workspace"), &data, &summary);

    let output = Command::new(env!("CARGO_BIN_EXE_ashiato"))
        .arg("run")
        .arg("--plan")
        .arg(&plan)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (summary, lines)
}

fn of_type<'a>(lines: &'a [Value], kind: &str) -> Vec<&'a Value> {
    lines.iter().filter(|line| line["type"] == kind).collect()
}

fn sorted(mut values: Vec<Value>) -> Vec<Value> {
    values.sort_by_key(|value| value.to_string());
    values
}

fn assert_matches_are_expected(summary: &DatasetSummary, lines: &[Value]) {
    let found = sorted(
        of_type(lines, "scenario_match")
            .into_iter()
            .map(|found| {
                json!({
                    "scenario_id": found["scenario_id"],
                    "origin": found["origin"],
                    "start_time": found["start_time"],
                    "end_time": found["end_time"],
                })
            })
            .collect(),
    );
    let expected = sorted(
        summary
            .expected_scenarios
            .iter()
            .map(|scenario| serde_json::to_value(scenario).unwrap())
            .collect(),
    );
    assert_eq!(found, expected);
}

fn missing_data_rules(lines: &[Value]) -> Vec<&str> {
    let mut ids: Vec<&str> = of_type(lines, "rule_skipped")
        .into_iter()
        .filter(|skipped| skipped["reason"] == "missing_data")
        .map(|skipped| skipped["rule_id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

fn assert_ingested_everything(summary: &DatasetSummary, run: &Value) {
    assert_eq!(run["type"], "run", "{run}");
    let file_errors = run["file_errors"].as_u64().unwrap();
    assert!(file_errors > 0, "{run}");
    assert_eq!(
        run["ingested_files"].as_u64().unwrap() + file_errors,
        summary.files,
        "{run}"
    );
    assert!(run["late_events"].as_u64().unwrap() > 0, "{run}");
}

#[test]
fn run_finds_the_embedded_scenarios_of_dataset_a() {
    let (summary, lines) = run_dataset(DatasetKind::A);

    assert_ingested_everything(&summary, &lines[0]);
    assert_eq!(lines[0]["data"]["s3_data_events"], true, "{}", lines[0]);
    assert!(missing_data_rules(&lines).is_empty(), "{lines:#?}");
    assert_eq!(summary.expected_scenarios.len(), 4);
    assert_matches_are_expected(&summary, &lines);
}

#[test]
fn run_skips_the_rules_that_need_s3_data_events_on_dataset_a_without_data_events() {
    let (summary, lines) = run_dataset(DatasetKind::ANoDataEvents);

    assert_ingested_everything(&summary, &lines[0]);
    assert_eq!(lines[0]["data"]["s3_data_events"], false, "{}", lines[0]);
    let skipped = missing_data_rules(&lines);
    for id in SKIPPED_FOR_DATA_EVENTS {
        assert!(skipped.contains(&id), "{id}: {skipped:?}");
    }
    assert_eq!(summary.expected_scenarios.len(), 3);
    assert_matches_are_expected(&summary, &lines);
}
