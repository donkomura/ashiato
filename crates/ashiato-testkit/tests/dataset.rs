use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;

use ashiato_testkit::dataset::{DatasetError, DatasetKind, DatasetSpec, DatasetSummary, generate};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use flate2::read::GzDecoder;
use serde_json::Value;
use tempfile::TempDir;

const SCALE: f64 = 0.005;
const SEED: u64 = 31;

fn spec(kind: DatasetKind, seed: u64, scale: f64) -> DatasetSpec {
    DatasetSpec { kind, seed, scale }
}

fn generated(spec: &DatasetSpec) -> (TempDir, DatasetSummary) {
    let dir = TempDir::new().unwrap();
    let summary = generate(spec, dir.path()).unwrap();
    (dir, summary)
}

fn all_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap().to_str().unwrap();
                files.insert(relative.to_owned(), fs::read(&path).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files);
    files
}

fn log_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    all_files(root)
        .into_iter()
        .filter(|(path, _)| path.starts_with("AWSLogs/"))
        .collect()
}

fn records(content: &[u8]) -> Option<Vec<Value>> {
    let mut text = String::new();
    GzDecoder::new(content).read_to_string(&mut text).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value["Records"].as_array().cloned()
}

struct LogPath {
    org_id: Option<String>,
    account: String,
    region: String,
    file_time: DateTime<Utc>,
}

// AWSLogs/[<org-id>/]<account>/CloudTrail/<region>/YYYY/MM/DD/
// <account>_CloudTrail_<region>_<YYYYMMDDTHHmmZ>_<unique>.json.gz
fn parse_log_path(path: &str) -> LogPath {
    let parts: Vec<&str> = path.split('/').collect();
    let (org_id, rest) = match parts.as_slice() {
        ["AWSLogs", org, rest @ ..] if org.starts_with("o-") => (Some((*org).to_owned()), rest),
        ["AWSLogs", rest @ ..] => (None, rest),
        _ => panic!("{path} is not under AWSLogs"),
    };
    let [account, "CloudTrail", region, year, month, day, name] = rest else {
        panic!("{path} does not follow the CloudTrail key layout");
    };
    let stem = name
        .strip_suffix(".json.gz")
        .unwrap_or_else(|| panic!("{path} is not a .json.gz file"));
    let fields: Vec<&str> = stem.splitn(5, '_').collect();
    let [name_account, "CloudTrail", name_region, time, unique] = fields.as_slice() else {
        panic!("{path} does not follow the CloudTrail file name");
    };
    assert_eq!(name_account, account, "{path}");
    assert_eq!(name_region, region, "{path}");
    assert!(!unique.is_empty(), "{path}");
    let file_time = NaiveDateTime::parse_from_str(time, "%Y%m%dT%H%MZ")
        .unwrap_or_else(|error| panic!("{path}: {error}"))
        .and_utc();
    assert_eq!(
        format!("{year}/{month}/{day}"),
        file_time.format("%Y/%m/%d").to_string(),
        "{path}"
    );
    LogPath {
        org_id,
        account: (*account).to_owned(),
        region: (*region).to_owned(),
        file_time,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Counts {
    files: u64,
    compressed_bytes: u64,
    events: u64,
}

fn count(files: &BTreeMap<String, Vec<u8>>) -> (Counts, BTreeMap<(String, String), Counts>) {
    let mut total = Counts::default();
    let mut partitions: BTreeMap<(String, String), Counts> = BTreeMap::new();
    for (path, content) in files {
        let log = parse_log_path(path);
        let events = records(content).map_or(0, |records| records.len() as u64);
        for counts in [
            &mut total,
            partitions.entry((log.account, log.region)).or_default(),
        ] {
            counts.files += 1;
            counts.compressed_bytes += content.len() as u64;
            counts.events += events;
        }
    }
    (total, partitions)
}

fn event_ids(files: &BTreeMap<String, Vec<u8>>) -> BTreeSet<String> {
    files
        .values()
        .filter_map(|content| records(content))
        .flatten()
        .map(|record| record["eventID"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn the_same_spec_writes_the_same_files_and_another_seed_writes_other_events() {
    let (first, first_summary) = generated(&spec(DatasetKind::A, SEED, SCALE));
    let (second, second_summary) = generated(&spec(DatasetKind::A, SEED, SCALE));
    let (other, _) = generated(&spec(DatasetKind::A, SEED + 1, SCALE));

    assert_eq!(first_summary, second_summary);
    let first_files = all_files(first.path());
    assert!(first_files.len() > 1);
    assert!(first_files == all_files(second.path()));
    let other_files = log_files(other.path());
    assert!(log_files(first.path()) != other_files);
    let first_ids = event_ids(&first_files);
    let other_ids = event_ids(&other_files);
    assert!(!first_ids.is_empty());
    assert!(first_ids.is_disjoint(&other_ids));
}

#[test]
fn the_summary_counts_the_written_files_bytes_and_events() {
    for kind in [DatasetKind::A, DatasetKind::ANoDataEvents, DatasetKind::B] {
        let (dir, summary) = generated(&spec(kind, SEED, SCALE));

        let written: DatasetSummary =
            serde_json::from_slice(&fs::read(dir.path().join("dataset.json")).unwrap()).unwrap();
        assert_eq!(written, summary, "{kind:?}");
        assert_eq!(summary.kind, kind);
        assert_eq!(summary.seed, SEED);
        assert_eq!(summary.scale, SCALE);
        let (total, partitions) = count(&log_files(dir.path()));
        assert_eq!(
            total,
            Counts {
                files: summary.files,
                compressed_bytes: summary.compressed_bytes,
                events: summary.events,
            },
            "{kind:?}"
        );
        for partition in &summary.partitions {
            let counts = partitions
                .get(&(partition.account.clone(), partition.region.clone()))
                .cloned()
                .unwrap_or_default();
            let expected = Counts {
                files: partition.files,
                compressed_bytes: partition.compressed_bytes,
                events: partition.events,
            };
            assert_eq!(
                counts, expected,
                "{kind:?} {}/{}",
                partition.account, partition.region
            );
        }
        assert_eq!(
            partitions.len(),
            summary
                .partitions
                .iter()
                .filter(|partition| partition.files > 0)
                .count()
        );
    }
}

#[test]
fn log_files_follow_the_cloudtrail_key_layout_within_the_period() {
    for kind in [DatasetKind::A, DatasetKind::B] {
        let (dir, summary) = generated(&spec(kind, SEED, SCALE));

        let top: BTreeSet<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            top,
            BTreeSet::from(["AWSLogs".to_owned(), "dataset.json".to_owned()])
        );
        for path in log_files(dir.path()).keys() {
            let log = parse_log_path(path);
            if let Some(org_id) = &log.org_id {
                assert_eq!(org_id, &summary.org_id, "{path}");
            }
            assert!(summary.accounts.contains(&log.account), "{path}");
            assert!(summary.regions.contains(&log.region), "{path}");
            assert!(
                summary.baseline_since <= log.file_time && log.file_time < summary.until,
                "{path}"
            );
        }
    }
}

#[test]
fn the_periods_are_a_week_of_baseline_and_a_week_of_analysis() {
    let (_dir, summary) = generated(&spec(DatasetKind::A, SEED, SCALE));

    assert_eq!(summary.since - summary.baseline_since, TimeDelta::days(7));
    assert_eq!(summary.until - summary.since, TimeDelta::days(7));
    assert_eq!(summary.lookback, "7d");
    for duration in [
        &summary.lookback,
        &summary.batch,
        &summary.allowed_lateness,
        &summary.max_skew,
    ] {
        duration.parse::<ashiato_core::Duration>().unwrap();
    }
}

#[test]
fn dataset_a_has_an_empty_partition_and_a_partition_with_ten_times_the_events() {
    let (dir, summary) = generated(&spec(DatasetKind::A, SEED, SCALE));

    assert_eq!(summary.accounts.len(), 4);
    assert_eq!(summary.regions.len(), 3);
    assert_eq!(summary.partitions.len(), 12);
    let empty: Vec<_> = summary
        .partitions
        .iter()
        .filter(|partition| partition.files == 0)
        .collect();
    assert_eq!(empty.len(), 1, "{:#?}", summary.partitions);
    assert_eq!(empty[0].events, 0);
    let (_, written) = count(&log_files(dir.path()));
    assert_eq!(written.len(), 11);
    assert!(!written.contains_key(&(empty[0].account.clone(), empty[0].region.clone())));
    let mut events: Vec<u64> = summary
        .partitions
        .iter()
        .filter(|partition| partition.files > 0)
        .map(|partition| partition.events)
        .collect();
    events.sort_unstable();
    let median = events[events.len() / 2];
    let largest = events[events.len() - 1];
    assert!(largest >= 9 * median, "{events:?}");
    assert!(events[events.len() - 2] < 2 * median, "{events:?}");
}

#[test]
fn dataset_b_has_24_partitions_with_the_same_files_and_events() {
    let (_dir, summary) = generated(&spec(DatasetKind::B, SEED, SCALE));

    assert_eq!(summary.accounts.len(), 4);
    assert_eq!(summary.regions.len(), 6);
    assert_eq!(summary.partitions.len(), 24);
    let first = &summary.partitions[0];
    assert!(first.events > 0);
    for partition in &summary.partitions {
        assert_eq!(partition.files, first.files, "{partition:?}");
        assert_eq!(partition.events, first.events, "{partition:?}");
    }
}

#[test]
fn a_smaller_scale_writes_fewer_files_and_events() {
    let (_large_dir, large) = generated(&spec(DatasetKind::B, SEED, 2.0 * SCALE));
    let (_small_dir, small) = generated(&spec(DatasetKind::B, SEED, SCALE));

    assert!(small.files < large.files);
    assert!(small.events < large.events);
}

#[test]
fn generate_rejects_a_scale_that_is_not_positive() {
    for scale in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let dir = TempDir::new().unwrap();

        let result = generate(&spec(DatasetKind::A, SEED, scale), dir.path());

        assert!(
            matches!(result, Err(DatasetError::InvalidScale(_))),
            "{scale}: {result:?}"
        );
    }
}

#[test]
fn generate_rejects_a_directory_that_is_not_empty() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("other"), b"other").unwrap();

    let result = generate(&spec(DatasetKind::A, SEED, SCALE), dir.path());

    assert!(
        matches!(result, Err(DatasetError::NotEmpty(_))),
        "{result:?}"
    );
}
