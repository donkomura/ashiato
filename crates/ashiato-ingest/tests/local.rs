use std::fs;
use std::path::{Path, PathBuf};

use ashiato_ingest::{ListError, ListScope, list_local};
use ashiato_pipeline::{EventTime, ManifestEntry, PartitionId, PartitionManifest, RunPlan};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use chrono::{TimeDelta, TimeZone, Utc};

fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

fn whole_october() -> ListScope<'static> {
    ListScope {
        accounts: None,
        regions: None,
        period: at(1, 0, 0)..at(31, 0, 0),
    }
}

fn log(account: &str, region: &str, time: EventTime, unique: &str) -> LogFile {
    LogFile {
        unique: unique.to_owned(),
        ..LogFile::new(account, region, time)
    }
}

fn org_log(account: &str, region: &str, time: EventTime, unique: &str) -> LogFile {
    LogFile {
        org_id: Some("o-abcdef1234".to_owned()),
        ..log(account, region, time, unique)
    }
}

fn entry(file_seq: u64, path: &Path, file_time: EventTime) -> ManifestEntry {
    ManifestEntry {
        file_seq,
        uri: path.to_str().unwrap().to_owned(),
        version_id: None,
        etag: None,
        size: fs::metadata(path).unwrap().len(),
        file_time,
    }
}

fn partition(account: &str, region: &str, files: Vec<ManifestEntry>) -> PartitionManifest {
    PartitionManifest {
        partition: PartitionId::new(account, region),
        files,
    }
}

fn write_other_file(root: &Path, relative: &str) -> PathBuf {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"not a cloudtrail log").unwrap();
    path
}

#[test]
fn files_are_ordered_by_file_time_then_file_name_without_digest_and_insight() {
    let root = tempfile::tempdir().unwrap();
    let late_b = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 5), "b"),
    );
    let early = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 0), "z"),
    );
    let late_a = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 5), "a"),
    );
    let next_day = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(2, 0, 0), "a"),
    );
    write_other_file(
        root.path(),
        "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/\
         111111111111_CloudTrail-Digest_us-east-1_trail_us-east-1_20261001T1000Z.json.gz",
    );
    write_other_file(
        root.path(),
        "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/\
         111111111111_CloudTrail_us-east-1_20261001T1001Z_digest.json.gz",
    );
    write_other_file(
        root.path(),
        "AWSLogs/111111111111/CloudTrail-Insight/us-east-1/2026/10/01/\
         111111111111_CloudTrail-Insight_us-east-1_20261001T1000Z_x.json.gz",
    );

    let listing = list_local(root.path(), &whole_october()).unwrap();

    assert_eq!(
        listing.manifest.partitions,
        vec![partition(
            "111111111111",
            "us-east-1",
            vec![
                entry(0, &early, at(1, 10, 0)),
                entry(1, &late_a, at(1, 10, 5)),
                entry(2, &late_b, at(1, 10, 5)),
                entry(3, &next_day, at(2, 0, 0)),
            ],
        )]
    );
    assert!(
        listing.unrecognized.is_empty(),
        "{:?}",
        listing.unrecognized
    );
}

#[test]
fn organization_and_account_trail_files_are_merged_in_one_partition() {
    let root = tempfile::tempdir().unwrap();
    let org_first = write_log_file(
        root.path(),
        &org_log("111111111111", "us-east-1", at(1, 10, 0), "o1"),
    );
    let account_middle = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 5), "a1"),
    );
    let org_last = write_log_file(
        root.path(),
        &org_log("111111111111", "us-east-1", at(1, 10, 10), "o2"),
    );
    let other_region = write_log_file(
        root.path(),
        &org_log("111111111111", "ap-northeast-1", at(1, 10, 0), "o3"),
    );
    let other_account = write_log_file(
        root.path(),
        &log("222222222222", "us-east-1", at(1, 9, 0), "a2"),
    );

    let listing = list_local(root.path(), &whole_october()).unwrap();

    assert_eq!(
        listing.manifest.partitions,
        vec![
            partition(
                "111111111111",
                "ap-northeast-1",
                vec![entry(0, &other_region, at(1, 10, 0))],
            ),
            partition(
                "111111111111",
                "us-east-1",
                vec![
                    entry(0, &org_first, at(1, 10, 0)),
                    entry(1, &account_middle, at(1, 10, 5)),
                    entry(2, &org_last, at(1, 10, 10)),
                ],
            ),
            partition(
                "222222222222",
                "us-east-1",
                vec![entry(0, &other_account, at(1, 9, 0))],
            ),
        ]
    );
}

#[test]
fn files_whose_names_do_not_follow_cloudtrail_are_reported_and_left_out() {
    let root = tempfile::tempdir().unwrap();
    let log_dir = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01";
    let kept = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 0), "a"),
    );
    let mut unrecognized = vec![
        write_other_file(root.path(), &format!("{log_dir}/notes.json.gz")),
        write_other_file(
            root.path(),
            &format!("{log_dir}/111111111111_CloudTrail_us-east-1_2026-10-01T10:00Z_a.json.gz"),
        ),
        write_other_file(
            root.path(),
            &format!("{log_dir}/11111_CloudTrail_us-east-1_20261001T1000Z_a.json.gz"),
        ),
        write_other_file(
            root.path(),
            &format!("{log_dir}/111111111111_CloudTrail_us-east-1_20261001T1000Z_.json.gz"),
        ),
        write_other_file(
            root.path(),
            "exported/111111111111_Trail_us-east-1_20261001T1000Z_a.json.gz",
        ),
    ];
    write_other_file(root.path(), &format!("{log_dir}/README.txt"));
    write_other_file(
        root.path(),
        &format!("{log_dir}/111111111111_CloudTrail_us-east-1_20261001T1000Z_b.json"),
    );

    let listing = list_local(root.path(), &whole_october()).unwrap();

    unrecognized.sort();
    assert_eq!(listing.unrecognized, unrecognized);
    assert_eq!(
        listing.manifest.partitions,
        vec![partition(
            "111111111111",
            "us-east-1",
            vec![entry(0, &kept, at(1, 10, 0))],
        )]
    );
}

#[test]
fn given_accounts_and_regions_make_every_partition_even_without_files() {
    let root = tempfile::tempdir().unwrap();
    let kept = write_log_file(
        root.path(),
        &log("111111111111", "us-east-1", at(1, 10, 0), "a"),
    );
    write_log_file(
        root.path(),
        &log("333333333333", "us-east-1", at(1, 10, 0), "a"),
    );
    write_log_file(
        root.path(),
        &log("111111111111", "ap-northeast-1", at(1, 10, 0), "a"),
    );
    let accounts = ["222222222222".to_owned(), "111111111111".to_owned()];
    let regions = ["us-east-1".to_owned(), "eu-west-1".to_owned()];
    let scope = ListScope {
        accounts: Some(&accounts),
        regions: Some(&regions),
        ..whole_october()
    };

    let listing = list_local(root.path(), &scope).unwrap();

    assert_eq!(
        listing.manifest.partitions,
        vec![
            partition("111111111111", "eu-west-1", vec![]),
            partition(
                "111111111111",
                "us-east-1",
                vec![entry(0, &kept, at(1, 10, 0))]
            ),
            partition("222222222222", "eu-west-1", vec![]),
            partition("222222222222", "us-east-1", vec![]),
        ]
    );
    assert!(
        listing.unrecognized.is_empty(),
        "{:?}",
        listing.unrecognized
    );
}

#[test]
fn given_accounts_alone_filter_the_files_found() {
    let root = tempfile::tempdir().unwrap();
    let kept = write_log_file(
        root.path(),
        &log("111111111111", "ap-northeast-1", at(1, 10, 0), "a"),
    );
    write_log_file(
        root.path(),
        &log("333333333333", "us-east-1", at(1, 10, 0), "a"),
    );
    let accounts = ["111111111111".to_owned(), "222222222222".to_owned()];
    let scope = ListScope {
        accounts: Some(&accounts),
        ..whole_october()
    };

    let listing = list_local(root.path(), &scope).unwrap();

    assert_eq!(
        listing.manifest.partitions,
        vec![partition(
            "111111111111",
            "ap-northeast-1",
            vec![entry(0, &kept, at(1, 10, 0))],
        )]
    );
}

#[test]
fn only_files_in_the_read_period_of_the_plan_are_listed() {
    let root = tempfile::tempdir().unwrap();
    let plan = RunPlan::parse(
        &format!(
            r#"
run_id: case-1
source:
  kind: local
  uri: '{}'
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: 2026-10-02T00:00:00Z
pipeline:
  allowed_lateness: 30m
placement:
  mode: single_process
"#,
            root.path().display()
        ),
        Path::new("/"),
    )
    .unwrap();
    let baseline_since = at(1, 0, 0) - TimeDelta::days(30);
    let write =
        |time: EventTime| write_log_file(root.path(), &log("111111111111", "us-east-1", time, "a"));
    write(baseline_since - TimeDelta::minutes(5));
    let first = write(baseline_since);
    let last = write(at(1, 23, 55));
    write(at(2, 0, 0));

    let scope = ListScope::from_plan(&plan);
    let listing = list_local(root.path(), &scope).unwrap();

    assert_eq!(scope.period, baseline_since..at(2, 0, 0));
    assert_eq!(scope.accounts, None);
    assert_eq!(scope.regions, None);
    assert_eq!(
        listing.manifest.partitions,
        vec![partition(
            "111111111111",
            "us-east-1",
            vec![
                entry(0, &first, baseline_since),
                entry(1, &last, at(1, 23, 55)),
            ],
        )]
    );
}

#[test]
fn the_read_period_reaches_back_by_the_lookback_of_the_context() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("context.yaml"),
        "baseline:\n  lookback: 7d\n",
    )
    .unwrap();
    let plan_path = dir.path().join("run.yaml");
    fs::write(
        &plan_path,
        r#"
run_id: case-1
source:
  kind: local
  uri: logs
analysis_window:
  since: 2026-10-10T00:00:00Z
  until: 2026-10-11T00:00:00Z
context: ./context.yaml
pipeline:
  allowed_lateness: 30m
placement:
  mode: single_process
"#,
    )
    .unwrap();
    let plan = RunPlan::load(&plan_path).unwrap();

    let scope = ListScope::from_plan(&plan);

    assert_eq!(scope.period, at(3, 0, 0)..at(11, 0, 0));
}

#[test]
fn scope_takes_accounts_and_regions_from_the_plan() {
    let plan = RunPlan::parse(
        r#"
run_id: case-1
source:
  kind: local
  uri: logs
  accounts: ["111111111111"]
  regions: [us-east-1, eu-west-1]
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: 2026-10-02T00:00:00Z
pipeline:
  allowed_lateness: 30m
placement:
  mode: single_process
"#,
        Path::new("/cases/case-1"),
    )
    .unwrap();

    let scope = ListScope::from_plan(&plan);

    assert_eq!(scope.accounts, Some(&["111111111111".to_owned()][..]));
    assert_eq!(
        scope.regions,
        Some(&["us-east-1".to_owned(), "eu-west-1".to_owned()][..])
    );
}

#[test]
fn empty_directory_gives_an_empty_manifest() {
    let root = tempfile::tempdir().unwrap();

    let listing = list_local(root.path(), &whole_october()).unwrap();

    assert!(listing.manifest.partitions.is_empty());
    assert!(listing.unrecognized.is_empty());
}

#[test]
fn missing_directory_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing");

    let error = list_local(&missing, &whole_october()).unwrap_err();

    assert!(
        matches!(&error, ListError::Io { path, .. } if *path == missing),
        "{error:?}"
    );
}
