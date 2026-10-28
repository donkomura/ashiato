use std::path::Path;

use ashiato_pipeline::{
    AssignedPartition, ControlError, ManifestEntry, ManifestPage, ManifestRequest, PartitionId,
    PartitionPosition, Position, ResumeRequest, ResumeResponse, RunPlan, WorkerPlan, WorkerSource,
    manifest_page, resume_from, resume_response, validate_distributed,
};
use chrono::{TimeDelta, TimeZone, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

const PLAN: &str = r#"
run_id: case-1
source:
  kind: s3
  uri: s3://trail-bucket/AWSLogs/o-xxxx/
  endpoint_url: http://minio:9000
analysis_window:
  since: 2026-09-24T00:00:00Z
  until: 2026-10-25T00:00:00Z
rules: [builtin, ./rules]
pipeline:
  allowed_lateness: 30m
placement:
  mode: distributed
  nats_url: nats://10.0.0.10:4222
  ingest_workers: 2
broker:
  max_message_size: 512KiB
"#;

fn plan(text: &str) -> RunPlan {
    RunPlan::parse(text, Path::new("/cases/case-1")).unwrap()
}

fn local_plan() -> String {
    PLAN.replace(
        "  kind: s3\n  uri: s3://trail-bucket/AWSLogs/o-xxxx/\n  endpoint_url: http://minio:9000\n",
        "  kind: local\n  uri: ./logs\n",
    )
}

fn single_process_plan(text: &str) -> String {
    text.replace(
        "  mode: distributed\n  nats_url: nats://10.0.0.10:4222\n",
        "  mode: single_process\n",
    )
}

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) {
    let json = serde_json::to_string(value).unwrap();
    assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value, "{json}");
}

fn partition(account: &str, region: &str) -> PartitionId {
    PartitionId::new(account, region)
}

// In the order of the saved manifest, which is the order of the identifiers.
fn manifest_partitions() -> Vec<PartitionId> {
    vec![
        partition("111111111111", "ap-northeast-1"),
        partition("111111111111", "us-east-1"),
        partition("222222222222", "ap-northeast-1"),
    ]
}

#[test]
fn a_distributed_plan_must_read_from_s3_and_name_the_nats_server() {
    let missing_url = PLAN.replace("  nats_url: nats://10.0.0.10:4222\n", "");
    let cases: Vec<(&str, String, Option<&str>)> = vec![
        ("distributed from s3", PLAN.to_owned(), None),
        (
            "distributed from a local directory",
            local_plan(),
            Some("LocalSourceInDistributed"),
        ),
        (
            "distributed without the nats server",
            missing_url,
            Some("MissingNatsUrl"),
        ),
        (
            "single process from a local directory",
            single_process_plan(&local_plan()),
            None,
        ),
        ("single process from s3", single_process_plan(PLAN), None),
    ];
    for (name, text, expected) in cases {
        let result = validate_distributed(&plan(&text));
        match expected {
            None => assert!(result.is_ok(), "{name}: {result:?}"),
            Some(variant) => {
                let error = format!("{:?}", result.unwrap_err());
                assert!(error.starts_with(variant), "{name}: {error}");
            }
        }
    }
}

#[test]
fn the_plan_of_a_worker_holds_what_the_worker_needs_and_its_partitions() {
    let plan = plan(PLAN);
    let partitions = manifest_partitions();

    let first = WorkerPlan::new(&plan, &partitions, 0).unwrap();
    let second = WorkerPlan::new(&plan, &partitions, 1).unwrap();

    let source = WorkerSource {
        uri: "s3://trail-bucket/AWSLogs/o-xxxx/".to_owned(),
        endpoint_url: Some("http://minio:9000".to_owned()),
    };
    assert_eq!(
        first,
        WorkerPlan {
            run_id: "case-1".to_owned(),
            worker_id: 0,
            source: source.clone(),
            allowed_lateness: "30m".parse().unwrap(),
            max_message_size: 512 << 10,
            partitions: vec![
                AssignedPartition {
                    index: 0,
                    partition: partitions[0].clone(),
                },
                AssignedPartition {
                    index: 2,
                    partition: partitions[2].clone(),
                },
            ],
        }
    );
    assert_eq!(
        second.partitions,
        vec![AssignedPartition {
            index: 1,
            partition: partitions[1].clone(),
        }]
    );
    assert_eq!(
        first.allowed_lateness.to_time_delta(),
        TimeDelta::minutes(30)
    );
    round_trip(&first);
    assert!(
        serde_json::to_string(&first)
            .unwrap()
            .contains(r#""allowed_lateness":"30m""#),
        "the duration is written as in the run plan"
    );
}

#[test]
fn a_worker_beyond_the_workers_of_the_run_gets_no_plan() {
    let error = WorkerPlan::new(&plan(PLAN), &manifest_partitions(), 2).unwrap_err();

    assert!(
        matches!(
            error,
            ControlError::UnknownWorker {
                worker_id: 2,
                workers: 2
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_plan_that_cannot_be_distributed_gives_no_worker_plan() {
    let cases = [
        ("local source", local_plan(), "LocalSourceInDistributed"),
        (
            "single process",
            single_process_plan(PLAN),
            "NotDistributed",
        ),
    ];
    for (name, text, variant) in cases {
        let error = WorkerPlan::new(&plan(&text), &manifest_partitions(), 0).unwrap_err();
        let error = format!("{error:?}");
        assert!(error.starts_with(variant), "{name}: {error}");
    }
}

fn entry(file_seq: u64, uri_len: usize) -> ManifestEntry {
    ManifestEntry {
        file_seq,
        uri: format!("s3://trail-bucket/{}", "k".repeat(uri_len)),
        version_id: Some(format!("version-{file_seq}")),
        etag: Some(format!("\"etag-{file_seq}\"")),
        size: 1000 + file_seq,
        file_time: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap()
            + TimeDelta::minutes(5 * i64::try_from(file_seq).unwrap()),
    }
}

fn entries(count: u64) -> Vec<ManifestEntry> {
    (0..count).map(|file_seq| entry(file_seq, 40)).collect()
}

fn encoded_len(page: &ManifestPage) -> usize {
    serde_json::to_vec(page).unwrap().len()
}

fn collect_pages(files: &[ManifestEntry], from_file_seq: u64, limit: usize) -> Vec<ManifestPage> {
    let mut pages = Vec::new();
    let mut next = Some(from_file_seq);
    while let Some(from) = next {
        let page = manifest_page(files, from, limit).unwrap();
        next = page.next_file_seq;
        pages.push(page);
        assert!(pages.len() <= files.len() + 1, "the pages do not advance");
    }
    pages
}

#[test]
fn manifest_pages_fit_in_the_reply_and_together_give_the_whole_list() {
    let one_entry = serde_json::to_vec(&entry(0, 40)).unwrap().len();
    let cases: Vec<(&str, u64, u64, usize, usize)> = vec![
        ("everything fits in one reply", 5, 0, 1 << 20, 1),
        ("an empty partition gives one empty page", 0, 0, 1 << 20, 1),
        ("a few entries per reply", 10, 0, one_entry * 3 + 100, 4),
        ("one entry per reply", 4, 0, one_entry + 60, 4),
        ("starting in the middle of the list", 10, 6, 1 << 20, 1),
        ("starting after the end of the list", 3, 3, 1 << 20, 1),
    ];
    for (name, count, from, limit, page_count) in cases {
        let files = entries(count);

        let pages = collect_pages(&files, from, limit);

        assert_eq!(pages.len(), page_count, "{name}");
        for page in &pages {
            assert!(encoded_len(page) <= limit, "{name}: {}", encoded_len(page));
            round_trip(page);
        }
        let collected: Vec<ManifestEntry> = pages.into_iter().flat_map(|page| page.files).collect();
        let from = usize::try_from(from).unwrap();
        assert_eq!(collected, files[from.min(files.len())..], "{name}");
    }
}

#[test]
fn a_manifest_entry_larger_than_the_reply_is_an_error() {
    let files = vec![entry(0, 40), entry(1, 4000), entry(2, 40)];
    let limit = 2000;

    let first = manifest_page(&files, 0, limit).unwrap();
    let error = manifest_page(&files, 1, limit).unwrap_err();

    assert_eq!(first.files, files[..1]);
    assert_eq!(first.next_file_seq, Some(1));
    assert!(
        matches!(
            error,
            ControlError::EntryTooLarge {
                file_seq: 1,
                limit: 2000,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn manifest_requests_round_trip_through_json() {
    round_trip(&ManifestRequest {
        partition: partition("111111111111", "us-east-1"),
        from_file_seq: 42,
    });
}

#[test]
fn the_resume_reply_gives_the_committed_position_of_each_requested_partition() {
    let partitions = manifest_partitions();
    let committed = [Position::new(3, 0), Position::new(7, 120), Position::START];
    let request = ResumeRequest {
        partitions: vec![partitions[2].clone(), partitions[0].clone()],
    };

    let response = resume_response(&request, &partitions, &committed).unwrap();

    assert_eq!(
        response,
        ResumeResponse {
            positions: vec![
                PartitionPosition {
                    partition: partitions[2].clone(),
                    position: Position::START,
                },
                PartitionPosition {
                    partition: partitions[0].clone(),
                    position: Position::new(3, 0),
                },
            ],
        }
    );
    round_trip(&request);
    round_trip(&response);
}

#[test]
fn the_resume_reply_rejects_a_partition_outside_the_run() {
    let partitions = manifest_partitions();
    let stranger = partition("999999999999", "us-east-1");
    let request = ResumeRequest {
        partitions: vec![partitions[0].clone(), stranger.clone()],
    };

    let error = resume_response(
        &request,
        &partitions,
        &[Position::START, Position::START, Position::START],
    )
    .unwrap_err();

    assert!(
        matches!(&error, ControlError::UnknownPartition(id) if *id == stranger),
        "{error:?}"
    );
}

#[test]
fn a_worker_resumes_from_whichever_of_the_committed_and_the_published_positions_is_ahead() {
    let cases: Vec<(&str, Position, Option<Position>, Position)> = vec![
        (
            "nothing in the stream",
            Position::new(2, 0),
            None,
            Position::new(2, 0),
        ),
        (
            "nothing committed nor published",
            Position::START,
            None,
            Position::START,
        ),
        (
            "the stream holds messages the analyzer has not committed",
            Position::new(2, 0),
            Some(Position::new(4, 0)),
            Position::new(4, 0),
        ),
        (
            "the stream ends in the middle of a file",
            Position::new(2, 0),
            Some(Position::new(3, 250)),
            Position::new(3, 250),
        ),
        (
            "the analyzer committed past the last message in the stream",
            Position::new(5, 100),
            Some(Position::new(5, 40)),
            Position::new(5, 100),
        ),
        (
            "both at the same position",
            Position::new(3, 0),
            Some(Position::new(3, 0)),
            Position::new(3, 0),
        ),
        (
            "the last message in the stream is the end of the partition",
            Position::new(6, 0),
            Some(Position::new(9, 0)),
            Position::new(9, 0),
        ),
        (
            "the end of the partition is committed",
            Position::new(9, 0),
            None,
            Position::new(9, 0),
        ),
    ];
    for (name, committed, last_published, expected) in cases {
        assert_eq!(resume_from(committed, last_published), expected, "{name}");
    }
}
