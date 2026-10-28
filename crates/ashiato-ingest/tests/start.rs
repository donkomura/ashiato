use std::collections::HashMap;
use std::sync::Mutex;

use ashiato_ingest::{PartitionAssignment, WorkerError, resume_assignments};
use ashiato_pipeline::{
    ControlClient, ManifestEntry, PartitionId, PartitionManifest, Position, TransportError,
    WorkerPlan,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use serde_json::json;

fn partition(account: &str) -> PartitionId {
    PartitionId::new(account, "us-east-1")
}

fn entries(account: &str, count: u64) -> Vec<ManifestEntry> {
    (0..count)
        .map(|file_seq| ManifestEntry {
            file_seq,
            uri: format!("s3://trail-bucket/AWSLogs/{account}/{file_seq}.json.gz"),
            version_id: Some(format!("v{file_seq}")),
            etag: Some(format!("\"etag-{file_seq}\"")),
            size: 100 + file_seq,
            file_time: Utc.with_ymd_and_hms(2026, 10, 1, 0, 5, 0).unwrap(),
        })
        .collect()
}

fn worker_plan() -> WorkerPlan {
    serde_json::from_value(json!({
        "run_id": "case-1",
        "worker_id": 1,
        "source": { "uri": "s3://trail-bucket/AWSLogs/", "endpoint_url": null },
        "allowed_lateness": "30m",
        "max_message_size": 1_048_576,
        "partitions": [
            { "index": 1, "partition": partition("111111111111") },
            { "index": 3, "partition": partition("333333333333") },
        ],
    }))
    .unwrap()
}

struct FakeControl {
    plan: Result<WorkerPlan, String>,
    files: HashMap<PartitionId, Vec<ManifestEntry>>,
    committed: HashMap<PartitionId, Position>,
    published: HashMap<PartitionId, Position>,
    manifest_requests: Mutex<Vec<(PartitionId, u64)>>,
}

impl FakeControl {
    fn new() -> Self {
        Self {
            plan: Ok(worker_plan()),
            files: HashMap::from([
                (partition("111111111111"), entries("111111111111", 4)),
                (partition("333333333333"), entries("333333333333", 2)),
            ]),
            committed: HashMap::from([
                (partition("111111111111"), Position::new(2, 0)),
                (partition("333333333333"), Position::new(1, 4)),
            ]),
            published: HashMap::from([(partition("111111111111"), Position::new(3, 5))]),
            manifest_requests: Mutex::new(Vec::new()),
        }
    }
}

fn refused(reason: &str) -> TransportError {
    TransportError::Broker(reason.into())
}

#[async_trait]
impl ControlClient for FakeControl {
    async fn plan(&self, worker_id: u32) -> Result<WorkerPlan, TransportError> {
        assert_eq!(worker_id, 1);
        self.plan.clone().map_err(|reason| refused(&reason))
    }

    async fn manifest(
        &self,
        partition: &PartitionId,
        from_file_seq: u64,
    ) -> Result<Vec<ManifestEntry>, TransportError> {
        self.manifest_requests
            .lock()
            .unwrap()
            .push((partition.clone(), from_file_seq));
        Ok(self.files[partition].clone())
    }

    async fn resume_positions(
        &self,
        partitions: &[PartitionId],
    ) -> Result<HashMap<PartitionId, Position>, TransportError> {
        Ok(partitions
            .iter()
            .filter_map(|partition| {
                let position = self.committed.get(partition)?;
                Some((partition.clone(), *position))
            })
            .collect())
    }

    async fn last_published(
        &self,
        partition: &PartitionId,
    ) -> Result<Option<Position>, TransportError> {
        Ok(self.published.get(partition).copied())
    }
}

#[tokio::test]
async fn a_worker_resumes_each_partition_from_the_committed_position_or_the_end_of_the_stream() {
    let control = FakeControl::new();

    let (plan, assignments) = resume_assignments(&control, 1).await.unwrap();

    assert_eq!(plan, worker_plan());
    assert_eq!(
        assignments,
        vec![
            PartitionAssignment {
                index: 1,
                manifest: PartitionManifest {
                    partition: partition("111111111111"),
                    files: entries("111111111111", 4),
                },
                start: Position::new(3, 5),
            },
            PartitionAssignment {
                index: 3,
                manifest: PartitionManifest {
                    partition: partition("333333333333"),
                    files: entries("333333333333", 2),
                },
                start: Position::new(1, 4),
            },
        ]
    );
    assert_eq!(
        *control.manifest_requests.lock().unwrap(),
        vec![
            (partition("111111111111"), 0),
            (partition("333333333333"), 0)
        ]
    );
}

#[tokio::test]
async fn a_stream_behind_the_committed_position_does_not_move_the_start_back() {
    let mut control = FakeControl::new();
    control
        .published
        .insert(partition("333333333333"), Position::new(0, 7));

    let (_, assignments) = resume_assignments(&control, 1).await.unwrap();

    assert_eq!(assignments[1].start, Position::new(1, 4));
}

#[tokio::test]
async fn a_partition_without_a_resume_position_stops_the_worker() {
    let mut control = FakeControl::new();
    control.committed.remove(&partition("333333333333"));

    let error = resume_assignments(&control, 1).await.unwrap_err();

    assert!(
        matches!(&error, WorkerError::MissingResumePosition(missing) if *missing == partition("333333333333")),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_refused_plan_stops_the_worker() {
    let mut control = FakeControl::new();
    control.plan = Err("worker 1 is not one of the 1 workers of the run".to_owned());

    let error = resume_assignments(&control, 1).await.unwrap_err();

    assert!(matches!(error, WorkerError::Control(_)), "{error:?}");
}
