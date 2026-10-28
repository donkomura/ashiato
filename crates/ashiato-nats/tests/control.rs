use std::path::Path;
use std::time::Duration;

use ashiato_nats::{ControlService, EventStream, NatsControl, NatsWriter, serve_control};
use ashiato_pipeline::{
    CommittedPositions, ControlClient, Manifest, ManifestEntry, PartitionId, PartitionManifest,
    PartitionMessage, PartitionWriter, Position, RunPlan, WorkerPlan,
};
use ashiato_testkit::nats::Nats;
use chrono::{TimeZone, Utc};

const RUN_ID: &str = "case-1";

fn plan(workers: u32) -> RunPlan {
    RunPlan::parse(
        &format!(
            "run_id: {RUN_ID}\n\
             source:\n  kind: s3\n  uri: s3://trail-bucket/AWSLogs/\n  \
             endpoint_url: http://127.0.0.1:9000\n\
             analysis_window:\n  since: 2026-10-01T00:00:00Z\n  until: 2026-10-02T00:00:00Z\n\
             pipeline:\n  batch: 1h\n  allowed_lateness: 30m\n\
             placement:\n  mode: distributed\n  nats_url: nats://127.0.0.1:4222\n  \
             ingest_workers: {workers}\n"
        ),
        Path::new("."),
    )
    .unwrap()
}

fn partition(account: &str) -> PartitionId {
    PartitionId::new(account, "us-east-1")
}

// The URIs are long so that the files of one partition take more than one reply.
fn entries(count: u64) -> Vec<ManifestEntry> {
    (0..count)
        .map(|file_seq| ManifestEntry {
            file_seq,
            uri: format!(
                "s3://trail-bucket/AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/{}{file_seq}.json.gz",
                "x".repeat(200)
            ),
            version_id: Some(format!("version-{file_seq}")),
            etag: Some(format!("\"etag-{file_seq}\"")),
            size: file_seq,
            file_time: Utc.with_ymd_and_hms(2026, 10, 1, 0, 5, 0).unwrap(),
        })
        .collect()
}

fn manifest(files: Vec<ManifestEntry>) -> Manifest {
    Manifest {
        partitions: ["111111111111", "222222222222", "333333333333"]
            .into_iter()
            .enumerate()
            .map(|(index, account)| PartitionManifest {
                partition: partition(account),
                files: if index == 0 {
                    files.clone()
                } else {
                    Vec::new()
                },
            })
            .collect(),
    }
}

fn partitions(manifest: &Manifest) -> Vec<PartitionId> {
    manifest
        .partitions
        .iter()
        .map(|partition| partition.partition.clone())
        .collect()
}

async fn connect(nats: &Nats) -> async_nats::Client {
    async_nats::connect(nats.url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for the NATS server; see docs/nats-testing.md"]
async fn a_manifest_larger_than_a_reply_is_returned_over_several_requests() {
    let nats = Nats::start();
    let client = connect(&nats).await;
    let count = u64::try_from(2 * client.max_payload() / 300).unwrap();
    let files = entries(count);
    assert!(serde_json::to_vec(&files).unwrap().len() > 2 * client.max_payload());
    let manifest = manifest(files.clone());
    let service = ControlService::new(&plan(2), manifest, CommittedPositions::default()).unwrap();
    let _server = serve_control(client.clone(), RUN_ID, service)
        .await
        .unwrap();
    let control = NatsControl::new(client, RUN_ID).unwrap();

    assert_eq!(
        control
            .manifest(&partition("111111111111"), 0)
            .await
            .unwrap(),
        files
    );
    assert_eq!(
        control
            .manifest(&partition("111111111111"), 5)
            .await
            .unwrap(),
        files[5..]
    );
    assert_eq!(
        control
            .manifest(&partition("222222222222"), 0)
            .await
            .unwrap(),
        Vec::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for the NATS server; see docs/nats-testing.md"]
async fn the_plan_of_a_worker_and_the_committed_positions_are_answered() {
    let nats = Nats::start();
    let client = connect(&nats).await;
    let manifest = manifest(entries(3));
    let committed = CommittedPositions::default();
    committed.set(vec![
        Position::new(1, 0),
        Position::START,
        Position::new(0, 4),
    ]);
    let plan = plan(2);
    let service = ControlService::new(&plan, manifest.clone(), committed.clone()).unwrap();
    let _server = serve_control(client.clone(), RUN_ID, service)
        .await
        .unwrap();
    let control = NatsControl::new(client, RUN_ID).unwrap();

    assert_eq!(
        control.plan(1).await.unwrap(),
        WorkerPlan::new(&plan, &partitions(&manifest), 1).unwrap()
    );
    let error = control.plan(2).await.unwrap_err();
    let refused = std::error::Error::source(&error).unwrap().to_string();
    assert!(
        refused.contains("worker 2 is not one of the 2 workers"),
        "{refused}"
    );
    let asked = [partition("333333333333"), partition("111111111111")];
    let positions = control.resume_positions(&asked).await.unwrap();
    assert_eq!(positions[&partition("333333333333")], Position::new(0, 4));
    assert_eq!(positions[&partition("111111111111")], Position::new(1, 0));

    committed.set(vec![
        Position::new(3, 0),
        Position::START,
        Position::new(0, 4),
    ]);
    let positions = control.resume_positions(&asked).await.unwrap();
    assert_eq!(positions[&partition("111111111111")], Position::new(3, 0));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for the NATS server; see docs/nats-testing.md"]
async fn a_worker_waits_until_the_analyzer_answers() {
    let nats = Nats::start();
    let client = connect(&nats).await;
    let plan = plan(1);
    let manifest = manifest(entries(3));
    let control = NatsControl::new(client.clone(), RUN_ID).unwrap();
    let asking = tokio::spawn(async move { control.plan(0).await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let service =
        ControlService::new(&plan, manifest.clone(), CommittedPositions::default()).unwrap();
    let _server = serve_control(client, RUN_ID, service).await.unwrap();

    assert_eq!(
        asking.await.unwrap().unwrap(),
        WorkerPlan::new(&plan, &partitions(&manifest), 0).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for the NATS server; see docs/nats-testing.md"]
async fn the_last_published_position_is_read_from_the_stream() {
    let nats = Nats::start();
    let client = connect(&nats).await;
    let plan = plan(1);
    let _stream = EventStream::create(client.clone(), RUN_ID, 3, &plan.broker)
        .await
        .unwrap();
    let writer = NatsWriter::new(client.clone(), RUN_ID, 1 << 20).unwrap();
    writer
        .put(PartitionMessage::EndOfPartition {
            partition: partition("222222222222"),
            position: Position::new(0, 0),
        })
        .await
        .unwrap();
    let control = NatsControl::new(client, RUN_ID).unwrap();

    assert_eq!(
        control
            .last_published(&partition("222222222222"))
            .await
            .unwrap(),
        Some(Position::START)
    );
    assert_eq!(
        control
            .last_published(&partition("111111111111"))
            .await
            .unwrap(),
        None
    );
}
