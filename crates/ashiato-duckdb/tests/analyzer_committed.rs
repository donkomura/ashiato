mod common;

use ashiato_pipeline::{CommittedPositions, Position};
use ashiato_testkit::record::RecordBuilder;
use common::{Workspace, at, log, open, settings, start_worker};
use serde_json::Value;

fn event(id: &str, minute: u32) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", at(1, 0, minute).to_rfc3339())
        .build()
}

fn fixture() -> Workspace {
    let workspace = Workspace::new();
    for (account, prefix) in [("111111111111", "a"), ("222222222222", "b")] {
        for step in 0..3_u32 {
            let minute = 5 + step * 5;
            workspace.write(&log(
                account,
                at(1, 0, minute),
                vec![event(&format!("{prefix}-{step}"), minute - 1)],
            ));
        }
    }
    workspace
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_positions_follow_the_commits_rather_than_the_accepted_messages() {
    let workspace = fixture();
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let shared = CommittedPositions::default();
    let mut analyzer =
        open(&database, &manifest, settings()).with_committed_positions(shared.clone());
    assert_eq!(shared.get(), vec![Position::START; 2]);
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());

    analyzer.receive(&mut reader).await.unwrap();
    let accepted = analyzer.resume_positions();
    assert_ne!(accepted, vec![Position::START; 2]);
    assert_eq!(shared.get(), vec![Position::START; 2]);

    analyzer.commit().unwrap();
    assert_eq!(shared.get(), accepted);

    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
    }
    analyzer.commit().unwrap();
    assert_eq!(shared.get(), vec![Position::new(3, 0); 2]);
    drop(analyzer);
    worker.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_analyzer_shares_the_positions_it_committed_before() {
    let workspace = fixture();
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let mut analyzer = open(&database, &manifest, settings());
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
    analyzer.receive(&mut reader).await.unwrap();
    analyzer.commit().unwrap();
    let committed = analyzer.resume_positions();
    drop(reader);
    drop(analyzer);
    let _ = worker.await.unwrap();

    let shared = CommittedPositions::default();
    let _resumed = open(&database, &manifest, settings()).with_committed_positions(shared.clone());

    assert_eq!(shared.get(), committed);
}
