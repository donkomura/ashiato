mod common;

use std::time::{Duration, Instant};

use ashiato_pipeline::PartitionId;
use common::{BIG_ACCOUNT, BIG_REGION, Cluster, assert_same_run, finish, finish_all, kill};

const BROKER: &str = "  max_messages_per_partition: 4\n";
// Each file of the big partition takes several messages, so that a worker can be stopped
// inside a file.
const SMALL_MESSAGES: &str = "  max_messages_per_partition: 2\n  max_message_size: 12KiB\n";
const BIG_FILE_RECORDS: u64 = 20;

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn a_distributed_run_gives_the_same_output_and_tables_as_a_single_process() {
    let cluster = Cluster::start();
    let (reference, expected) = cluster.run_single_process();

    let plan = cluster.distributed_plan("distributed", 2, BROKER);
    let run = cluster.spawn_run(&plan);
    let workers = cluster.spawn_workers(2);
    finish_all(workers);
    let output = finish(run);

    assert_same_run(&cluster, (&reference, &expected), (&plan, &output.stdout));
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn a_worker_stopped_inside_a_file_resumes_and_gives_the_same_output() {
    let cluster = Cluster::start();
    let (reference, expected) = cluster.run_single_process();

    let plan = cluster.distributed_plan("restarted", 1, SMALL_MESSAGES);
    let mut run = cluster.spawn_run(&plan);
    let mut worker = cluster.spawn_worker(0);
    let big = PartitionId::new(BIG_ACCOUNT, BIG_REGION);
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        assert!(
            Instant::now() < deadline,
            "no message inside a file was seen"
        );
        if !worker.is_running() {
            let output = worker.output();
            panic!(
                "the worker ended before a message inside a file was seen: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if cluster
            .last_published(&big)
            .is_some_and(|end| end.record_offset > 0 && end.record_offset < BIG_FILE_RECORDS)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    kill(&mut worker);
    std::thread::sleep(Duration::from_secs(1));
    assert!(run.is_running(), "the run ended without the worker");
    let worker = cluster.spawn_worker(0);
    finish(worker);
    let output = finish(run);

    assert_same_run(&cluster, (&reference, &expected), (&plan, &output.stdout));
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn a_worker_id_outside_the_plan_is_an_error() {
    let cluster = Cluster::start();
    let plan = cluster.distributed_plan("unknown-worker", 2, BROKER);
    let mut run = cluster.spawn_run(&plan);

    let output = cluster.spawn_worker(2).output();

    kill(&mut run);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("worker 2 is not one of the 2 workers"),
        "{stderr}"
    );
}
