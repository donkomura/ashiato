mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{Cluster, Process, assert_same_run, finish, finish_all, kill};

// Four workers, as in 14.1.3 節. A few messages for each partition make the workers wait for
// the analyzer, so that the run is still going when a failure is injected.
const WORKERS: u32 = 4;
const BROKER: &str = "  max_messages_per_partition: 2\n";

struct Reference {
    plan: PathBuf,
    output: Vec<u8>,
}

fn reference(cluster: &Cluster) -> Reference {
    let (plan, output) = cluster.run_single_process();
    Reference { plan, output }
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn f1_a_worker_stopped_and_started_again_gives_the_same_output() {
    let cluster = Cluster::start();
    let reference = reference(&cluster);

    let plan = cluster.distributed_plan("f1", WORKERS, BROKER);
    let mut run = cluster.spawn_run(&plan);
    let mut workers = cluster.spawn_workers(WORKERS);
    cluster.wait_until_published(0.3, &mut run);
    kill(&mut workers[1]);
    std::thread::sleep(Duration::from_secs(2));
    workers[1] = cluster.spawn_worker(1);
    finish_all(workers);
    let output = finish(run);

    assert_same_run(
        &cluster,
        (&reference.plan, &reference.output),
        (&plan, &output.stdout),
    );
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn f2_a_restart_of_the_nats_server_gives_the_same_output() {
    let cluster = Cluster::start();
    let reference = reference(&cluster);

    let plan = cluster.distributed_plan("f2", WORKERS, BROKER);
    let mut run = cluster.spawn_run(&plan);
    let workers = cluster.spawn_workers(WORKERS);
    cluster.wait_until_published(0.5, &mut run);
    cluster.nats.restart();
    finish_all(workers);
    let output = finish(run);

    assert_same_run(
        &cluster,
        (&reference.plan, &reference.output),
        (&plan, &output.stdout),
    );
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn f3_the_analyzer_stopped_and_started_again_gives_the_same_output() {
    let cluster = Cluster::start();
    let reference = reference(&cluster);

    let plan = cluster.distributed_plan("f3", WORKERS, BROKER);
    let mut run = cluster.spawn_run(&plan);
    let workers = cluster.spawn_workers(WORKERS);
    cluster.wait_until_published(0.7, &mut run);
    kill(&mut run);
    std::thread::sleep(Duration::from_secs(2));
    let run = cluster.spawn_run(&plan);
    finish_all(workers);
    let output = finish(run);

    assert_same_run(
        &cluster,
        (&reference.plan, &reference.output),
        (&plan, &output.stdout),
    );
}

#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn f4_a_worker_started_late_gives_the_same_output() {
    let cluster = Cluster::start();
    let reference = reference(&cluster);

    let plan = cluster.distributed_plan("f4", WORKERS, BROKER);
    let mut run = cluster.spawn_run(&plan);
    let mut workers: Vec<Process> = (0..WORKERS)
        .filter(|worker_id| *worker_id != 2)
        .map(|worker_id| cluster.spawn_worker(worker_id))
        .collect();
    std::thread::sleep(Duration::from_secs(10));
    assert!(run.is_running(), "the run ended without worker 2");
    workers.push(cluster.spawn_worker(2));
    finish_all(workers);
    let output = finish(run);

    assert_same_run(
        &cluster,
        (&reference.plan, &reference.output),
        (&plan, &output.stdout),
    );
}
