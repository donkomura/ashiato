use std::time::Duration;

use xtask::compose::{Layout, Service};
use xtask::metrics::{AnalyzerMetrics, AnalyzerPhases, AnalyzerTimes};
use xtask::schedule::{Fault, REFERENCE, RunSpec, Step, Trigger, correctness_runs};

fn run(name: &str) -> RunSpec {
    correctness_runs()
        .into_iter()
        .find(|run| run.name == name)
        .unwrap_or_else(|| panic!("no run {name}"))
}

fn fault(name: &str) -> Fault {
    run(name)
        .fault
        .unwrap_or_else(|| panic!("{name} has no fault"))
}

fn metrics(manifest_files: u64, ingested_files: u64) -> AnalyzerMetrics {
    AnalyzerMetrics {
        unix_ms: 1_730_000_000_000,
        is_final: false,
        manifest_files,
        ingested_files,
        global_watermark: None,
        pending_rows: 0,
        pending_max_rows: 0,
        time_ms: AnalyzerTimes {
            fetch_wait: 0,
            commit: 0,
            batch: 0,
        },
        phases_unix_ms: AnalyzerPhases {
            first_plan_response: None,
            last_end_committed: None,
            correlated: None,
        },
    }
}

#[test]
fn the_reference_runs_first_and_the_runs_of_14_1_3_follow_in_order() {
    let runs = correctness_runs();
    let names: Vec<&str> = runs.iter().map(|run| run.name).collect();

    assert_eq!(
        names,
        ["r0", "d1", "d2", "d3", "d4", "f1", "f2", "f3", "f4"]
    );
    assert_eq!(runs[0].name, REFERENCE);
    assert_eq!(runs[0].layout, Layout::SingleProcess);
    assert_eq!(runs[0].fault, None);
}

#[test]
fn the_distributed_runs_use_one_to_four_workers_without_faults() {
    for workers in 1..=4 {
        let run = run(&format!("d{workers}"));
        assert_eq!(run.layout, Layout::Distributed { workers });
        assert_eq!(run.fault, None, "d{workers}");
    }
}

#[test]
fn the_fault_runs_use_four_workers_and_the_faults_of_14_1_3() {
    let cases = [
        (
            "f1",
            Fault::RestartWorker {
                worker: 2,
                percent: 30,
                down_for: Duration::from_secs(10),
            },
        ),
        ("f2", Fault::RestartNats { percent: 50 }),
        ("f3", Fault::RestartAnalyzer { percent: 70 }),
        (
            "f4",
            Fault::DelayWorker {
                worker: 3,
                delay: Duration::from_secs(60),
            },
        ),
    ];
    for (name, expected) in cases {
        let run = run(name);
        assert_eq!(run.layout, Layout::Distributed { workers: 4 }, "{name}");
        assert_eq!(run.fault, Some(expected), "{name}");
    }
}

#[test]
fn the_servers_start_before_the_roles_of_ashiato() {
    let cases = [
        ("r0", vec![Service::Minio], vec![Service::Analyzer]),
        (
            "d2",
            vec![Service::Minio, Service::Nats],
            vec![Service::Analyzer, Service::Worker(1), Service::Worker(2)],
        ),
        (
            "f1",
            vec![Service::Minio, Service::Nats],
            vec![
                Service::Analyzer,
                Service::Worker(1),
                Service::Worker(2),
                Service::Worker(3),
                Service::Worker(4),
            ],
        ),
    ];
    for (name, servers, roles) in cases {
        let run = run(name);
        assert_eq!(run.servers(), servers, "{name}");
        assert_eq!(run.roles(), roles, "{name}");
    }
}

#[test]
fn the_late_worker_of_f4_is_held_back_from_the_first_start() {
    let run = run("f4");

    assert_eq!(fault("f4").held_back(), Some(Service::Worker(3)));
    assert_eq!(
        run.roles(),
        [
            Service::Analyzer,
            Service::Worker(1),
            Service::Worker(2),
            Service::Worker(4),
        ]
    );
    for name in ["f1", "f2", "f3"] {
        assert_eq!(fault(name).held_back(), None, "{name}");
    }
}

#[test]
fn each_fault_is_injected_with_docker_kill_and_docker_start() {
    let cases = [
        (
            "f1",
            vec![
                Step::Kill(Service::Worker(2)),
                Step::Pause(Duration::from_secs(10)),
                Step::Start(Service::Worker(2)),
            ],
        ),
        (
            "f2",
            vec![Step::Kill(Service::Nats), Step::Start(Service::Nats)],
        ),
        (
            "f3",
            vec![
                Step::Kill(Service::Analyzer),
                Step::Start(Service::Analyzer),
            ],
        ),
        ("f4", vec![Step::Start(Service::Worker(3))]),
    ];
    for (name, steps) in cases {
        assert_eq!(fault(name).steps(), steps, "{name}");
    }
}

#[test]
fn the_faults_of_f1_to_f3_wait_for_the_ingested_files_and_f4_for_the_time() {
    let cases = [
        ("f1", Trigger::Ingested { percent: 30 }),
        ("f2", Trigger::Ingested { percent: 50 }),
        ("f3", Trigger::Ingested { percent: 70 }),
        ("f4", Trigger::After(Duration::from_secs(60))),
    ];
    for (name, trigger) in cases {
        assert_eq!(fault(name).trigger(), trigger, "{name}");
    }
}

#[test]
fn a_trigger_on_the_ingested_files_waits_for_the_analyzer_to_reach_the_share() {
    let trigger = Trigger::Ingested { percent: 30 };
    let long = Duration::from_secs(3600);

    assert!(!trigger.is_due(None, long));
    assert!(!trigger.is_due(Some(&metrics(0, 0)), long));
    assert!(!trigger.is_due(Some(&metrics(100, 29)), long));
    assert!(trigger.is_due(Some(&metrics(100, 30)), Duration::ZERO));
    assert!(trigger.is_due(Some(&metrics(10, 9)), Duration::ZERO));
}

#[test]
fn a_trigger_on_the_time_counts_from_the_start_of_the_roles() {
    let trigger = Trigger::After(Duration::from_secs(60));

    assert!(!trigger.is_due(None, Duration::from_secs(59)));
    assert!(!trigger.is_due(Some(&metrics(100, 100)), Duration::from_secs(59)));
    assert!(trigger.is_due(None, Duration::from_secs(60)));
}
