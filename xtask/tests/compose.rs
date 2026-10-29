use std::fs;
use std::path::{Path, PathBuf};

use ashiato_testkit::minio::MINIO_IMAGE;
use ashiato_testkit::nats::NATS_IMAGE;
use serde_yaml::Value;
use tempfile::TempDir;
use xtask::compose::{
    AshiatoBinary, ComposeSpec, Layout, RUNTIME_IMAGE, RunDir, Service, compose_file,
};
use xtask::topology::{CpuAssignment, Cpuset};

const RUN_ID: &str = "integration-a";
const USER: &str = "1000:1000";
const MINIO_VOLUME: &str = "ashiato-integration-minio-a";

fn cpus() -> CpuAssignment {
    CpuAssignment {
        servers: Cpuset::new([0, 1]),
        analyzer: Cpuset::new([2, 3]),
        workers: vec![
            Cpuset::new([4, 5]),
            Cpuset::new([6, 7]),
            Cpuset::new([8, 9]),
            Cpuset::new([10, 11]),
        ],
    }
}

fn spec(layout: Layout, run_dir: &Path) -> ComposeSpec {
    let mut cpus = cpus();
    cpus.workers.truncate(layout.workers() as usize);
    ComposeSpec {
        project: "ashiato-integration-d4".to_owned(),
        layout,
        cpus,
        user: USER.to_owned(),
        run_id: RUN_ID.to_owned(),
        run_dir: RunDir::new(run_dir),
        ashiato: AshiatoBinary {
            binary: PathBuf::from("/repo/target/release/ashiato"),
            library_dir: PathBuf::from("/repo/target/duckdb-download/x86_64/v2.0.0"),
        },
        minio_volume: MINIO_VOLUME.to_owned(),
    }
}

fn compose(layout: Layout) -> Value {
    serde_yaml::from_str(&compose_file(&spec(
        layout,
        Path::new("/repo/target/integration/d4"),
    )))
    .unwrap()
}

fn service<'a>(compose: &'a Value, name: &str) -> &'a Value {
    &compose["services"][name]
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_sequence()
        .unwrap_or_else(|| panic!("{value:?} is not a sequence"))
        .iter()
        .map(|item| item.as_str().unwrap().to_owned())
        .collect()
}

fn command(compose: &Value, name: &str) -> String {
    strings(&service(compose, name)["command"]).join(" ")
}

fn service_names(compose: &Value) -> Vec<String> {
    let mut names: Vec<String> = compose["services"]
        .as_mapping()
        .unwrap()
        .keys()
        .map(|key| key.as_str().unwrap().to_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn each_layout_has_its_services() {
    let cases = [
        (Layout::SingleProcess, vec!["minio", "analyzer"]),
        (
            Layout::Distributed { workers: 1 },
            vec!["minio", "nats", "analyzer", "worker-1"],
        ),
        (
            Layout::Distributed { workers: 4 },
            vec![
                "minio", "nats", "analyzer", "worker-1", "worker-2", "worker-3", "worker-4",
            ],
        ),
    ];
    for (layout, expected) in cases {
        let names: Vec<String> = layout.services().iter().map(Service::name).collect();
        assert_eq!(names, expected, "{layout:?}");
        let mut sorted = expected.clone();
        sorted.sort();
        assert_eq!(service_names(&compose(layout)), sorted, "{layout:?}");
    }
}

#[test]
fn a_worker_service_runs_the_worker_id_before_its_number() {
    assert_eq!(Service::Worker(1).worker_id(), Some(0));
    assert_eq!(Service::Worker(4).worker_id(), Some(3));
    assert_eq!(Service::Analyzer.worker_id(), None);
}

#[test]
fn every_container_has_the_cpus_and_the_memory_of_14_1_1() {
    let compose = compose(Layout::Distributed { workers: 4 });
    let cases = [
        ("minio", "0,1", "1g"),
        ("nats", "0,1", "1g"),
        ("analyzer", "2,3", "3g"),
        ("worker-1", "4,5", "1g"),
        ("worker-2", "6,7", "1g"),
        ("worker-3", "8,9", "1g"),
        ("worker-4", "10,11", "1g"),
    ];
    for (name, cpuset, memory) in cases {
        let service = service(&compose, name);
        assert_eq!(service["cpuset"].as_str(), Some(cpuset), "{name}");
        assert_eq!(service["mem_limit"].as_str(), Some(memory), "{name}");
        assert_eq!(service["memswap_limit"].as_str(), Some(memory), "{name}");
    }
}

#[test]
fn the_servers_use_the_pinned_images_and_ashiato_runs_from_the_host_build() {
    let compose = compose(Layout::Distributed { workers: 2 });
    let cases = [
        ("minio", MINIO_IMAGE),
        ("nats", NATS_IMAGE),
        ("analyzer", RUNTIME_IMAGE),
        ("worker-1", RUNTIME_IMAGE),
        ("worker-2", RUNTIME_IMAGE),
    ];
    for (name, image) in cases {
        assert_eq!(service(&compose, name)["image"].as_str(), Some(image));
    }
    assert!(RUNTIME_IMAGE.starts_with("debian:bookworm-2"));
    for name in ["analyzer", "worker-1", "worker-2"] {
        let service = service(&compose, name);
        let volumes = strings(&service["volumes"]);
        assert!(
            volumes
                .contains(&"/repo/target/release/ashiato:/opt/ashiato/bin/ashiato:ro".to_owned()),
            "{name}: {volumes:?}"
        );
        assert!(
            volumes.contains(
                &"/repo/target/duckdb-download/x86_64/v2.0.0:/opt/ashiato/lib:ro".to_owned()
            ),
            "{name}: {volumes:?}"
        );
        assert_eq!(
            service["environment"]["LD_LIBRARY_PATH"].as_str(),
            Some("/opt/ashiato/lib")
        );
        assert_eq!(
            service["environment"]["AWS_ACCESS_KEY_ID"].as_str(),
            Some(ashiato_testkit::minio::ACCESS_KEY)
        );
        assert_eq!(service["user"].as_str(), Some(USER), "{name}");
    }
}

#[test]
fn the_analyzer_runs_the_plan_in_its_workspace_and_keeps_its_output_apart_from_its_metrics() {
    let compose = compose(Layout::Distributed { workers: 4 });
    let analyzer = service(&compose, "analyzer");

    assert_eq!(
        command(&compose, "analyzer"),
        "exec /opt/ashiato/bin/ashiato run --plan /work/run.yaml --emit-signals \
         >/logs/analyzer.stdout 2>>/logs/analyzer.stderr"
    );
    assert_eq!(
        strings(&analyzer["entrypoint"]),
        ["/bin/sh".to_owned(), "-c".to_owned()]
    );
    let volumes = strings(&analyzer["volumes"]);
    assert!(volumes.contains(&"/repo/target/integration/d4/workspace:/work".to_owned()));
    assert!(volumes.contains(&"/repo/target/integration/d4/logs:/logs".to_owned()));
    assert_eq!(
        analyzer["environment"]["ASHIATO_DUCKDB_MEMORY_LIMIT"].as_str(),
        Some("2GB")
    );
}

#[test]
fn each_worker_asks_the_analyzer_of_the_run_through_nats_with_its_own_id() {
    let compose = compose(Layout::Distributed { workers: 4 });
    for number in 1..=4 {
        let name = format!("worker-{number}");
        assert_eq!(
            command(&compose, &name),
            format!(
                "exec /opt/ashiato/bin/ashiato ingest-worker --nats nats://nats:4222 \
                 --run {RUN_ID} --worker-id {} \
                 >>/logs/{name}.stdout 2>>/logs/{name}.stderr",
                number - 1
            )
        );
    }
}

#[test]
fn the_jetstream_store_is_in_the_run_directory_so_that_a_restart_keeps_it() {
    let compose = compose(Layout::Distributed { workers: 1 });
    let nats = service(&compose, "nats");

    let volumes = strings(&nats["volumes"]);
    assert!(volumes.contains(&"/repo/target/integration/d4/nats:/data".to_owned()));
    assert!(
        volumes.contains(
            &"/repo/target/integration/d4/nats.conf:/etc/ashiato/nats.conf:ro".to_owned()
        )
    );
    assert_eq!(
        strings(&nats["command"]),
        ["--config".to_owned(), "/etc/ashiato/nats.conf".to_owned()]
    );
    assert_eq!(nats["user"].as_str(), Some(USER));
}

#[test]
fn every_run_reads_the_same_minio_volume() {
    let cases = [Layout::SingleProcess, Layout::Distributed { workers: 3 }];
    for layout in cases {
        let compose = compose(layout);
        let minio = service(&compose, "minio");
        assert_eq!(
            strings(&minio["volumes"]),
            [format!("{MINIO_VOLUME}:/data")]
        );
        assert_eq!(
            compose["volumes"][MINIO_VOLUME]["external"].as_bool(),
            Some(true)
        );
        assert_eq!(
            strings(&minio["command"]),
            ["server".to_owned(), "/data".to_owned()]
        );
        assert_eq!(minio["user"].as_str(), Some("0:0"));
    }
}

#[test]
fn the_reference_run_has_the_analyzer_alone_in_single_process() {
    let compose = compose(Layout::SingleProcess);

    assert_eq!(compose["name"].as_str(), Some("ashiato-integration-d4"));
    assert_eq!(
        service(&compose, "analyzer")["cpuset"].as_str(),
        Some("2,3")
    );
    assert_eq!(
        service(&compose, "analyzer")["mem_limit"].as_str(),
        Some("3g")
    );
    assert!(command(&compose, "analyzer").contains("run --plan /work/run.yaml"));
}

#[test]
fn preparing_a_run_directory_starts_it_afresh() {
    let dir = TempDir::new().unwrap();
    let run_dir = RunDir::new(dir.path().join("d4"));
    fs::create_dir_all(run_dir.workspace()).unwrap();
    fs::write(run_dir.database(), b"old workspace").unwrap();
    fs::create_dir_all(run_dir.nats_store().join("jetstream")).unwrap();
    fs::write(run_dir.nats_store().join("jetstream").join("old"), b"old").unwrap();

    run_dir.prepare().unwrap();

    assert!(!run_dir.database().exists());
    assert_eq!(fs::read_dir(run_dir.nats_store()).unwrap().count(), 0);
    assert_eq!(fs::read_dir(run_dir.workspace()).unwrap().count(), 0);
    assert!(run_dir.logs().is_dir());
    let config = fs::read_to_string(run_dir.nats_config()).unwrap();
    assert!(config.contains("store_dir: /data"), "{config}");
    assert!(config.contains("sync_interval: always"), "{config}");
}

#[test]
fn the_files_of_a_run_are_under_its_directory() {
    let run_dir = RunDir::new("/repo/target/integration/f1");
    let cases = [
        (run_dir.workspace(), "/repo/target/integration/f1/workspace"),
        (
            run_dir.plan(),
            "/repo/target/integration/f1/workspace/run.yaml",
        ),
        (
            run_dir.database(),
            "/repo/target/integration/f1/workspace/ashiato.duckdb",
        ),
        (
            run_dir.compose_file(),
            "/repo/target/integration/f1/compose.yaml",
        ),
        (
            run_dir.stdout(Service::Analyzer),
            "/repo/target/integration/f1/logs/analyzer.stdout",
        ),
        (
            run_dir.stderr(Service::Worker(2)),
            "/repo/target/integration/f1/logs/worker-2.stderr",
        ),
    ];
    for (path, expected) in cases {
        assert_eq!(path, PathBuf::from(expected));
    }
}
