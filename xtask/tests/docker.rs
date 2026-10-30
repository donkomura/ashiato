use std::path::{Path, PathBuf};

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use ashiato_testkit::minio::{ACCESS_KEY, MINIO_IMAGE, SECRET_KEY};
use xtask::compose::Service;
use xtask::docker::{
    BUCKET, ComposeProject, ContainerState, MinioSetup, dataset_dir, dataset_name, minio_volume,
    parse_ps, project_name,
};

fn project() -> ComposeProject {
    ComposeProject {
        file: PathBuf::from("/repo/target/integration/f1/compose.yaml"),
        project: "ashiato-integration-a-f1".to_owned(),
    }
}

fn compose(rest: &[&str]) -> Vec<String> {
    [
        "compose",
        "--file",
        "/repo/target/integration/f1/compose.yaml",
        "--project-name",
        "ashiato-integration-a-f1",
    ]
    .iter()
    .chain(rest)
    .map(|arg| (*arg).to_owned())
    .collect()
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}

fn spec(kind: DatasetKind, scale: f64) -> DatasetSpec {
    DatasetSpec {
        kind,
        seed: 1,
        scale,
    }
}

fn setup() -> MinioSetup {
    MinioSetup {
        container: "ashiato-integration-setup-42".to_owned(),
        volume: "ashiato-integration-minio-a-0.01-seed1".to_owned(),
        dataset: PathBuf::from("/repo/target/integration/datasets/a-0.01"),
    }
}

#[test]
fn each_run_is_a_compose_project_of_its_own() {
    assert_eq!(project_name("a", "r0"), "ashiato-integration-a-r0");
    assert_eq!(
        project_name("a-no-data-events", "f3"),
        "ashiato-integration-a-no-data-events-f3"
    );
}

#[test]
fn the_services_are_started_killed_and_started_again_by_name() {
    let project = project();
    let cases = [
        (
            project.up(&[Service::Minio, Service::Nats]),
            compose(&["up", "--detach", "minio", "nats"]),
        ),
        (
            project.up(&[Service::Analyzer, Service::Worker(1), Service::Worker(4)]),
            compose(&["up", "--detach", "analyzer", "worker-1", "worker-4"]),
        ),
        (
            project.kill(Service::Worker(2)),
            compose(&["kill", "worker-2"]),
        ),
        (
            project.start(Service::Worker(2)),
            compose(&["start", "worker-2"]),
        ),
        (project.kill(Service::Nats), compose(&["kill", "nats"])),
        (
            project.start(Service::Analyzer),
            compose(&["start", "analyzer"]),
        ),
    ];
    for (args, expected) in cases {
        assert_eq!(args, expected);
    }
}

// docker compose start does nothing for a service without a container, so the late worker of F4
// is created with the other roles and started later.
#[test]
fn a_service_held_back_is_created_without_starting_it() {
    let project = project();

    assert_eq!(
        project.create(&[Service::Worker(3)]),
        compose(&["create", "worker-3"])
    );
    assert_eq!(project.create(&[]), compose(&["create"]));
}

#[test]
fn the_state_of_a_container_is_asked_in_json_and_the_servers_are_probed() {
    let project = project();

    assert_eq!(
        project.ps(Service::Analyzer),
        compose(&["ps", "--all", "--format", "json", "analyzer"])
    );
    assert_eq!(
        project.logs(Service::Nats),
        compose(&["logs", "--no-color", "nats"])
    );
    assert_eq!(
        project.minio_live(),
        compose(&[
            "exec",
            "-T",
            "minio",
            "curl",
            "-fsS",
            "http://127.0.0.1:9000/minio/health/live"
        ])
    );
}

#[test]
fn a_run_ends_by_keeping_the_logs_and_removing_its_containers_and_anonymous_volumes() {
    let project = project();

    assert_eq!(
        project.all_logs(),
        compose(&["logs", "--no-color", "--timestamps"])
    );
    assert_eq!(
        project.down(),
        compose(&["down", "--volumes", "--remove-orphans", "--timeout", "0"])
    );
}

#[test]
fn the_state_of_a_container_is_read_from_the_json_of_compose_ps() {
    let running = r#"{"Command":"\"/bin/sh -c 'exec /opt/ashiato/bin/ashiato run'\"","ExitCode":0,"Name":"ashiato-integration-a-r0-analyzer-1","Service":"analyzer","State":"running","Status":"Up 2 seconds"}"#;
    let exited = r#"{"ExitCode":137,"Name":"ashiato-integration-a-f3-analyzer-1","Service":"analyzer","State":"exited","Status":"Exited (137) Less than a second ago"}"#;
    let dead = r#"{"ExitCode":1,"Service":"analyzer","State":"dead"}"#;

    let state = parse_ps(running).unwrap();
    assert_eq!(
        state,
        ContainerState {
            state: "running".to_owned(),
            exit_code: 0
        }
    );
    assert!(!state.exited());
    let state = parse_ps(&format!("{exited}\n")).unwrap();
    assert_eq!(state.exit_code, 137);
    assert!(state.exited());
    assert!(parse_ps(dead).unwrap().exited());
    assert!(
        parse_ps(&format!("[{exited}]")).unwrap().exited(),
        "older compose prints an array"
    );
    assert_eq!(parse_ps(""), None);
    assert_eq!(parse_ps("[]"), None);
}

#[test]
fn a_dataset_is_kept_under_the_integration_directory_by_its_kind_and_scale() {
    let dir = Path::new("/repo/target/integration");
    let cases = [
        (
            spec(DatasetKind::A, 0.01),
            "/repo/target/integration/datasets/a-0.01",
        ),
        (
            spec(DatasetKind::ANoDataEvents, 1.0),
            "/repo/target/integration/datasets/a-no-data-events-1",
        ),
        (
            spec(DatasetKind::B, 0.5),
            "/repo/target/integration/datasets/b-0.5",
        ),
    ];
    for (spec, expected) in cases {
        assert_eq!(dataset_dir(dir, &spec), PathBuf::from(expected));
    }
    assert_eq!(dataset_name(DatasetKind::A), "a");
    assert_eq!(dataset_name(DatasetKind::ANoDataEvents), "a-no-data-events");
    assert_eq!(dataset_name(DatasetKind::B), "b");
}

#[test]
fn each_dataset_has_a_minio_volume_named_by_its_kind_scale_and_seed() {
    assert_eq!(
        minio_volume(&spec(DatasetKind::A, 0.01)),
        "ashiato-integration-minio-a-0.01-seed1"
    );
    assert_eq!(
        minio_volume(&spec(DatasetKind::ANoDataEvents, 1.0)),
        "ashiato-integration-minio-a-no-data-events-1-seed1"
    );
    let other_seed = DatasetSpec {
        seed: 2,
        ..spec(DatasetKind::A, 0.01)
    };
    assert_ne!(
        minio_volume(&other_seed),
        minio_volume(&spec(DatasetKind::A, 0.01))
    );
}

#[test]
fn the_setup_container_serves_the_volume_and_reads_the_dataset_read_only() {
    let setup = setup();

    assert_eq!(
        setup.run(),
        strings(&[
            "run",
            "--detach",
            "--rm",
            "--name",
            "ashiato-integration-setup-42",
            "--label",
            "ashiato-integration=setup",
            "--user",
            "0:0",
            "--volume",
            "ashiato-integration-minio-a-0.01-seed1:/data",
            "--volume",
            "/repo/target/integration/datasets/a-0.01:/dataset:ro",
            "--env",
            &format!("MINIO_ROOT_USER={ACCESS_KEY}"),
            "--env",
            &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
            "--env",
            &format!("MC_HOST_local=http://{ACCESS_KEY}:{SECRET_KEY}@127.0.0.1:9000"),
            "--entrypoint",
            "minio",
            MINIO_IMAGE,
            "server",
            "/data",
        ])
    );
    assert_eq!(
        setup.live(),
        strings(&[
            "exec",
            "ashiato-integration-setup-42",
            "curl",
            "-fsS",
            "http://127.0.0.1:9000/minio/health/live",
        ])
    );
    assert_eq!(
        setup.remove(),
        strings(&["rm", "--force", "ashiato-integration-setup-42"])
    );
}

#[test]
fn the_summary_is_uploaded_last_so_that_it_marks_a_complete_upload() {
    let setup = setup();
    let exec = |args: &[&str]| {
        let mut command = strings(&["exec", "ashiato-integration-setup-42", "mc"]);
        command.extend(strings(args));
        command
    };

    assert_eq!(
        setup.uploaded_summary(),
        exec(&["cat", &format!("local/{BUCKET}/dataset.json")])
    );
    assert_eq!(
        setup.upload(),
        vec![
            exec(&["mb", "--ignore-existing", &format!("local/{BUCKET}")]),
            exec(&["version", "enable", &format!("local/{BUCKET}")]),
            exec(&[
                "cp",
                "--quiet",
                "--recursive",
                "/dataset/AWSLogs/",
                &format!("local/{BUCKET}/AWSLogs/"),
            ]),
            exec(&[
                "cp",
                "--quiet",
                "/dataset/dataset.json",
                &format!("local/{BUCKET}/dataset.json"),
            ]),
        ]
    );
}
