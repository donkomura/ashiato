use std::path::{Path, PathBuf};

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use ashiato_testkit::minio::{ACCESS_KEY, MINIO_IMAGE, SECRET_KEY};
use serde::Deserialize;

use crate::compose::Service;

pub const BUCKET: &str = "ashiato-integration";
pub const SETUP_LABEL: &str = "ashiato-integration=setup";
// The nats image has no shell to probe the server with, so its log tells when it listens.
pub const NATS_READY: &str = "Server is ready";

const PROJECT_PREFIX: &str = "ashiato-integration";
const MINIO_LIVE: &str = "http://127.0.0.1:9000/minio/health/live";
const MINIO_DATA: &str = "/data";
const DATASET_IN_CONTAINER: &str = "/dataset";
const SUMMARY_FILE: &str = "dataset.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeProject {
    pub file: PathBuf,
    pub project: String,
}

impl ComposeProject {
    pub fn up(&self, services: &[Service]) -> Vec<String> {
        let mut args = self.command(&["up", "--detach"]);
        args.extend(services.iter().map(Service::name));
        args
    }

    pub fn kill(&self, service: Service) -> Vec<String> {
        self.command(&["kill", &service.name()])
    }

    pub fn start(&self, service: Service) -> Vec<String> {
        self.command(&["start", &service.name()])
    }

    pub fn ps(&self, service: Service) -> Vec<String> {
        self.command(&["ps", "--all", "--format", "json", &service.name()])
    }

    pub fn logs(&self, service: Service) -> Vec<String> {
        self.command(&["logs", "--no-color", &service.name()])
    }

    pub fn all_logs(&self) -> Vec<String> {
        self.command(&["logs", "--no-color", "--timestamps"])
    }

    pub fn minio_live(&self) -> Vec<String> {
        self.command(&["exec", "-T", "minio", "curl", "-fsS", MINIO_LIVE])
    }

    // The MinIO image declares volumes of its own, which only --volumes removes. The volume with
    // the dataset is external, so compose leaves it for the next run.
    pub fn down(&self) -> Vec<String> {
        self.command(&["down", "--volumes", "--remove-orphans", "--timeout", "0"])
    }

    fn command(&self, rest: &[&str]) -> Vec<String> {
        let mut args = vec![
            "compose".to_owned(),
            "--file".to_owned(),
            self.file.display().to_string(),
            "--project-name".to_owned(),
            self.project.clone(),
        ];
        args.extend(rest.iter().map(|arg| (*arg).to_owned()));
        args
    }
}

pub fn project_name(dataset: &str, run: &str) -> String {
    format!("{PROJECT_PREFIX}-{dataset}-{run}")
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ContainerState {
    #[serde(rename = "State")]
    pub state: String,
    #[serde(rename = "ExitCode")]
    pub exit_code: i64,
}

impl ContainerState {
    pub fn exited(&self) -> bool {
        matches!(self.state.as_str(), "exited" | "dead")
    }
}

// Compose prints one JSON object per line since v2.21 and a JSON array before it.
pub fn parse_ps(output: &str) -> Option<ContainerState> {
    let output = output.trim();
    if output.starts_with('[') {
        let states: Vec<ContainerState> = serde_json::from_str(output).ok()?;
        return states.into_iter().next();
    }
    output
        .lines()
        .find_map(|line| serde_json::from_str(line).ok())
}

pub fn dataset_name(kind: DatasetKind) -> &'static str {
    match kind {
        DatasetKind::A => "a",
        DatasetKind::ANoDataEvents => "a-no-data-events",
        DatasetKind::B => "b",
    }
}

pub fn dataset_dir(integration_dir: &Path, spec: &DatasetSpec) -> PathBuf {
    integration_dir
        .join("datasets")
        .join(format!("{}-{}", dataset_name(spec.kind), spec.scale))
}

// The name holds everything that makes the files differ, so that a volume never holds another
// dataset than its name says.
pub fn minio_volume(spec: &DatasetSpec) -> String {
    format!(
        "{PROJECT_PREFIX}-minio-{}-{}-seed{}",
        dataset_name(spec.kind),
        spec.scale,
        spec.seed
    )
}

// A MinIO server of its own fills the volume before the runs, since the MinIO of a run must not
// start before the dataset is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinioSetup {
    pub container: String,
    pub volume: String,
    pub dataset: PathBuf,
}

impl MinioSetup {
    pub fn run(&self) -> Vec<String> {
        [
            "run",
            "--detach",
            "--rm",
            "--name",
            &self.container,
            "--label",
            SETUP_LABEL,
            "--user",
            "0:0",
            "--volume",
            &format!("{}:{MINIO_DATA}", self.volume),
            "--volume",
            &format!("{}:{DATASET_IN_CONTAINER}:ro", self.dataset.display()),
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
            MINIO_DATA,
        ]
        .map(str::to_owned)
        .to_vec()
    }

    pub fn live(&self) -> Vec<String> {
        self.exec(&["curl", "-fsS", MINIO_LIVE])
    }

    pub fn uploaded_summary(&self) -> Vec<String> {
        self.exec(&["mc", "cat", &format!("local/{BUCKET}/{SUMMARY_FILE}")])
    }

    // Versioning is enabled as in the S3 tests (docs/s3-testing.md), so that the runs read the
    // listed versions. The summary goes last, so that its presence marks a complete upload.
    pub fn upload(&self) -> Vec<Vec<String>> {
        let bucket = format!("local/{BUCKET}");
        vec![
            self.exec(&["mc", "mb", "--ignore-existing", &bucket]),
            self.exec(&["mc", "version", "enable", &bucket]),
            self.exec(&[
                "mc",
                "cp",
                "--quiet",
                "--recursive",
                &format!("{DATASET_IN_CONTAINER}/AWSLogs/"),
                &format!("{bucket}/AWSLogs/"),
            ]),
            self.exec(&[
                "mc",
                "cp",
                "--quiet",
                &format!("{DATASET_IN_CONTAINER}/{SUMMARY_FILE}"),
                &format!("{bucket}/{SUMMARY_FILE}"),
            ]),
        ]
    }

    pub fn remove(&self) -> Vec<String> {
        vec![
            "rm".to_owned(),
            "--force".to_owned(),
            self.container.clone(),
        ]
    }

    fn exec(&self, command: &[&str]) -> Vec<String> {
        let mut args = vec!["exec".to_owned(), self.container.clone()];
        args.extend(command.iter().map(|arg| (*arg).to_owned()));
        args
    }
}
