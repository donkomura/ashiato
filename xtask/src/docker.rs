use std::path::{Path, PathBuf};

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use serde::Deserialize;

use crate::compose::Service;

pub const BUCKET: &str = "ashiato-integration";
pub const SETUP_LABEL: &str = "ashiato-integration=setup";
pub const NATS_READY: &str = "Server is ready";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeProject {
    pub file: PathBuf,
    pub project: String,
}

impl ComposeProject {
    pub fn up(&self, services: &[Service]) -> Vec<String> {
        let _ = services;
        todo!()
    }

    pub fn kill(&self, service: Service) -> Vec<String> {
        let _ = service;
        todo!()
    }

    pub fn start(&self, service: Service) -> Vec<String> {
        let _ = service;
        todo!()
    }

    pub fn ps(&self, service: Service) -> Vec<String> {
        let _ = service;
        todo!()
    }

    pub fn logs(&self, service: Service) -> Vec<String> {
        let _ = service;
        todo!()
    }

    pub fn all_logs(&self) -> Vec<String> {
        todo!()
    }

    pub fn minio_live(&self) -> Vec<String> {
        todo!()
    }

    pub fn down(&self) -> Vec<String> {
        todo!()
    }
}

pub fn project_name(dataset: &str, run: &str) -> String {
    let _ = (dataset, run);
    todo!()
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
        todo!()
    }
}

pub fn parse_ps(output: &str) -> Option<ContainerState> {
    let _ = output;
    todo!()
}

pub fn dataset_name(kind: DatasetKind) -> &'static str {
    let _ = kind;
    todo!()
}

pub fn dataset_dir(integration_dir: &Path, spec: &DatasetSpec) -> PathBuf {
    let _ = (integration_dir, spec);
    todo!()
}

pub fn minio_volume(spec: &DatasetSpec) -> String {
    let _ = spec;
    todo!()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinioSetup {
    pub container: String,
    pub volume: String,
    pub dataset: PathBuf,
}

impl MinioSetup {
    pub fn run(&self) -> Vec<String> {
        todo!()
    }

    pub fn live(&self) -> Vec<String> {
        todo!()
    }

    pub fn uploaded_summary(&self) -> Vec<String> {
        todo!()
    }

    pub fn upload(&self) -> Vec<Vec<String>> {
        todo!()
    }

    pub fn remove(&self) -> Vec<String> {
        todo!()
    }
}
