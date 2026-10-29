use std::io;
use std::path::{Path, PathBuf};

use crate::topology::CpuAssignment;

// ashiato is built on the host (glibc 2.31) and mounted into a slim image with a newer glibc,
// so that no Rust toolchain is needed in Docker.
pub const RUNTIME_IMAGE: &str = "debian:bookworm-20251229-slim";
pub const NATS_URL: &str = "nats://nats:4222";
pub const MINIO_ENDPOINT: &str = "http://minio:9000";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    SingleProcess,
    Distributed { workers: u32 },
}

impl Layout {
    pub fn workers(self) -> u32 {
        todo!()
    }

    pub fn services(self) -> Vec<Service> {
        todo!()
    }
}

// Workers are numbered from 1 as in 14.1.3 節, while ashiato numbers them from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    Minio,
    Nats,
    Analyzer,
    Worker(u32),
}

impl Service {
    pub fn name(&self) -> String {
        todo!()
    }

    pub fn worker_id(&self) -> Option<u32> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunDir {
    root: PathBuf,
}

impl RunDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn workspace(&self) -> PathBuf {
        todo!()
    }

    pub fn plan(&self) -> PathBuf {
        todo!()
    }

    pub fn database(&self) -> PathBuf {
        todo!()
    }

    pub fn logs(&self) -> PathBuf {
        todo!()
    }

    pub fn stdout(&self, service: Service) -> PathBuf {
        let _ = service;
        todo!()
    }

    pub fn stderr(&self, service: Service) -> PathBuf {
        let _ = service;
        todo!()
    }

    pub fn nats_store(&self) -> PathBuf {
        todo!()
    }

    pub fn nats_config(&self) -> PathBuf {
        todo!()
    }

    pub fn compose_file(&self) -> PathBuf {
        todo!()
    }

    pub fn prepare(&self) -> io::Result<()> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AshiatoBinary {
    pub binary: PathBuf,
    pub library_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeSpec {
    pub project: String,
    pub layout: Layout,
    pub cpus: CpuAssignment,
    pub user: String,
    pub run_id: String,
    pub run_dir: RunDir,
    pub ashiato: AshiatoBinary,
    pub minio_volume: String,
}

pub fn compose_file(spec: &ComposeSpec) -> String {
    let _ = spec;
    todo!()
}
