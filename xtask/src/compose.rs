use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ashiato_testkit::minio::{ACCESS_KEY, MINIO_IMAGE, REGION, SECRET_KEY};
use ashiato_testkit::nats::NATS_IMAGE;
use serde::Serialize;

use crate::topology::{CpuAssignment, Cpuset};

// ashiato is built on the host (glibc 2.31) and mounted into a slim image with a newer glibc,
// so that no Rust toolchain is needed in Docker.
pub const RUNTIME_IMAGE: &str = "debian:bookworm-20251229-slim";
pub const NATS_URL: &str = "nats://nats:4222";
pub const MINIO_ENDPOINT: &str = "http://minio:9000";
// TODO: ashiato does not read this variable yet; the analyzer must apply it as the DuckDB
// memory_limit of 14.1.1 節 before the runs are measured.
pub const DUCKDB_MEMORY_LIMIT: &str = "2GB";

const SERVER_MEMORY: &str = "1g";
const ANALYZER_MEMORY: &str = "3g";
const WORKER_MEMORY: &str = "1g";
const ASHIATO: &str = "/opt/ashiato/bin/ashiato";
const LIBRARY_DIR: &str = "/opt/ashiato/lib";
const WORKSPACE: &str = "/work";
const LOGS: &str = "/logs";
const NATS_CONFIG_IN_CONTAINER: &str = "/etc/ashiato/nats.conf";
const MINIO_DATA: &str = "/data";
const NATS_DATA: &str = "/data";
// NATS keeps JetStream under <store_dir>/jetstream. Without sync_interval: always, a PubAck can
// come before the message is on disk, and a restart of the server loses messages (7.1 節).
const NATS_CONFIG: &str = "\
port: 4222
jetstream {
  store_dir: /data
  sync_interval: always
}
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    SingleProcess,
    Distributed { workers: u32 },
}

impl Layout {
    pub fn workers(self) -> u32 {
        match self {
            Self::SingleProcess => 0,
            Self::Distributed { workers } => workers,
        }
    }

    pub fn services(self) -> Vec<Service> {
        let mut services = vec![Service::Minio];
        if self != Self::SingleProcess {
            services.push(Service::Nats);
        }
        services.push(Service::Analyzer);
        services.extend((1..=self.workers()).map(Service::Worker));
        services
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
        match self {
            Self::Minio => "minio".to_owned(),
            Self::Nats => "nats".to_owned(),
            Self::Analyzer => "analyzer".to_owned(),
            Self::Worker(number) => format!("worker-{number}"),
        }
    }

    pub fn worker_id(&self) -> Option<u32> {
        match self {
            Self::Worker(number) => Some(number - 1),
            _ => None,
        }
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
        self.root.join("workspace")
    }

    pub fn plan(&self) -> PathBuf {
        self.workspace().join("run.yaml")
    }

    pub fn database(&self) -> PathBuf {
        self.workspace().join("ashiato.duckdb")
    }

    pub fn logs(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn stdout(&self, service: Service) -> PathBuf {
        self.logs().join(format!("{}.stdout", service.name()))
    }

    pub fn stderr(&self, service: Service) -> PathBuf {
        self.logs().join(format!("{}.stderr", service.name()))
    }

    pub fn nats_store(&self) -> PathBuf {
        self.root.join("nats")
    }

    pub fn nats_config(&self) -> PathBuf {
        self.root.join("nats.conf")
    }

    pub fn compose_file(&self) -> PathBuf {
        self.root.join("compose.yaml")
    }

    // A run must not take over the workspace or the JetStream store of an earlier run
    // (14.1.6 節), so its directory is removed and created again.
    pub fn prepare(&self) -> io::Result<()> {
        match fs::remove_dir_all(&self.root) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        for dir in [self.workspace(), self.logs(), self.nats_store()] {
            fs::create_dir_all(dir)?;
        }
        fs::write(self.nats_config(), NATS_CONFIG)
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

#[derive(Serialize)]
struct ComposeFile {
    name: String,
    services: BTreeMap<String, ServiceConfig>,
    volumes: BTreeMap<String, ExternalVolume>,
}

#[derive(Serialize)]
struct ExternalVolume {
    external: bool,
}

#[derive(Serialize)]
struct ServiceConfig {
    image: &'static str,
    user: String,
    cpuset: String,
    mem_limit: &'static str,
    memswap_limit: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    entrypoint: Vec<String>,
    command: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    environment: BTreeMap<&'static str, String>,
    volumes: Vec<String>,
}

pub fn compose_file(spec: &ComposeSpec) -> String {
    let services = spec
        .layout
        .services()
        .into_iter()
        .map(|service| (service.name(), service_config(spec, service)))
        .collect();
    let file = ComposeFile {
        name: spec.project.clone(),
        services,
        volumes: BTreeMap::from([(spec.minio_volume.clone(), ExternalVolume { external: true })]),
    };
    serde_yaml::to_string(&file).expect("a compose file is plain strings and maps")
}

fn service_config(spec: &ComposeSpec, service: Service) -> ServiceConfig {
    let dir = &spec.run_dir;
    match service {
        // The MinIO volume is created by Docker and owned by root, unlike the directories of a
        // run, and it is kept over all runs so that every run reads the same object versions.
        Service::Minio => ServiceConfig {
            entrypoint: vec!["minio".to_owned()],
            command: vec!["server".to_owned(), MINIO_DATA.to_owned()],
            environment: BTreeMap::from([
                ("MINIO_ROOT_USER", ACCESS_KEY.to_owned()),
                ("MINIO_ROOT_PASSWORD", SECRET_KEY.to_owned()),
            ]),
            volumes: vec![format!("{}:{MINIO_DATA}", spec.minio_volume)],
            ..server(MINIO_IMAGE, "0:0".to_owned(), &spec.cpus.servers)
        },
        Service::Nats => ServiceConfig {
            command: vec!["--config".to_owned(), NATS_CONFIG_IN_CONTAINER.to_owned()],
            volumes: vec![
                format!(
                    "{}:{NATS_CONFIG_IN_CONTAINER}:ro",
                    dir.nats_config().display()
                ),
                format!("{}:{NATS_DATA}", dir.nats_store().display()),
            ],
            ..server(NATS_IMAGE, spec.user.clone(), &spec.cpus.servers)
        },
        Service::Analyzer => {
            let mut config = ashiato(
                spec,
                &spec.cpus.analyzer,
                ANALYZER_MEMORY,
                // The output is truncated when the analyzer is started again, so that it holds
                // only the run that finished, while the metrics of every start are kept.
                format!(
                    "run --plan {WORKSPACE}/run.yaml --emit-signals \
                     >{LOGS}/analyzer.stdout 2>>{LOGS}/analyzer.stderr"
                ),
            );
            config.environment.insert(
                "ASHIATO_DUCKDB_MEMORY_LIMIT",
                DUCKDB_MEMORY_LIMIT.to_owned(),
            );
            config
                .volumes
                .push(format!("{}:{WORKSPACE}", dir.workspace().display()));
            config
        }
        Service::Worker(number) => {
            let name = service.name();
            ashiato(
                spec,
                &spec.cpus.workers[number as usize - 1],
                WORKER_MEMORY,
                format!(
                    "ingest-worker --nats {NATS_URL} --run {} --worker-id {} \
                     >>{LOGS}/{name}.stdout 2>>{LOGS}/{name}.stderr",
                    spec.run_id,
                    number - 1
                ),
            )
        }
    }
}

fn server(image: &'static str, user: String, cpuset: &Cpuset) -> ServiceConfig {
    ServiceConfig {
        image,
        user,
        cpuset: cpuset.to_string(),
        mem_limit: SERVER_MEMORY,
        memswap_limit: SERVER_MEMORY,
        entrypoint: Vec::new(),
        command: Vec::new(),
        environment: BTreeMap::new(),
        volumes: Vec::new(),
    }
}

// The containers run as the user of the host, so that the files they leave in the run
// directory can be removed by the next run.
fn ashiato(
    spec: &ComposeSpec,
    cpuset: &Cpuset,
    memory: &'static str,
    arguments: String,
) -> ServiceConfig {
    ServiceConfig {
        image: RUNTIME_IMAGE,
        user: spec.user.clone(),
        cpuset: cpuset.to_string(),
        mem_limit: memory,
        memswap_limit: memory,
        entrypoint: vec!["/bin/sh".to_owned(), "-c".to_owned()],
        command: vec![format!("exec {ASHIATO} {arguments}")],
        environment: BTreeMap::from([
            ("AWS_ACCESS_KEY_ID", ACCESS_KEY.to_owned()),
            ("AWS_SECRET_ACCESS_KEY", SECRET_KEY.to_owned()),
            ("AWS_REGION", REGION.to_owned()),
            ("AWS_EC2_METADATA_DISABLED", "true".to_owned()),
            ("LD_LIBRARY_PATH", LIBRARY_DIR.to_owned()),
        ]),
        volumes: vec![
            format!("{}:{ASHIATO}:ro", spec.ashiato.binary.display()),
            format!("{}:{LIBRARY_DIR}:ro", spec.ashiato.library_dir.display()),
            format!("{}:{LOGS}", spec.run_dir.logs().display()),
        ],
    }
}
