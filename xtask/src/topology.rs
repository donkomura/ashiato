use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

// Physical core 0 runs MinIO and NATS, core 1 the analyzer, and the cores after them one worker
// each (14.1.1 節). The test machine has six physical cores, so it runs at most four workers.
pub const MAX_WORKERS: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cpuset(BTreeSet<u32>);

impl Cpuset {
    pub fn new(cpus: impl IntoIterator<Item = u32>) -> Self {
        Self(cpus.into_iter().collect())
    }

    pub fn cpus(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().copied()
    }
}

impl fmt::Display for Cpuset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = formatter;
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuAssignment {
    pub servers: Cpuset,
    pub analyzer: Cpuset,
    pub workers: Vec<Cpuset>,
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("invalid CPU list {0:?}")]
    InvalidCpuList(String),
    #[error("failed to read {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("no CPU topology was found")]
    NoCpus,
    #[error("{workers} workers were requested, but at most {MAX_WORKERS} are supported")]
    TooManyWorkers { workers: u32 },
    #[error("{needed} physical cores are needed, but only {available} were found")]
    NotEnoughCores { needed: usize, available: usize },
}

pub fn parse_cpu_list(text: &str) -> Result<Cpuset, TopologyError> {
    let _ = text;
    todo!()
}

pub fn physical_cores<'a>(
    sibling_lists: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<Cpuset>, TopologyError> {
    let _ = sibling_lists.into_iter();
    todo!()
}

pub fn read_physical_cores(cpu_dir: &Path) -> Result<Vec<Cpuset>, TopologyError> {
    let _ = cpu_dir;
    todo!()
}

pub fn assign_cpus(cores: &[Cpuset], workers: u32) -> Result<CpuAssignment, TopologyError> {
    let _ = (cores, workers);
    todo!()
}
