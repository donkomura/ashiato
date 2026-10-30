use std::collections::BTreeSet;
use std::fmt;
use std::fs;
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

    pub fn count(&self) -> usize {
        todo!()
    }
}

impl fmt::Display for Cpuset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cpus: Vec<String> = self.0.iter().map(u32::to_string).collect();
        formatter.write_str(&cpus.join(","))
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
    let invalid = || TopologyError::InvalidCpuList(text.to_owned());
    let mut cpus = BTreeSet::new();
    for range in text.trim_end().split(',') {
        let (first, last) = range.split_once('-').unwrap_or((range, range));
        let first: u32 = first.parse().map_err(|_| invalid())?;
        let last: u32 = last.parse().map_err(|_| invalid())?;
        if first > last {
            return Err(invalid());
        }
        cpus.extend(first..=last);
    }
    Ok(Cpuset(cpus))
}

// Every logical CPU of a physical core lists the same siblings, so the distinct lists are the
// physical cores. Ordering them by their lowest CPU numbers the cores as the kernel does.
pub fn physical_cores<'a>(
    sibling_lists: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<Cpuset>, TopologyError> {
    let cores = sibling_lists
        .into_iter()
        .map(parse_cpu_list)
        .collect::<Result<BTreeSet<_>, _>>()?;
    if cores.is_empty() {
        return Err(TopologyError::NoCpus);
    }
    Ok(cores.into_iter().collect())
}

// Offline CPUs have no topology directory, so they are left out rather than failing the read.
pub fn read_physical_cores(cpu_dir: &Path) -> Result<Vec<Cpuset>, TopologyError> {
    let read_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| TopologyError::Read { path, source }
    };
    let mut lists = Vec::new();
    for entry in fs::read_dir(cpu_dir).map_err(read_error(cpu_dir))? {
        let entry = entry.map_err(read_error(cpu_dir))?;
        let name = entry.file_name();
        let is_cpu = name
            .to_str()
            .and_then(|name| name.strip_prefix("cpu"))
            .is_some_and(|number| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            });
        let siblings = entry.path().join("topology").join("thread_siblings_list");
        if is_cpu && siblings.is_file() {
            lists.push(fs::read_to_string(&siblings).map_err(read_error(&siblings))?);
        }
    }
    physical_cores(lists.iter().map(String::as_str))
}

pub fn assign_cpus(cores: &[Cpuset], workers: u32) -> Result<CpuAssignment, TopologyError> {
    if workers > MAX_WORKERS {
        return Err(TopologyError::TooManyWorkers { workers });
    }
    let needed = 2 + workers as usize;
    if cores.len() < needed {
        return Err(TopologyError::NotEnoughCores {
            needed,
            available: cores.len(),
        });
    }
    Ok(CpuAssignment {
        servers: cores[0].clone(),
        analyzer: cores[1].clone(),
        workers: cores[2..needed].to_vec(),
    })
}
