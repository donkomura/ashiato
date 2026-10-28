use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};

use ashiato_pipeline::{
    EventTime, Manifest, ManifestEntry, PartitionId, PartitionManifest, RunPlan,
};
use chrono::NaiveDateTime;

const LOG_FILE_SUFFIX: &str = ".json.gz";
const EXCLUDED_DIRECTORIES: [&str; 2] = ["CloudTrail-Digest", "CloudTrail-Insight"];
const ACCOUNT_ID_LENGTH: usize = 12;
const FILE_TIME_FORMAT: &str = "%Y%m%dT%H%MZ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListScope<'a> {
    pub accounts: Option<&'a [String]>,
    pub regions: Option<&'a [String]>,
    pub period: Range<EventTime>,
}

impl<'a> ListScope<'a> {
    pub fn from_plan(plan: &'a RunPlan) -> Self {
        let window = plan.analysis_window();
        Self {
            accounts: plan.source.accounts.as_deref(),
            regions: plan.source.regions.as_deref(),
            period: window.baseline_since()..window.until(),
        }
    }

    fn given_partitions(&self) -> Vec<PartitionId> {
        let (Some(accounts), Some(regions)) = (self.accounts, self.regions) else {
            return Vec::new();
        };
        accounts
            .iter()
            .flat_map(|account| {
                regions
                    .iter()
                    .map(move |region| PartitionId::new(account, region))
            })
            .collect()
    }

    fn includes(&self, file: &LogFileName) -> bool {
        let partition = &file.partition;
        self.accounts
            .is_none_or(|accounts| accounts.contains(&partition.account))
            && self
                .regions
                .is_none_or(|regions| regions.contains(&partition.region))
            && self.period.contains(&file.file_time)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalListing {
    pub manifest: Manifest,
    pub unrecognized: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum ListError {
    #[error("failed to read {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log file path {0:?} is not valid UTF-8")]
    NonUtf8Path(PathBuf),
}

pub(crate) struct LogFileName {
    partition: PartitionId,
    file_time: EventTime,
    name: String,
}

pub fn list_local(root: &Path, scope: &ListScope<'_>) -> Result<LocalListing, ListError> {
    let root = std::path::absolute(root).map_err(|source| ListError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    let mut paths = Vec::new();
    collect_log_files(&root, &mut paths)?;

    let mut found = Vec::new();
    let mut unrecognized = Vec::new();
    for path in paths {
        match path
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(parse_file_name)
        {
            Some(name) => found.push((name, path)),
            None => unrecognized.push(path),
        }
    }
    unrecognized.sort();

    let partitions = arrange(scope, found)
        .into_iter()
        .map(|(partition, files)| {
            Ok(PartitionManifest {
                partition,
                files: manifest_entries(files)?,
            })
        })
        .collect::<Result<_, ListError>>()?;
    Ok(LocalListing {
        manifest: Manifest { partitions },
        unrecognized,
    })
}

// Files of a partition are read in the order of their file time, and of their file name when
// the times are equal, so that the files of the organization trail and the account trail of a
// partition are merged into one sequence (5.1 節, 6.1 節).
pub(crate) fn arrange<T: Ord>(
    scope: &ListScope<'_>,
    files: Vec<(LogFileName, T)>,
) -> Vec<(PartitionId, Vec<(EventTime, T)>)> {
    let mut partitions: BTreeMap<PartitionId, Vec<(EventTime, String, T)>> = scope
        .given_partitions()
        .into_iter()
        .map(|partition| (partition, Vec::new()))
        .collect();
    for (name, file) in files {
        if scope.includes(&name) {
            partitions
                .entry(name.partition)
                .or_default()
                .push((name.file_time, name.name, file));
        }
    }
    partitions
        .into_iter()
        .map(|(partition, mut files)| {
            files.sort();
            let files = files
                .into_iter()
                .map(|(file_time, _, file)| (file_time, file))
                .collect();
            (partition, files)
        })
        .collect()
}

fn manifest_entries(files: Vec<(EventTime, PathBuf)>) -> Result<Vec<ManifestEntry>, ListError> {
    (0..)
        .zip(files)
        .map(|(file_seq, (file_time, path))| {
            let size = fs::metadata(&path)
                .map_err(|source| ListError::Io {
                    path: path.clone(),
                    source,
                })?
                .len();
            let uri = path
                .into_os_string()
                .into_string()
                .map_err(|path| ListError::NonUtf8Path(path.into()))?;
            Ok(ManifestEntry {
                file_seq,
                uri,
                version_id: None,
                etag: None,
                size,
                file_time,
            })
        })
        .collect()
}

fn collect_log_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), ListError> {
    let io_error = |source| ListError::Io {
        path: dir.to_path_buf(),
        source,
    };
    for entry in fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let path = entry.path();
        if entry.file_type().map_err(io_error)?.is_dir() {
            if !entry
                .file_name()
                .to_str()
                .is_some_and(is_excluded_directory)
            {
                collect_log_files(&path, files)?;
            }
        } else if is_log_file(&path.to_string_lossy()) {
            files.push(path);
        }
    }
    Ok(())
}

pub(crate) fn is_excluded_directory(name: &str) -> bool {
    EXCLUDED_DIRECTORIES.contains(&name)
}

pub(crate) fn is_log_file(name: &str) -> bool {
    name.ends_with(LOG_FILE_SUFFIX)
}

pub(crate) fn is_account_id(segment: &str) -> bool {
    segment.len() == ACCOUNT_ID_LENGTH && segment.bytes().all(|byte| byte.is_ascii_digit())
}

pub(crate) fn is_region(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

// CloudTrail names log files <account>_CloudTrail_<region>_<YYYYMMDDTHHmmZ>_<unique>.json.gz.
// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/get-and-view-cloudtrail-log-files.html
pub(crate) fn parse_file_name(name: &str) -> Option<LogFileName> {
    let stem = name.strip_suffix(LOG_FILE_SUFFIX)?;
    let mut fields = stem.splitn(5, '_');
    let (account, service, region, time, unique) = (
        fields.next()?,
        fields.next()?,
        fields.next()?,
        fields.next()?,
        fields.next()?,
    );
    let valid = is_account_id(account)
        && service == "CloudTrail"
        && is_region(region)
        && !unique.is_empty();
    if !valid {
        return None;
    }
    let file_time = NaiveDateTime::parse_from_str(time, FILE_TIME_FORMAT)
        .ok()?
        .and_utc();
    Some(LogFileName {
        partition: PartitionId::new(account, region),
        file_time,
        name: name.to_owned(),
    })
}
