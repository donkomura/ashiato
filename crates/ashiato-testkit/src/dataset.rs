use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::log_file::LogFile;
use rng::{SplitMix64, hash, mix};
use scenario::{Contents, RawFile};

mod background;
mod event;
mod rng;
mod scenario;

const ORG_ID: &str = "o-ashiato0001";
const ACCOUNTS: [&str; 4] = [
    "111111111111",
    "222222222222",
    "333333333333",
    "444444444444",
];
const A_REGIONS: [&str; 3] = ["us-east-1", "us-west-2", "ap-northeast-1"];
const B_REGIONS: [&str; 6] = [
    "us-east-1",
    "us-west-2",
    "ap-northeast-1",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
];
const BASELINE_DAYS: i64 = 7;
const ANALYSIS_DAYS: i64 = 7;
const LOOKBACK: &str = "7d";
// One hour is a multiple of every time_bin of the builtin rules (5m, 10m, 1h).
const BATCH: &str = "1h";
const ALLOWED_LATENESS: &str = "30m";
// Small enough that the partition with ten times the events holds the others back.
const MAX_SKEW: &str = "1h";
// CloudTrail delivers a file for each account and region about every five minutes.
const DELIVERY_MINUTES: f64 = 5.0;
const MAX_INTERVAL_MINUTES: f64 = 24.0 * 60.0;
// Events in a file at scale 1.0, chosen so that A is a few hundred MB and B about 2 GB
// after compression.
const A_EVENTS_PER_FILE: f64 = 25.0;
const B_EVENTS_PER_FILE: f64 = 155.0;
const HEAVY_FACTOR: u64 = 10;
// Every file of B holds the same number of events, so that the partitions stay equal, and a
// file must have room for the scripted events delivered in it (at most five).
const B_MIN_EVENTS_PER_FILE: u64 = 8;
const UNIQUE_LENGTH: usize = 16;
const SUMMARY_FILE: &str = "dataset.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    A,
    ANoDataEvents,
    B,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatasetSpec {
    pub kind: DatasetKind,
    pub seed: u64,
    pub scale: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub kind: DatasetKind,
    pub seed: u64,
    pub scale: f64,
    pub org_id: String,
    pub accounts: Vec<String>,
    pub regions: Vec<String>,
    pub files: u64,
    pub compressed_bytes: u64,
    pub events: u64,
    pub baseline_since: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub lookback: String,
    pub batch: String,
    pub allowed_lateness: String,
    pub max_skew: String,
    pub partitions: Vec<PartitionSummary>,
    pub expected_scenarios: Vec<ExpectedScenario>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSummary {
    pub account: String,
    pub region: String,
    pub files: u64,
    pub compressed_bytes: u64,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedScenario {
    pub scenario_id: String,
    pub origin: ExpectedOrigin,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedOrigin {
    pub id: String,
    pub kind: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("scale must be a positive finite number, but got {0}")]
    InvalidScale(f64),
    #[error("output directory {0} is not empty")]
    NotEmpty(PathBuf),
    #[error("failed to write {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Partition {
    account: &'static str,
    region: &'static str,
}

struct Layout {
    regions: &'static [&'static str],
    partitions: Vec<Partition>,
    empty: Option<Partition>,
    heavy: Option<Partition>,
    files: Vec<DateTime<Utc>>,
    events_per_file: u64,
    same_files_everywhere: bool,
}

struct FilePlan {
    partition: Partition,
    log: LogFile,
    content: Content,
}

#[derive(Clone)]
enum Content {
    Events {
        seed: u64,
        background: u64,
        scripted: Vec<Value>,
    },
    Raw(Vec<u8>),
}

struct Written {
    bytes: u64,
    events: u64,
}

pub fn generate(spec: &DatasetSpec, out_dir: &Path) -> Result<DatasetSummary, DatasetError> {
    if !(spec.scale.is_finite() && spec.scale > 0.0) {
        return Err(DatasetError::InvalidScale(spec.scale));
    }
    prepare(out_dir)?;
    let layout = Layout::new(spec);
    let contents = scenario::contents(spec.kind, spec.seed);
    let plans = layout.plans(spec.seed, &contents);
    let written = write_files(out_dir, &plans, spec.kind == DatasetKind::ANoDataEvents)?;
    let summary = layout.summary(spec, &plans, &written, contents.expected);
    let path = out_dir.join(SUMMARY_FILE);
    let content = serde_json::to_vec_pretty(&summary).expect("a summary is serializable");
    fs::write(&path, content).map_err(|source| DatasetError::Io { path, source })?;
    Ok(summary)
}

fn prepare(out_dir: &Path) -> Result<(), DatasetError> {
    let io_error = |source| DatasetError::Io {
        path: out_dir.to_path_buf(),
        source,
    };
    fs::create_dir_all(out_dir).map_err(io_error)?;
    if fs::read_dir(out_dir).map_err(io_error)?.next().is_some() {
        return Err(DatasetError::NotEmpty(out_dir.to_path_buf()));
    }
    Ok(())
}

fn baseline_since() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()
}

fn since() -> DateTime<Utc> {
    baseline_since() + TimeDelta::days(BASELINE_DAYS)
}

fn until() -> DateTime<Utc> {
    since() + TimeDelta::days(ANALYSIS_DAYS)
}

impl Layout {
    // A smaller scale delivers files less often, so that a small dataset still spans both
    // periods with files of a realistic size.
    fn new(spec: &DatasetSpec) -> Self {
        let regions: &'static [&'static str] = match spec.kind {
            DatasetKind::A | DatasetKind::ANoDataEvents => &A_REGIONS,
            DatasetKind::B => &B_REGIONS,
        };
        let partitions = ACCOUNTS
            .iter()
            .flat_map(|account| regions.iter().map(|region| Partition { account, region }))
            .collect();
        let interval = (DELIVERY_MINUTES / spec.scale)
            .clamp(DELIVERY_MINUTES, MAX_INTERVAL_MINUTES)
            .round();
        let per_file = match spec.kind {
            DatasetKind::A | DatasetKind::ANoDataEvents => A_EVENTS_PER_FILE,
            DatasetKind::B => B_EVENTS_PER_FILE,
        };
        let mut events_per_file = (per_file * spec.scale * interval / DELIVERY_MINUTES)
            .round()
            .max(1.0) as u64;
        let same_files_everywhere = spec.kind == DatasetKind::B;
        if same_files_everywhere {
            events_per_file = events_per_file.max(B_MIN_EVENTS_PER_FILE);
        }
        let interval = TimeDelta::minutes(interval as i64);
        let files = std::iter::successors(Some(baseline_since() + interval), |time| {
            Some(*time + interval).filter(|next| *next < until())
        })
        .collect();
        let (empty, heavy) = match spec.kind {
            DatasetKind::A | DatasetKind::ANoDataEvents => (
                Some(Partition {
                    account: ACCOUNTS[3],
                    region: A_REGIONS[2],
                }),
                Some(Partition {
                    account: ACCOUNTS[0],
                    region: A_REGIONS[0],
                }),
            ),
            DatasetKind::B => (None, None),
        };
        Self {
            regions,
            partitions,
            empty,
            heavy,
            files,
            events_per_file,
            same_files_everywhere,
        }
    }

    fn events_per_file(&self, partition: Partition) -> u64 {
        if Some(partition) == self.heavy {
            HEAVY_FACTOR * self.events_per_file
        } else {
            self.events_per_file
        }
    }

    // Scripted events are delivered in the files of the regular deliveries when their times
    // meet, and in files of their own otherwise. B gives every partition a file at each of
    // those times, so that all partitions have the same files and events.
    fn plans(&self, seed: u64, contents: &Contents) -> Vec<FilePlan> {
        let scripted_times: BTreeSet<DateTime<Utc>> = if self.same_files_everywhere {
            contents
                .events
                .iter()
                .map(|event| event.file_time)
                .collect()
        } else {
            BTreeSet::new()
        };
        let mut files: BTreeMap<(Partition, DateTime<Utc>), Vec<Value>> = BTreeMap::new();
        for &partition in &self.partitions {
            if Some(partition) != self.empty {
                for &time in self.files.iter().chain(&scripted_times) {
                    files.entry((partition, time)).or_default();
                }
            }
        }
        for event in &contents.events {
            files
                .entry((event.partition, event.file_time))
                .or_default()
                .push(event.record.clone());
        }
        let mut plans: Vec<FilePlan> = files
            .into_iter()
            .map(|((partition, time), scripted)| {
                let background = self
                    .events_per_file(partition)
                    .saturating_sub(scripted.len() as u64);
                FilePlan::events(seed, partition, time, background, scripted)
            })
            .collect();
        if let Some(trail) = &contents.account_trail {
            let copies: Vec<FilePlan> = plans
                .iter()
                .filter(|plan| {
                    plan.partition == trail.partition
                        && (trail.start..=trail.end).contains(&plan.log.time)
                })
                .map(FilePlan::account_trail_copy)
                .collect();
            plans.extend(copies);
        }
        plans.extend(
            contents
                .raw_files
                .iter()
                .map(|raw| FilePlan::raw(seed, raw)),
        );
        plans
    }

    fn summary(
        &self,
        spec: &DatasetSpec,
        plans: &[FilePlan],
        written: &[Written],
        expected_scenarios: Vec<ExpectedScenario>,
    ) -> DatasetSummary {
        let mut partitions: Vec<PartitionSummary> = self
            .partitions
            .iter()
            .map(|partition| PartitionSummary {
                account: partition.account.to_owned(),
                region: partition.region.to_owned(),
                files: 0,
                compressed_bytes: 0,
                events: 0,
            })
            .collect();
        for (plan, written) in plans.iter().zip(written) {
            let index = self
                .partitions
                .iter()
                .position(|partition| *partition == plan.partition)
                .expect("every file belongs to a partition of the layout");
            partitions[index].files += 1;
            partitions[index].compressed_bytes += written.bytes;
            partitions[index].events += written.events;
        }
        DatasetSummary {
            kind: spec.kind,
            seed: spec.seed,
            scale: spec.scale,
            org_id: ORG_ID.to_owned(),
            accounts: ACCOUNTS
                .iter()
                .map(|account| (*account).to_owned())
                .collect(),
            regions: self
                .regions
                .iter()
                .map(|region| (*region).to_owned())
                .collect(),
            files: partitions.iter().map(|partition| partition.files).sum(),
            compressed_bytes: partitions
                .iter()
                .map(|partition| partition.compressed_bytes)
                .sum(),
            events: partitions.iter().map(|partition| partition.events).sum(),
            baseline_since: baseline_since(),
            since: since(),
            until: until(),
            lookback: LOOKBACK.to_owned(),
            batch: BATCH.to_owned(),
            allowed_lateness: ALLOWED_LATENESS.to_owned(),
            max_skew: MAX_SKEW.to_owned(),
            partitions,
            expected_scenarios,
        }
    }
}

impl FilePlan {
    fn events(
        seed: u64,
        partition: Partition,
        time: DateTime<Utc>,
        background: u64,
        scripted: Vec<Value>,
    ) -> Self {
        let seed = file_seed(seed, partition, time);
        Self {
            partition,
            log: organization_trail_file(seed, partition, time),
            content: Content::Events {
                seed,
                background,
                scripted,
            },
        }
    }

    fn raw(seed: u64, raw: &RawFile) -> Self {
        let seed = file_seed(seed, raw.partition, raw.file_time);
        Self {
            partition: raw.partition,
            log: organization_trail_file(seed, raw.partition, raw.file_time),
            content: Content::Raw(raw.content.clone()),
        }
    }

    // The account trail delivers the same events a minute after the organization trail.
    fn account_trail_copy(&self) -> Self {
        let Content::Events { seed, .. } = &self.content else {
            unreachable!("only files of events are copied to the account trail");
        };
        let unique =
            SplitMix64::new(mix(&[*seed, hash("account trail")])).alphanumeric(UNIQUE_LENGTH);
        Self {
            partition: self.partition,
            log: LogFile {
                org_id: None,
                unique,
                time: self.log.time + TimeDelta::minutes(1),
                ..self.log.clone()
            },
            content: self.content.clone(),
        }
    }

    fn content(&self, no_data_events: bool) -> (Vec<u8>, u64) {
        let (seed, background, scripted) = match &self.content {
            Content::Raw(content) => return (content.clone(), 0),
            Content::Events {
                seed,
                background,
                scripted,
            } => (*seed, *background, scripted),
        };
        let mut rng = SplitMix64::new(seed);
        let Partition { account, region } = self.partition;
        let mut records: Vec<Value> = (0..background)
            .map(|_| background::record(&mut rng, account, region, self.log.time))
            .chain(scripted.iter().cloned())
            .collect();
        if no_data_events {
            records.retain(|record| !is_s3_data_event(record));
        }
        records.sort_by(|a, b| a["eventTime"].as_str().cmp(&b["eventTime"].as_str()));
        let events = records.len() as u64;
        let json =
            serde_json::to_vec(&json!({ "Records": records })).expect("records are serializable");
        (gzip(&json), events)
    }
}

fn file_seed(seed: u64, partition: Partition, time: DateTime<Utc>) -> u64 {
    mix(&[
        seed,
        hash(partition.account),
        hash(partition.region),
        time.timestamp() as u64,
    ])
}

fn organization_trail_file(seed: u64, partition: Partition, time: DateTime<Utc>) -> LogFile {
    LogFile {
        org_id: Some(ORG_ID.to_owned()),
        unique: SplitMix64::new(mix(&[seed, hash("unique")])).alphanumeric(UNIQUE_LENGTH),
        ..LogFile::new(partition.account, partition.region, time)
    }
}

fn is_s3_data_event(record: &Value) -> bool {
    record["eventSource"] == "s3.amazonaws.com" && record["eventCategory"] == "Data"
}

// GzEncoder writes a zero modification time and no file name into the header, so the same
// content always gives the same bytes.
fn gzip(content: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(content)
        .and_then(|()| encoder.finish())
        .expect("writing to a Vec does not fail")
}

// Each file is made from its own seed, so writing the files in parallel and in any order
// gives the same bytes.
fn write_files(
    out_dir: &Path,
    plans: &[FilePlan],
    no_data_events: bool,
) -> Result<Vec<Written>, DatasetError> {
    let next = AtomicUsize::new(0);
    let threads = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let results: Vec<Result<Vec<(usize, Written)>, DatasetError>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut written = Vec::new();
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(plan) = plans.get(index) else {
                            return Ok(written);
                        };
                        written.push((index, write_file(out_dir, plan, no_data_events)?));
                    }
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("a file writer does not panic"))
            .collect()
    });
    let mut written = Vec::with_capacity(plans.len());
    for result in results {
        written.extend(result?);
    }
    written.sort_by_key(|(index, _)| *index);
    Ok(written.into_iter().map(|(_, written)| written).collect())
}

fn write_file(
    out_dir: &Path,
    plan: &FilePlan,
    no_data_events: bool,
) -> Result<Written, DatasetError> {
    let (content, events) = plan.content(no_data_events);
    let path = out_dir.join(plan.log.relative_path());
    let io_error = |source| DatasetError::Io {
        path: path.clone(),
        source,
    };
    let dir = path.parent().expect("a log file path has a directory");
    fs::create_dir_all(dir).map_err(io_error)?;
    fs::write(&path, &content).map_err(io_error)?;
    Ok(Written {
        bytes: content.len() as u64,
        events,
    })
}
