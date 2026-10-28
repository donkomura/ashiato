use std::fmt;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use ashiato_core::context::{Context, ContextError};
use ashiato_core::{AnalysisWindow, Duration, WindowError};
use chrono::{DateTime, TimeDelta, Utc};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};

const BUILTIN_RULES: &str = "builtin";
// TODO: The lookback is fixed until the context file (stage 18) can set baseline.lookback.
const BASELINE_LOOKBACK: TimeDelta = TimeDelta::days(30);
// These sections only decide the speed and the placement of the run and do not change its
// output, so a run moved to another placement keeps the same digest.
const EXCLUDED_FROM_DIGEST: [&str; 2] = ["placement", "broker"];
const BYTE_UNITS: [(&str, u32); 4] = [("KiB", 10), ("MiB", 20), ("GiB", 30), ("TiB", 40)];

#[derive(Debug, Clone, PartialEq)]
pub struct RunPlan {
    pub run_id: String,
    pub source: Source,
    pub rules: Vec<RuleOrigin>,
    pub ruleset_dirs: Vec<PathBuf>,
    pub rulesets: Vec<String>,
    pub rule: Vec<String>,
    pub pipeline: PipelineSettings,
    pub placement: Placement,
    pub broker: BrokerSettings,
    pub context: Context,
    window: AnalysisWindow,
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub location: SourceLocation,
    pub accounts: Option<Vec<String>>,
    pub regions: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceLocation {
    Local {
        root: PathBuf,
    },
    S3 {
        uri: String,
        endpoint_url: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleOrigin {
    Builtin,
    Directory(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSettings {
    #[serde(default = "default_batch")]
    pub batch: Duration,
    pub allowed_lateness: Duration,
    #[serde(default)]
    pub max_skew: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub mode: PlacementMode,
    #[serde(default)]
    pub nats_url: Option<String>,
    #[serde(default = "default_ingest_workers")]
    pub ingest_workers: NonZeroU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementMode {
    SingleProcess,
    Distributed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrokerSettings {
    #[serde(deserialize_with = "byte_size")]
    pub max_bytes: u64,
    pub max_messages_per_partition: u32,
    #[serde(deserialize_with = "byte_size")]
    pub max_message_size: u64,
    pub duplicate_window: Duration,
    pub ack_wait: Duration,
}

impl Default for BrokerSettings {
    fn default() -> Self {
        Self {
            max_bytes: 50 << 30,
            max_messages_per_partition: 64,
            max_message_size: 1 << 20,
            duplicate_window: constant_duration("10m"),
            ack_wait: constant_duration("10m"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("failed to read run plan {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse run plan")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid analysis_window")]
    Window(#[from] WindowError),
    #[error("source.endpoint_url is only valid for an s3 source")]
    EndpointUrlForLocalSource,
    #[error("failed to compute the digest of the run plan")]
    Digest(#[source] serde_json::Error),
    #[error("failed to read context file {path}")]
    ReadContext {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid context file {path}")]
    Context {
        path: PathBuf,
        #[source]
        source: ContextError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ByteSizeError {
    #[error(
        "invalid byte size {0:?}: expected a positive integer, optionally followed by KiB, MiB, GiB or TiB"
    )]
    Invalid(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlan {
    run_id: String,
    source: RawSource,
    analysis_window: RawWindow,
    #[serde(default = "default_rules")]
    rules: Vec<String>,
    #[serde(default)]
    ruleset_dirs: Vec<PathBuf>,
    #[serde(default)]
    rulesets: Vec<String>,
    #[serde(default)]
    rule: Vec<String>,
    pipeline: PipelineSettings,
    placement: Placement,
    #[serde(default)]
    broker: BrokerSettings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    kind: SourceKind,
    uri: String,
    #[serde(default)]
    endpoint_url: Option<String>,
    #[serde(default)]
    accounts: Option<Vec<String>>,
    #[serde(default)]
    regions: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    Local,
    S3,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWindow {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}

impl RunPlan {
    pub fn load(path: &Path) -> Result<Self, PlanError> {
        let text = fs::read_to_string(path).map_err(|source| PlanError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text, path.parent().unwrap_or(Path::new("")))
    }

    pub fn parse(text: &str, base_dir: &Path) -> Result<Self, PlanError> {
        let raw: RawPlan = serde_yaml::from_str(text)?;
        let location = match raw.source.kind {
            SourceKind::Local if raw.source.endpoint_url.is_some() => {
                return Err(PlanError::EndpointUrlForLocalSource);
            }
            SourceKind::Local => SourceLocation::Local {
                root: resolve(base_dir, Path::new(&raw.source.uri)),
            },
            SourceKind::S3 => SourceLocation::S3 {
                uri: raw.source.uri,
                endpoint_url: raw.source.endpoint_url,
            },
        };
        let window = AnalysisWindow::new(
            raw.analysis_window.since,
            raw.analysis_window.until,
            BASELINE_LOOKBACK,
        )?;
        let rules = raw
            .rules
            .iter()
            .map(|origin| match origin.as_str() {
                BUILTIN_RULES => RuleOrigin::Builtin,
                dir => RuleOrigin::Directory(resolve(base_dir, Path::new(dir))),
            })
            .collect();
        Ok(Self {
            run_id: raw.run_id,
            source: Source {
                location,
                accounts: raw.source.accounts,
                regions: raw.source.regions,
            },
            rules,
            ruleset_dirs: raw
                .ruleset_dirs
                .iter()
                .map(|dir| resolve(base_dir, dir))
                .collect(),
            rulesets: raw.rulesets,
            rule: raw.rule,
            pipeline: raw.pipeline,
            placement: raw.placement,
            broker: raw.broker,
            context: Context::default(),
            window,
            digest: digest(text)?,
        })
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn analysis_window(&self) -> AnalysisWindow {
        self.window
    }
}

pub fn parse_byte_size(text: &str) -> Result<u64, ByteSizeError> {
    let invalid = || ByteSizeError::Invalid(text.to_owned());
    let (amount, shift) = BYTE_UNITS
        .iter()
        .find_map(|(unit, shift)| text.strip_suffix(unit).map(|amount| (amount, *shift)))
        .unwrap_or((text, 0));
    if amount.is_empty() || !amount.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let amount: u64 = amount.parse().map_err(|_| invalid())?;
    if amount == 0 {
        return Err(invalid());
    }
    amount.checked_mul(1 << shift).ok_or_else(invalid)
}

fn resolve(base_dir: &Path, path: &Path) -> PathBuf {
    base_dir.join(path).components().collect()
}

// The digest is taken over the YAML values rather than the text, so that comments, layout
// and key order do not change it.
fn digest(text: &str) -> Result<String, PlanError> {
    let mut plan: serde_yaml::Value = serde_yaml::from_str(text)?;
    if let Some(sections) = plan.as_mapping_mut() {
        for key in EXCLUDED_FROM_DIGEST {
            sections.remove(key);
        }
    }
    let plan = serde_json::to_value(&plan).map_err(PlanError::Digest)?;
    let mut canonical = String::new();
    write_canonical_json(&plan, &mut canonical);
    let hex: String = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(format!("sha256:{hex}"))
}

fn write_canonical_json(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            out.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::from(key.as_str()).to_string());
                out.push(':');
                write_canonical_json(value, out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

fn default_rules() -> Vec<String> {
    vec![BUILTIN_RULES.to_owned()]
}

fn default_batch() -> Duration {
    constant_duration("1h")
}

fn default_ingest_workers() -> NonZeroU32 {
    NonZeroU32::MIN
}

fn constant_duration(text: &str) -> Duration {
    text.parse()
        .expect("default durations are written as valid literals")
}

fn byte_size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct ByteSizeVisitor;

    impl Visitor<'_> for ByteSizeVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a byte size such as 1048576 or 1MiB")
        }

        fn visit_u64<E: de::Error>(self, bytes: u64) -> Result<u64, E> {
            self.visit_str(&bytes.to_string())
        }

        fn visit_str<E: de::Error>(self, text: &str) -> Result<u64, E> {
            parse_byte_size(text).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(ByteSizeVisitor)
}
