use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use ashiato_core::{AnalysisWindow, Duration, WindowError};
use serde::{Deserialize, Deserializer};

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
        todo!()
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
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ByteSizeError {
    #[error(
        "invalid byte size {0:?}: expected a positive integer, optionally followed by KiB, MiB, GiB or TiB"
    )]
    Invalid(String),
}

impl RunPlan {
    pub fn load(_path: &Path) -> Result<Self, PlanError> {
        todo!()
    }

    pub fn parse(_text: &str, _base_dir: &Path) -> Result<Self, PlanError> {
        todo!()
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn analysis_window(&self) -> AnalysisWindow {
        self.window
    }
}

pub fn parse_byte_size(_text: &str) -> Result<u64, ByteSizeError> {
    todo!()
}

fn default_batch() -> Duration {
    todo!()
}

fn default_ingest_workers() -> NonZeroU32 {
    todo!()
}

fn byte_size<'de, D: Deserializer<'de>>(_deserializer: D) -> Result<u64, D::Error> {
    todo!()
}
