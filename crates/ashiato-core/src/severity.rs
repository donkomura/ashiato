use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown severity {0:?}; expected one of low, medium, high, critical")]
pub struct UnknownSeverity(pub String);

impl FromStr for Severity {
    type Err = UnknownSeverity;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "critical" => Ok(Self::Critical),
            _ => Err(UnknownSeverity(name.to_owned())),
        }
    }
}
