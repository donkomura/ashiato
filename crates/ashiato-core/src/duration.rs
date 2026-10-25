use std::fmt;
use std::str::FromStr;

use chrono::TimeDelta;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct Duration {
    amount: u32,
    unit: DurationUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurationError {
    #[error("invalid duration {0:?}: expected a positive integer followed by s, m, h or d")]
    Invalid(String),
}

impl Duration {
    pub fn amount(&self) -> u32 {
        self.amount
    }

    pub fn unit(&self) -> DurationUnit {
        self.unit
    }

    pub fn to_time_delta(&self) -> TimeDelta {
        todo!()
    }
}

impl FromStr for Duration {
    type Err = DurationError;

    fn from_str(_text: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl TryFrom<String> for Duration {
    type Error = DurationError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
