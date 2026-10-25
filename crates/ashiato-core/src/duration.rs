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
        let amount = i64::from(self.amount);
        match self.unit {
            DurationUnit::Seconds => TimeDelta::seconds(amount),
            DurationUnit::Minutes => TimeDelta::minutes(amount),
            DurationUnit::Hours => TimeDelta::hours(amount),
            DurationUnit::Days => TimeDelta::days(amount),
        }
    }
}

impl FromStr for Duration {
    type Err = DurationError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || DurationError::Invalid(text.to_owned());
        let split = text.len().checked_sub(1).ok_or_else(invalid)?;
        let (amount, unit) = text.split_at_checked(split).ok_or_else(invalid)?;
        let unit = match unit {
            "s" => DurationUnit::Seconds,
            "m" => DurationUnit::Minutes,
            "h" => DurationUnit::Hours,
            "d" => DurationUnit::Days,
            _ => return Err(invalid()),
        };
        if amount.is_empty() || !amount.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        let amount: u32 = amount.parse().map_err(|_| invalid())?;
        if amount == 0 {
            return Err(invalid());
        }
        Ok(Self { amount, unit })
    }
}

impl TryFrom<String> for Duration {
    type Error = DurationError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let unit = match self.unit {
            DurationUnit::Seconds => "s",
            DurationUnit::Minutes => "m",
            DurationUnit::Hours => "h",
            DurationUnit::Days => "d",
        };
        write!(f, "{}{unit}", self.amount)
    }
}
