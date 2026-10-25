use chrono::{DateTime, TimeDelta, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalysisWindow {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    baseline_since: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum WindowError {
    #[error("since ({since}) must be earlier than until ({until})")]
    Empty {
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    },
    #[error("baseline lookback must not be negative: {0}")]
    NegativeLookback(TimeDelta),
}

impl AnalysisWindow {
    pub fn new(
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        lookback: TimeDelta,
    ) -> Result<Self, WindowError> {
        if since >= until {
            return Err(WindowError::Empty { since, until });
        }
        if lookback < TimeDelta::zero() {
            return Err(WindowError::NegativeLookback(lookback));
        }
        Ok(Self {
            since,
            until,
            baseline_since: since - lookback,
        })
    }

    pub fn since(&self) -> DateTime<Utc> {
        self.since
    }

    pub fn until(&self) -> DateTime<Utc> {
        self.until
    }

    pub fn baseline_since(&self) -> DateTime<Utc> {
        self.baseline_since
    }

    pub fn baseline_until(&self) -> DateTime<Utc> {
        self.since
    }
}
