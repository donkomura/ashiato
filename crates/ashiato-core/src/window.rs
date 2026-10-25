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
        _since: DateTime<Utc>,
        _until: DateTime<Utc>,
        _lookback: TimeDelta,
    ) -> Result<Self, WindowError> {
        todo!()
    }

    pub fn since(&self) -> DateTime<Utc> {
        todo!()
    }

    pub fn until(&self) -> DateTime<Utc> {
        todo!()
    }

    pub fn baseline_since(&self) -> DateTime<Utc> {
        todo!()
    }

    pub fn baseline_until(&self) -> DateTime<Utc> {
        todo!()
    }
}
