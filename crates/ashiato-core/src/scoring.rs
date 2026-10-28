use crate::output::{OriginScoreRecord, SignalRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScoringSettings {
    pub min_tactics: usize,
    pub threshold: u32,
}

impl Default for ScoringSettings {
    fn default() -> Self {
        todo!()
    }
}

pub fn score(_signals: &[SignalRecord], _settings: ScoringSettings) -> Vec<OriginScoreRecord> {
    todo!()
}
