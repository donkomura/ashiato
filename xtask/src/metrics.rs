use serde::Deserialize;

// The analyzer line of the metrics in the JSON Lines that `ashiato run` writes to stderr
// (14.1.5 節). Fields that the integration test does not read are ignored.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AnalyzerMetrics {
    pub unix_ms: i64,
    #[serde(rename = "final")]
    pub is_final: bool,
    pub manifest_files: u64,
    pub ingested_files: u64,
    pub global_watermark: Option<String>,
    pub pending_rows: u64,
    pub pending_max_rows: u64,
    pub time_ms: AnalyzerTimes,
    pub phases_unix_ms: AnalyzerPhases,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct AnalyzerTimes {
    pub fetch_wait: u64,
    pub commit: u64,
    pub batch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct AnalyzerPhases {
    pub first_plan_response: Option<i64>,
    pub last_end_committed: Option<i64>,
    pub correlated: Option<i64>,
}

pub fn latest_analyzer(log: &str) -> Option<AnalyzerMetrics> {
    let _ = log;
    todo!()
}

pub fn reached(metrics: &AnalyzerMetrics, percent: u32) -> bool {
    let _ = (metrics, percent);
    todo!()
}
