use chrono::{TimeDelta, TimeZone, Utc};

use crate::AnalysisWindow;

const FIXTURE_BASELINE_DAYS: i64 = 30;

// The analysis window covers the day of the default eventTime of RecordBuilder,
// so that records which do not set eventTime fall inside it. Records for the
// baseline period are written with an eventTime in the preceding 30 days.
pub fn fixture_window() -> AnalysisWindow {
    let since = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    AnalysisWindow::new(
        since,
        since + TimeDelta::days(1),
        TimeDelta::days(FIXTURE_BASELINE_DAYS),
    )
    .expect("the fixture window is a fixed, non-empty window")
}
