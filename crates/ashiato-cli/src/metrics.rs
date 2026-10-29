use std::time::Duration;

use ashiato_pipeline::unix_ms;
use serde_json::Value;
use tokio::task::JoinHandle;

// The metrics go to stderr so that stdout holds only the results of the analysis (14.1.5 節).
pub(crate) fn write(lines: impl IntoIterator<Item = Value>) {
    for line in lines {
        eprintln!("{line}");
    }
}

// Writes the lines every second until it is stopped or dropped.
pub(crate) struct Reporter(JoinHandle<()>);

impl Reporter {
    pub(crate) fn every_second(lines: impl Fn(u64) -> Vec<Value> + Send + 'static) -> Self {
        Self(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await;
            loop {
                interval.tick().await;
                write(lines(unix_ms()));
            }
        }))
    }

    // Waits for the task to end, so that none of its lines comes after the final one.
    pub(crate) async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        self.0.abort();
    }
}
