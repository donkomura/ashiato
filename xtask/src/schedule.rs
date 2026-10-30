use std::time::Duration;

use crate::compose::{Layout, Service};
use crate::metrics::AnalyzerMetrics;

pub const REFERENCE: &str = "r0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    RestartWorker {
        worker: u32,
        percent: u32,
        down_for: Duration,
    },
    RestartNats {
        percent: u32,
    },
    RestartAnalyzer {
        percent: u32,
    },
    DelayWorker {
        worker: u32,
        delay: Duration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Ingested { percent: u32 },
    After(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Kill(Service),
    Pause(Duration),
    Start(Service),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSpec {
    pub name: &'static str,
    pub layout: Layout,
    pub fault: Option<Fault>,
}

pub fn correctness_runs() -> Vec<RunSpec> {
    todo!()
}

impl RunSpec {
    pub fn servers(&self) -> Vec<Service> {
        todo!()
    }

    pub fn roles(&self) -> Vec<Service> {
        todo!()
    }
}

impl Fault {
    pub fn trigger(self) -> Trigger {
        todo!()
    }

    pub fn steps(self) -> Vec<Step> {
        todo!()
    }

    pub fn held_back(self) -> Option<Service> {
        todo!()
    }
}

impl Trigger {
    pub fn is_due(self, metrics: Option<&AnalyzerMetrics>, since_start: Duration) -> bool {
        let _ = (metrics, since_start);
        todo!()
    }
}
