use std::time::Duration;

use crate::compose::{Layout, Service};
use crate::metrics::{AnalyzerMetrics, reached};

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

// The fault runs of 14.1.3 節 all run the four workers of D4.
const FAULT_LAYOUT: Layout = Layout::Distributed { workers: 4 };

pub fn correctness_runs() -> Vec<RunSpec> {
    let reference = RunSpec {
        name: REFERENCE,
        layout: Layout::SingleProcess,
        fault: None,
    };
    let distributed = ["d1", "d2", "d3", "d4"]
        .into_iter()
        .zip(1..)
        .map(|(name, workers)| RunSpec {
            name,
            layout: Layout::Distributed { workers },
            fault: None,
        });
    let faults = [
        (
            "f1",
            Fault::RestartWorker {
                worker: 2,
                percent: 30,
                down_for: Duration::from_secs(10),
            },
        ),
        ("f2", Fault::RestartNats { percent: 50 }),
        ("f3", Fault::RestartAnalyzer { percent: 70 }),
        (
            "f4",
            Fault::DelayWorker {
                worker: 3,
                delay: Duration::from_secs(60),
            },
        ),
    ]
    .into_iter()
    .map(|(name, fault)| RunSpec {
        name,
        layout: FAULT_LAYOUT,
        fault: Some(fault),
    });
    std::iter::once(reference)
        .chain(distributed)
        .chain(faults)
        .collect()
}

// The warm-up run of each number of workers is compared with R0 but not measured (14.1.4 節).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalingRun {
    pub spec: RunSpec,
    pub measured: bool,
}

pub fn scaling_runs(_workers: &[u32], _repeat: u32) -> Vec<ScalingRun> {
    todo!()
}

// ashiato connects to MinIO and NATS once at its start, so the servers are started and waited for
// before the analyzer and the workers.
impl RunSpec {
    pub fn servers(&self) -> Vec<Service> {
        self.layout
            .services()
            .into_iter()
            .filter(|service| matches!(service, Service::Minio | Service::Nats))
            .collect()
    }

    pub fn roles(&self) -> Vec<Service> {
        let held_back = self.fault.and_then(Fault::held_back);
        self.layout
            .services()
            .into_iter()
            .filter(|service| matches!(service, Service::Analyzer | Service::Worker(_)))
            .filter(|service| Some(*service) != held_back)
            .collect()
    }
}

impl Fault {
    pub fn trigger(self) -> Trigger {
        match self {
            Self::RestartWorker { percent, .. }
            | Self::RestartNats { percent }
            | Self::RestartAnalyzer { percent } => Trigger::Ingested { percent },
            Self::DelayWorker { delay, .. } => Trigger::After(delay),
        }
    }

    pub fn steps(self) -> Vec<Step> {
        match self {
            Self::RestartWorker {
                worker, down_for, ..
            } => vec![
                Step::Kill(Service::Worker(worker)),
                Step::Pause(down_for),
                Step::Start(Service::Worker(worker)),
            ],
            Self::RestartNats { .. } => vec![Step::Kill(Service::Nats), Step::Start(Service::Nats)],
            Self::RestartAnalyzer { .. } => {
                vec![
                    Step::Kill(Service::Analyzer),
                    Step::Start(Service::Analyzer),
                ]
            }
            Self::DelayWorker { worker, .. } => vec![Step::Start(Service::Worker(worker))],
        }
    }

    pub fn held_back(self) -> Option<Service> {
        match self {
            Self::DelayWorker { worker, .. } => Some(Service::Worker(worker)),
            _ => None,
        }
    }
}

impl Trigger {
    pub fn is_due(self, metrics: Option<&AnalyzerMetrics>, since_start: Duration) -> bool {
        match self {
            Self::Ingested { percent } => metrics.is_some_and(|metrics| reached(metrics, percent)),
            Self::After(delay) => since_start >= delay,
        }
    }
}
