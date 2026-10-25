use serde::Serialize;

use crate::SignalRow;

pub const DEFAULT_MAX_DEPTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub access_key_id: Option<String>,
    pub principal_key: String,
    pub principal_arn: Option<String>,
    pub identity_type: String,
    pub account_id: Option<String>,
    pub invoked_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Federation {
    pub provider: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageEdge {
    pub child_access_key_id: String,
    pub parent: Caller,
    pub role_arn: Option<String>,
    pub issued_by_event_id: String,
    pub cross_account: bool,
    pub federation: Option<Federation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Origin {
    pub id: String,
    pub kind: OriginKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginKind {
    IamUser,
    Root,
    ExternalPrincipal,
    Federated,
    Workload,
    UnresolvedRole,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageStep {
    pub access_key_id: String,
    pub role_arn: Option<String>,
    pub issued_by_event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageWarning {
    Cycle {
        access_key_id: String,
    },
    DepthLimit {
        access_key_id: String,
        max_depth: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub origin: Origin,
    pub lineage: Vec<LineageStep>,
    pub warning: Option<LineageWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOrigin {
    pub origin: Origin,
    pub lineage: Vec<LineageStep>,
    pub signals: Vec<SignalRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSignals {
    pub origins: Vec<ResolvedOrigin>,
    pub warnings: Vec<LineageWarning>,
}

#[derive(Debug, Clone)]
pub struct LineageGraph {}

impl LineageGraph {
    pub fn new(edges: impl IntoIterator<Item = LineageEdge>) -> Self {
        let _ = edges;
        todo!()
    }

    pub fn with_max_depth(self, max_depth: usize) -> Self {
        let _ = max_depth;
        todo!()
    }

    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        todo!()
    }

    pub fn resolve(&self, access_key_id: Option<&str>, principal_key: &str) -> Resolution {
        let _ = (access_key_id, principal_key);
        todo!()
    }

    pub fn resolve_signals(&self, signals: Vec<SignalRow>) -> ResolvedSignals {
        let _ = signals;
        todo!()
    }
}
