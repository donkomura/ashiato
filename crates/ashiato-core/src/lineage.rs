use std::collections::{BTreeMap, HashMap, HashSet};

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
pub struct LineageGraph {
    edges: HashMap<String, LineageEdge>,
    max_depth: usize,
}

impl LineageGraph {
    pub fn new(edges: impl IntoIterator<Item = LineageEdge>) -> Self {
        let mut by_child = HashMap::new();
        for edge in edges {
            by_child
                .entry(edge.child_access_key_id.clone())
                .or_insert(edge);
        }
        Self {
            edges: by_child,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    pub fn with_max_depth(self, max_depth: usize) -> Self {
        Self { max_depth, ..self }
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    pub fn resolve(&self, access_key_id: Option<&str>, principal_key: &str) -> Resolution {
        let Some(mut edge) = access_key_id.and_then(|key| self.edges.get(key)) else {
            return Resolution {
                origin: origin_of_principal_key(principal_key),
                lineage: Vec::new(),
                warning: None,
            };
        };
        let mut visited = HashSet::from([edge.child_access_key_id.as_str()]);
        let mut path = vec![edge];
        let mut warning = None;
        while let Some(parent) = self.parent_edge(edge) {
            if !visited.insert(&parent.child_access_key_id) {
                warning = Some(LineageWarning::Cycle {
                    access_key_id: parent.child_access_key_id.clone(),
                });
                break;
            }
            if path.len() == self.max_depth {
                warning = Some(LineageWarning::DepthLimit {
                    access_key_id: edge.child_access_key_id.clone(),
                    max_depth: self.max_depth,
                });
                break;
            }
            path.push(parent);
            edge = parent;
        }
        Resolution {
            origin: origin_of_caller(edge),
            lineage: path.iter().rev().map(|edge| step_of(edge)).collect(),
            warning,
        }
    }

    pub fn resolve_signals(&self, mut signals: Vec<SignalRow>) -> ResolvedSignals {
        signals.sort_by_key(|signal| signal.start_time);
        let mut origins: BTreeMap<String, ResolvedOrigin> = BTreeMap::new();
        let mut warnings = Vec::new();
        for signal in signals {
            let resolution = self.resolve(signal.access_key_id.as_deref(), &signal.principal_key);
            if let Some(warning) = resolution.warning
                && !warnings.contains(&warning)
            {
                warnings.push(warning);
            }
            let resolved = origins
                .entry(resolution.origin.id.clone())
                .or_insert_with(|| ResolvedOrigin {
                    origin: resolution.origin,
                    lineage: Vec::new(),
                    signals: Vec::new(),
                });
            for step in resolution.lineage {
                if !resolved.lineage.contains(&step) {
                    resolved.lineage.push(step);
                }
            }
            resolved.signals.push(signal);
        }
        ResolvedSignals {
            origins: origins.into_values().collect(),
            warnings,
        }
    }

    fn parent_edge(&self, edge: &LineageEdge) -> Option<&LineageEdge> {
        let parent_key = edge.parent.access_key_id.as_deref()?;
        self.edges.get(parent_key)
    }
}

fn step_of(edge: &LineageEdge) -> LineageStep {
    LineageStep {
        access_key_id: edge.child_access_key_id.clone(),
        role_arn: edge.role_arn.clone(),
        issued_by_event_id: edge.issued_by_event_id.clone(),
    }
}

fn origin(kind: OriginKind, id: String) -> Origin {
    Origin { id, kind }
}

fn origin_of_caller(edge: &LineageEdge) -> Origin {
    let caller = &edge.parent;
    if caller.identity_type == "AWSService" {
        let service = caller.invoked_by.as_deref().unwrap_or_default();
        let role = edge.role_arn.as_deref().unwrap_or_default();
        return origin(OriginKind::Workload, format!("workload:{service}:{role}"));
    }
    if let Some(federation) = &edge.federation {
        return origin(
            OriginKind::Federated,
            format!("federated:{}:{}", federation.provider, federation.subject),
        );
    }
    if edge.cross_account {
        let principal = caller
            .principal_arn
            .as_deref()
            .unwrap_or(&caller.principal_key);
        return origin(
            OriginKind::ExternalPrincipal,
            format!("external:{principal}"),
        );
    }
    if let ("Root", Some(account_id)) = (caller.identity_type.as_str(), &caller.account_id) {
        return origin(OriginKind::Root, format!("root:{account_id}"));
    }
    origin_of_principal_key(&caller.principal_key)
}

// Sessions whose issuing event is not in the logs can only be tied to an origin through
// principal_key, so the origin is derived from its format (design document section 5).
// Assumed roles are grouped per role, which matches the granularity of principal_key.
fn origin_of_principal_key(principal_key: &str) -> Origin {
    if let Some(service) = principal_key.strip_prefix("service:") {
        return origin(OriginKind::Workload, format!("workload:{service}"));
    }
    if let Some(account) = principal_key.strip_prefix("account:") {
        return origin(
            OriginKind::ExternalPrincipal,
            format!("external:account:{account}"),
        );
    }
    let resource = principal_key.split(':').nth(5).unwrap_or_default();
    if principal_key.starts_with("arn:") {
        if resource == "root" {
            let account = principal_key.split(':').nth(4).unwrap_or_default();
            return origin(OriginKind::Root, format!("root:{account}"));
        }
        if resource.starts_with("user/") {
            return origin(OriginKind::IamUser, format!("iam_user:{principal_key}"));
        }
        if resource.starts_with("role/") {
            return origin(OriginKind::UnresolvedRole, format!("role:{principal_key}"));
        }
    }
    origin(OriginKind::Unknown, format!("unknown:{principal_key}"))
}
