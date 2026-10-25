use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalRow {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub event_ids: Vec<String>,
    pub access_key_id: Option<String>,
    pub principal_key: String,
    pub source_ip: String,
    pub account_id: String,
    pub region: String,
    pub attrs: BTreeMap<String, String>,
}
