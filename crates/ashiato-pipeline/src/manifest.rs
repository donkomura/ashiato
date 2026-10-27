use serde::{Deserialize, Serialize};

use crate::{EventTime, PartitionId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub file_seq: u64,
    pub uri: String,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub file_time: EventTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub partition: PartitionId,
    pub files: Vec<ManifestEntry>,
}

// Partitions are kept in PartitionId order, and the index of a partition in this list is the
// partition number that file_id is derived from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub partitions: Vec<PartitionManifest>,
}
