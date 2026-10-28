use std::num::NonZeroU32;

use crate::PartitionId;

pub fn assign_partitions(partitions: &[PartitionId], workers: NonZeroU32) -> Vec<Vec<usize>> {
    let _ = (partitions, workers);
    todo!()
}
