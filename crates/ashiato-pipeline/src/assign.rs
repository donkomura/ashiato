use std::num::NonZeroU32;

use crate::PartitionId;

// Deals the partitions, in the order of their identifiers, to the workers one at a time, so that
// the numbers of partitions of the workers differ by at most one (5.1 節). Returns the indexes of
// the partitions of each worker.
pub fn assign_partitions(partitions: &[PartitionId], workers: NonZeroU32) -> Vec<Vec<usize>> {
    let workers = usize::try_from(workers.get()).expect("the number of workers fits in usize");
    let mut ordered: Vec<usize> = (0..partitions.len()).collect();
    ordered.sort_by_key(|index| &partitions[*index]);
    let mut assigned = vec![Vec::new(); workers];
    for (turn, index) in ordered.into_iter().enumerate() {
        assigned[turn % workers].push(index);
    }
    assigned
}
