use ashiato_pipeline::{ControlClient, WorkerPlan};

use crate::{PartitionAssignment, WorkerError};

// Follows the steps of 6.4 節: the plan and the files of the assigned partitions, the positions
// that the analyzer has committed, and the last messages left in the stream.
pub async fn resume_assignments(
    control: &dyn ControlClient,
    worker_id: u32,
) -> Result<(WorkerPlan, Vec<PartitionAssignment>), WorkerError> {
    let _ = (control, worker_id);
    todo!()
}
