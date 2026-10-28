use ashiato_pipeline::{ControlClient, PartitionId, PartitionManifest, WorkerPlan, resume_from};

use crate::{PartitionAssignment, WorkerError};

// Follows the steps of 6.4 節: the plan and the files of the assigned partitions, the positions
// that the analyzer has committed, and the last messages left in the stream.
pub async fn resume_assignments(
    control: &dyn ControlClient,
    worker_id: u32,
) -> Result<(WorkerPlan, Vec<PartitionAssignment>), WorkerError> {
    let plan = control
        .plan(worker_id)
        .await
        .map_err(WorkerError::Control)?;
    let partitions: Vec<PartitionId> = plan
        .partitions
        .iter()
        .map(|assigned| assigned.partition.clone())
        .collect();
    let committed = control
        .resume_positions(&partitions)
        .await
        .map_err(WorkerError::Control)?;
    let mut assignments = Vec::with_capacity(plan.partitions.len());
    for assigned in &plan.partitions {
        let partition = &assigned.partition;
        let files = control
            .manifest(partition, 0)
            .await
            .map_err(WorkerError::Control)?;
        let committed = *committed
            .get(partition)
            .ok_or_else(|| WorkerError::MissingResumePosition(partition.clone()))?;
        let last_published = control
            .last_published(partition)
            .await
            .map_err(WorkerError::Control)?;
        assignments.push(PartitionAssignment {
            index: assigned.index,
            manifest: PartitionManifest {
                partition: partition.clone(),
                files,
            },
            start: resume_from(committed, last_published),
        });
    }
    Ok((plan, assignments))
}
