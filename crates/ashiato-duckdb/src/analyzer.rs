use std::collections::BTreeSet;

use ashiato_pipeline::{
    AckTiming, Admission, Delivery, DeliveryHandle, EventTime, IngestedFile, Manifest, PartitionId,
    PartitionMessage, PartitionProgress, PartitionReader, PartitionWatermark, Position,
    TransportError, admit, batch_start, file_id, global_watermark, is_batch_complete,
};
use chrono::TimeDelta;
use duckdb::OptionalExt;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::{TimeUnit, Value};

use crate::{PipelineStore, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyzerSettings {
    pub batch: TimeDelta,
    pub allowed_lateness: TimeDelta,
    pub read_since: EventTime,
    pub fetch_max: usize,
    pub commit_every: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("failed to receive the messages of partition {partition}")]
    Transport {
        partition: PartitionId,
        #[source]
        source: TransportError,
    },
    #[error("failed to acknowledge the committed messages")]
    Ack(#[source] TransportError),
    #[error(
        "partition {partition} skipped from {expected:?} to {received:?}; \
         a message was lost or sent out of order"
    )]
    Gap {
        partition: PartitionId,
        expected: Position,
        received: Position,
    },
    #[error("partition {0} stopped sending before its end")]
    Closed(PartitionId),
}

struct PartitionState {
    id: PartitionId,
    file_count: u64,
    progress: PartitionProgress,
    watermark: Option<EventTime>,
    committed_watermark: Option<EventTime>,
}

impl PartitionState {
    fn committed_watermark(&self) -> PartitionWatermark {
        match (self.progress.end_committed, self.committed_watermark) {
            (true, _) => PartitionWatermark::Ended,
            (false, None) => PartitionWatermark::NotStarted,
            (false, Some(watermark)) => PartitionWatermark::At(watermark),
        }
    }
}

enum Received {
    Events {
        events: RecordBatch,
        watermark: EventTime,
    },
    File {
        partition_index: u32,
        file_seq: u64,
        file: IngestedFile,
        error: Option<String>,
    },
}

pub struct Analyzer {
    store: PipelineStore,
    settings: AnalyzerSettings,
    partitions: Vec<PartitionState>,
    received: Vec<Received>,
    changed: BTreeSet<usize>,
    acks: Vec<DeliveryHandle>,
    uncommitted_messages: usize,
}

impl Analyzer {
    pub fn open(
        store: PipelineStore,
        manifest: Manifest,
        settings: AnalyzerSettings,
    ) -> Result<Self, AnalyzerError> {
        let mut partitions = Vec::with_capacity(manifest.partitions.len());
        for (index, partition) in manifest.partitions.into_iter().enumerate() {
            let saved = store
                .connection
                .query_row(
                    "SELECT file_seq, record_offset, epoch_us(watermark), ended \
                     FROM partition_progress WHERE partition_index = ?",
                    [index_value(index)],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, bool>(3)?,
                        ))
                    },
                )
                .optional()?;
            let (progress, watermark) = match saved {
                Some((file_seq, record_offset, watermark, ended)) => (
                    PartitionProgress {
                        ended,
                        end_committed: ended,
                        ..PartitionProgress::starting_at(Position::new(file_seq, record_offset))
                    },
                    watermark.and_then(EventTime::from_timestamp_micros),
                ),
                None => (PartitionProgress::starting_at(Position::START), None),
            };
            partitions.push(PartitionState {
                file_count: partition.files.len() as u64,
                id: partition.partition,
                progress,
                watermark,
                committed_watermark: watermark,
            });
        }
        Ok(Self {
            store,
            settings,
            partitions,
            received: Vec::new(),
            changed: BTreeSet::new(),
            acks: Vec::new(),
            uncommitted_messages: 0,
        })
    }

    pub fn resume_positions(&self) -> Vec<Position> {
        self.partitions
            .iter()
            .map(|partition| partition.progress.next)
            .collect()
    }

    pub fn all_partitions_ended(&self) -> bool {
        self.partitions
            .iter()
            .all(|partition| partition.progress.ended)
    }

    // Reading first the partition that holds the global watermark back lets batches be
    // finalized as early as possible.
    fn next_partition(&self) -> Option<usize> {
        self.partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| !partition.progress.ended)
            .min_by_key(|(index, partition)| (partition.watermark, *index))
            .map(|(index, _)| index)
    }

    pub async fn receive<R: PartitionReader + ?Sized>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        let Some(index) = self.next_partition() else {
            return Ok(());
        };
        let partition = self.partitions[index].id.clone();
        let transport_error = |source| AnalyzerError::Transport {
            partition: partition.clone(),
            source,
        };
        let deliveries = reader
            .fetch(&partition, self.settings.fetch_max)
            .await
            .map_err(transport_error)?;
        if deliveries.is_empty() {
            return Err(AnalyzerError::Closed(partition));
        }
        let mut acks_now = Vec::new();
        for delivery in deliveries {
            if let Some(handle) = self.accept(index, delivery)? {
                acks_now.push(handle);
            }
        }
        if !acks_now.is_empty() {
            reader.ack(&acks_now).await.map_err(AnalyzerError::Ack)?;
        }
        Ok(())
    }

    fn accept(
        &mut self,
        index: usize,
        delivery: Delivery,
    ) -> Result<Option<DeliveryHandle>, AnalyzerError> {
        let Delivery { message, handle } = delivery;
        let partition = &mut self.partitions[index];
        self.uncommitted_messages += 1;
        match admit(&message, &partition.progress, partition.file_count) {
            Admission::Duplicate {
                ack: AckTiming::Now,
            } => return Ok(Some(handle)),
            Admission::Duplicate {
                ack: AckTiming::AfterNextCommit,
            } => {}
            Admission::Gap { expected, received } => {
                return Err(AnalyzerError::Gap {
                    partition: partition.id.clone(),
                    expected,
                    received,
                });
            }
            Admission::Accept {
                skip_records,
                next,
                ends_partition,
            } => {
                let partition_index = index_value(index);
                match message {
                    PartitionMessage::FileChunk {
                        position,
                        events,
                        file,
                        watermark,
                        ..
                    } => {
                        let skip = usize::try_from(skip_records)
                            .expect("a chunk holds fewer records than usize::MAX");
                        let events = events.slice(skip, events.num_rows() - skip);
                        if events.num_rows() > 0 {
                            self.received.push(Received::Events { events, watermark });
                        }
                        if next == position.next_file() {
                            self.received.push(Received::File {
                                partition_index,
                                file_seq: position.file_seq,
                                file,
                                error: None,
                            });
                        }
                        partition.watermark = partition.watermark.max(Some(watermark));
                    }
                    PartitionMessage::FileError {
                        position,
                        file,
                        reason,
                        watermark,
                        ..
                    } => {
                        self.received.push(Received::File {
                            partition_index,
                            file_seq: position.file_seq,
                            file,
                            error: Some(reason),
                        });
                        partition.watermark = partition.watermark.max(Some(watermark));
                    }
                    PartitionMessage::EndOfPartition { .. } => {}
                }
                partition.progress.next = next;
                partition.progress.ended |= ends_partition;
                self.changed.insert(index);
            }
        }
        self.acks.push(handle);
        Ok(None)
    }

    // The received events, the files and the positions are committed in one transaction, so
    // that a restart neither counts a message twice nor misses one.
    pub fn commit(&mut self) -> Result<Vec<DeliveryHandle>, AnalyzerError> {
        let transaction = self.store.connection.transaction()?;
        for received in self.received.drain(..) {
            match received {
                Received::Events { events, watermark } => {
                    {
                        let mut appender = transaction.appender("incoming")?;
                        appender.append_record_batch(events)?;
                    }
                    // Whether an event is late depends only on its own time and the watermark of
                    // its file, so the result does not change with the order of arrival (11 節).
                    transaction.execute(
                        "INSERT INTO late_events SELECT event_id, file_id, record_index \
                         FROM incoming WHERE event_time < ?",
                        [timestamp(watermark)],
                    )?;
                    transaction.execute(
                        "INSERT INTO pending SELECT * FROM incoming WHERE event_time >= ?",
                        [timestamp(watermark)],
                    )?;
                    transaction.execute("DELETE FROM incoming", [])?;
                }
                Received::File {
                    partition_index,
                    file_seq,
                    file,
                    error,
                } => {
                    transaction.execute(
                        "INSERT INTO ingested_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        duckdb::params![
                            partition_index,
                            file_seq,
                            file_id(partition_index, file_seq),
                            file.uri,
                            file.version_id,
                            file.etag,
                            file.size,
                            file.sha256,
                            timestamp(file.file_time),
                            file.record_count,
                            error,
                        ],
                    )?;
                }
            }
        }
        for index in &self.changed {
            let partition = &self.partitions[*index];
            transaction.execute(
                "INSERT OR REPLACE INTO partition_progress VALUES (?, ?, ?, ?, ?)",
                duckdb::params![
                    index_value(*index),
                    partition.progress.next.file_seq,
                    partition.progress.next.record_offset,
                    partition.watermark.map_or(Value::Null, timestamp),
                    partition.progress.ended,
                ],
            )?;
        }
        transaction.commit()?;
        for index in std::mem::take(&mut self.changed) {
            let partition = &mut self.partitions[index];
            partition.progress.committed = partition.progress.next;
            partition.progress.end_committed = partition.progress.ended;
            partition.committed_watermark = partition.watermark;
        }
        self.uncommitted_messages = 0;
        Ok(std::mem::take(&mut self.acks))
    }

    pub fn finalize_batches(&mut self) -> Result<Vec<(EventTime, EventTime)>, AnalyzerError> {
        let global = global_watermark(
            self.partitions
                .iter()
                .map(PartitionState::committed_watermark),
            self.settings.read_since,
            self.settings.allowed_lateness,
        );
        let mut finalized = Vec::new();
        loop {
            let earliest: Option<i64> = self.store.connection.query_row(
                "SELECT epoch_us(min(event_time)) FROM pending",
                [],
                |row| row.get(0),
            )?;
            let Some(earliest) = earliest.and_then(EventTime::from_timestamp_micros) else {
                break;
            };
            let start = batch_start(earliest, self.settings.batch);
            let end = start + self.settings.batch;
            if !is_batch_complete(end, global) {
                break;
            }
            let transaction = self.store.connection.transaction()?;
            transaction.execute(
                "DELETE FROM pending WHERE event_time >= ? AND event_time < ?",
                [timestamp(start), timestamp(end)],
            )?;
            transaction.execute(
                "INSERT INTO batches VALUES (?, ?)",
                [timestamp(start), timestamp(end)],
            )?;
            transaction.commit()?;
            finalized.push((start, end));
        }
        Ok(finalized)
    }

    pub async fn run<R: PartitionReader + ?Sized>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        while !self.all_partitions_ended() {
            self.receive(reader).await?;
            if self.uncommitted_messages >= self.settings.commit_every {
                self.commit_and_ack(reader).await?;
                self.finalize_batches()?;
            }
        }
        self.commit_and_ack(reader).await?;
        self.finalize_batches()?;
        Ok(())
    }

    async fn commit_and_ack<R: PartitionReader + ?Sized>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        let acks = self.commit()?;
        if !acks.is_empty() {
            reader.ack(&acks).await.map_err(AnalyzerError::Ack)?;
        }
        Ok(())
    }
}

fn index_value(index: usize) -> u32 {
    u32::try_from(index).expect("the manifest holds fewer than 2^32 partitions")
}

fn timestamp(time: EventTime) -> Value {
    Value::Timestamp(TimeUnit::Microsecond, time.timestamp_micros())
}
