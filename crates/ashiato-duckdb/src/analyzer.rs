use std::collections::BTreeSet;

use ashiato_core::AnalysisWindow;
use ashiato_core::rule::SignalRule;
use ashiato_pipeline::{
    AckTiming, Admission, Delivery, DeliveryHandle, EventTime, IngestedFile, Manifest, PartitionId,
    PartitionMessage, PartitionProgress, PartitionReader, PartitionWatermark, Position,
    TransportError, admit, batch_start, file_id, global_watermark, is_batch_complete,
    readable_partitions, recent_event_expiry,
};
use chrono::TimeDelta;
use duckdb::OptionalExt;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::{TimeUnit, Value};

use crate::batch_queries::{BatchQueries, BatchQueryError};
use crate::{PipelineStore, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyzerSettings {
    pub batch: TimeDelta,
    pub allowed_lateness: TimeDelta,
    pub read_since: EventTime,
    pub fetch_max: usize,
    pub commit_every: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnalyzerMetrics {
    pub pending_max_rows: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("failed to receive the messages")]
    Receive(#[source] TransportError),
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
    #[error(transparent)]
    Batch(#[from] BatchQueryError),
}

struct PartitionState {
    id: PartitionId,
    file_count: u64,
    progress: PartitionProgress,
    watermark: Option<EventTime>,
    committed_watermark: Option<EventTime>,
    renewed_since_accepted: bool,
}

impl PartitionState {
    fn accepted_watermark(&self) -> PartitionWatermark {
        match (self.progress.ended, self.watermark) {
            (true, _) => PartitionWatermark::Ended,
            (false, None) => PartitionWatermark::NotStarted,
            (false, Some(watermark)) => PartitionWatermark::At(watermark),
        }
    }

    fn committed_watermark(&self) -> PartitionWatermark {
        match (self.progress.end_committed, self.committed_watermark) {
            (true, _) => PartitionWatermark::Ended,
            (false, None) => PartitionWatermark::NotStarted,
            (false, Some(watermark)) => PartitionWatermark::At(watermark),
        }
    }
}

enum Accepted {
    AckNow(DeliveryHandle),
    AckAfterCommit,
    Gap {
        expected: Position,
        received: Position,
    },
}

enum Received {
    Events {
        partition_index: u32,
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
    queries: Option<BatchQueries>,
    max_skew: Option<TimeDelta>,
    pending_rows: u64,
    metrics: AnalyzerMetrics,
}

impl Analyzer {
    pub fn open(
        store: PipelineStore,
        manifest: Manifest,
        settings: AnalyzerSettings,
    ) -> Result<Self, AnalyzerError> {
        let pending_rows =
            store
                .connection
                .query_row("SELECT count(*) FROM pending", [], |row| row.get(0))?;
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
                renewed_since_accepted: false,
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
            queries: None,
            max_skew: None,
            pending_rows,
            metrics: AnalyzerMetrics {
                pending_max_rows: pending_rows,
            },
        })
    }

    pub fn with_queries(self, rules: Vec<SignalRule>, window: AnalysisWindow) -> Self {
        Self {
            queries: Some(BatchQueries::new(rules, window)),
            ..self
        }
    }

    pub fn with_max_skew(self, max_skew: Option<TimeDelta>) -> Self {
        Self { max_skew, ..self }
    }

    pub fn pending_rows(&self) -> Result<u64, AnalyzerError> {
        Ok(self
            .store
            .connection
            .query_row("SELECT count(*) FROM pending", [], |row| row.get(0))?)
    }

    pub fn into_store(self) -> PipelineStore {
        self.store
    }

    pub fn metrics(&self) -> AnalyzerMetrics {
        self.metrics
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

    // The global watermark for max_skew is taken from the accepted watermarks rather than the
    // committed ones. The partition that holds it back is then always readable, so the analyzer
    // never waits on partitions none of which it may read, even with messages accepted but not
    // committed yet. Which partition is read does not change the output (11 節), so deciding it
    // from state that is not committed yet is safe. Lower watermarks come first, because reading
    // the partition that holds the global watermark back lets batches be finalized earliest.
    fn readable_partitions(&self) -> Vec<usize> {
        let watermarks: Vec<PartitionWatermark> = self
            .partitions
            .iter()
            .map(PartitionState::accepted_watermark)
            .collect();
        let readable = readable_partitions(
            &watermarks,
            self.settings.read_since,
            self.settings.allowed_lateness,
            self.max_skew,
        );
        let mut indexes: Vec<usize> = (0..self.partitions.len())
            .filter(|index| readable[*index])
            .collect();
        indexes.sort_by_key(|index| (self.partitions[*index].watermark, *index));
        indexes
    }

    // Takes messages from whichever readable partition has them, so that a lagging partition
    // does not stop the others short of max_skew (9 節).
    pub async fn receive<R: PartitionReader + ?Sized>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), AnalyzerError> {
        let candidates = self.readable_partitions();
        if candidates.is_empty() {
            return Ok(());
        }
        let partitions: Vec<PartitionId> = candidates
            .iter()
            .map(|index| self.partitions[*index].id.clone())
            .collect();
        let (chosen, deliveries) = reader
            .fetch_ready(&partitions, self.settings.fetch_max)
            .await
            .map_err(AnalyzerError::Receive)?;
        if deliveries.is_empty() {
            return Err(AnalyzerError::Closed(partitions[chosen].clone()));
        }
        let index = candidates[chosen];
        let mut acks_now = Vec::new();
        let mut gap = None;
        for delivery in deliveries {
            match self.accept(index, delivery)? {
                Accepted::AckNow(handle) => acks_now.push(handle),
                Accepted::AckAfterCommit => {}
                // The deliveries after a gap are left unacknowledged, so that the new consumer
                // delivers them again in the order of the stream.
                Accepted::Gap { expected, received } => {
                    gap = Some((expected, received));
                    break;
                }
            }
        }
        if !acks_now.is_empty() {
            reader.ack(&acks_now).await.map_err(AnalyzerError::Ack)?;
        }
        match gap {
            Some((expected, received)) => self.renew(reader, index, expected, received).await,
            None => Ok(()),
        }
    }

    // A new consumer delivers the unacknowledged messages in the order of the stream, which
    // removes a gap that a redelivery after ack_wait made. A gap that remains right after it is
    // a lost message, which another new consumer would not bring back (10 節). The messages
    // accepted but not committed yet are delivered again as duplicates, and every
    // acknowledgement is still sent only after the commit that holds the message.
    async fn renew<R: PartitionReader + ?Sized>(
        &mut self,
        reader: &mut R,
        index: usize,
        expected: Position,
        received: Position,
    ) -> Result<(), AnalyzerError> {
        let partition = &mut self.partitions[index];
        let gap = AnalyzerError::Gap {
            partition: partition.id.clone(),
            expected,
            received,
        };
        if partition.renewed_since_accepted {
            return Err(gap);
        }
        match reader.reset(&partition.id).await {
            Ok(()) => {
                partition.renewed_since_accepted = true;
                Ok(())
            }
            Err(TransportError::RedeliveryUnsupported(_)) => Err(gap),
            Err(error) => Err(AnalyzerError::Receive(error)),
        }
    }

    fn accept(&mut self, index: usize, delivery: Delivery) -> Result<Accepted, AnalyzerError> {
        let Delivery { message, handle } = delivery;
        let partition = &mut self.partitions[index];
        self.uncommitted_messages += 1;
        match admit(&message, &partition.progress, partition.file_count) {
            Admission::Duplicate {
                ack: AckTiming::Now,
            } => return Ok(Accepted::AckNow(handle)),
            Admission::Duplicate {
                ack: AckTiming::AfterNextCommit,
            } => {}
            Admission::Gap { expected, received } => {
                return Ok(Accepted::Gap { expected, received });
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
                            self.received.push(Received::Events {
                                partition_index,
                                events,
                                watermark,
                            });
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
                partition.renewed_since_accepted = false;
                self.changed.insert(index);
            }
        }
        self.acks.push(handle);
        Ok(Accepted::AckAfterCommit)
    }

    // The received events, the files and the positions are committed in one transaction, so
    // that a restart neither counts a message twice nor misses one.
    pub fn commit(&mut self) -> Result<Vec<DeliveryHandle>, AnalyzerError> {
        let transaction = self.store.connection.transaction()?;
        let mut added_rows = 0;
        for received in self.received.drain(..) {
            match received {
                Received::Events {
                    partition_index,
                    events,
                    watermark,
                } => {
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
                    // The same event can be in both the organization trail and the account trail
                    // of a partition. recent_event_ids is written in the same transaction as the
                    // positions, so an event received again after a restart is not mistaken for
                    // a duplicate of itself (8.2 節).
                    transaction.execute(
                        "CREATE OR REPLACE TEMP TABLE accepted AS \
                         SELECT * FROM incoming i \
                         WHERE event_time >= ? \
                           AND NOT EXISTS (SELECT 1 FROM recent_event_ids r \
                                           WHERE r.partition_index = ? \
                                             AND r.event_id = i.event_id) \
                         QUALIFY row_number() OVER (PARTITION BY event_id \
                                                    ORDER BY record_index) = 1",
                        duckdb::params![timestamp(watermark), partition_index],
                    )?;
                    added_rows +=
                        transaction.execute("INSERT INTO pending SELECT * FROM accepted", [])?;
                    transaction.execute(
                        "INSERT INTO recent_event_ids \
                         SELECT ?, event_id, event_time FROM accepted",
                        [partition_index],
                    )?;
                    transaction.execute_batch("DROP TABLE accepted; DELETE FROM incoming;")?;
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
            if let Some(watermark) = partition.watermark {
                transaction.execute(
                    "DELETE FROM recent_event_ids WHERE partition_index = ? AND event_time < ?",
                    duckdb::params![
                        index_value(*index),
                        timestamp(recent_event_expiry(
                            watermark,
                            self.settings.allowed_lateness
                        )),
                    ],
                )?;
            }
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
        self.pending_rows += added_rows as u64;
        self.metrics.pending_max_rows = self.metrics.pending_max_rows.max(self.pending_rows);
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
            if let Some(queries) = &self.queries {
                queries.run(&transaction, start, end)?;
            }
            let removed_rows = transaction.execute(
                "DELETE FROM pending WHERE event_time >= ? AND event_time < ?",
                [timestamp(start), timestamp(end)],
            )?;
            transaction.execute(
                "INSERT INTO batches VALUES (?, ?)",
                [timestamp(start), timestamp(end)],
            )?;
            transaction.commit()?;
            self.pending_rows -= removed_rows as u64;
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
