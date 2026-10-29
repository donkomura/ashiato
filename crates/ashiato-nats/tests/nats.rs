mod common;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ashiato_duckdb::{Analyzer, AnalyzerError, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{FileSource, LocalSource, run_worker};
use ashiato_nats::{
    CapacityError, EventStream, NatsError, NatsReader, NatsWriter, consumer_name, decode,
    max_ack_pending, stream_name,
};
use ashiato_pipeline::{
    BrokerSettings, Delivery, DeliveryHandle, Manifest, PartitionId, PartitionMessage,
    PartitionReader, PartitionWriter, Position, SendTimes, TransportError, partition_channels,
};
use ashiato_testkit::nats::Nats;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::{DiscardPolicy, RetentionPolicy, StorageType};
use async_trait::async_trait;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, assignments, at, fixture, worker_settings};

const RUN_ID: &str = "case-1";
const MAX_MESSAGE_SIZE: usize = 1 << 20;
const TABLES: [&str; 6] = [
    "pending",
    "ingested_files",
    "partition_progress",
    "late_events",
    "recent_event_ids",
    "batches",
];

fn broker(ack_wait: &str) -> BrokerSettings {
    BrokerSettings {
        max_bytes: 1 << 30,
        max_messages_per_partition: 64,
        max_message_size: MAX_MESSAGE_SIZE as u64,
        duplicate_window: "10m".parse().unwrap(),
        ack_wait: ack_wait.parse().unwrap(),
    }
}

fn analyzer_settings(commit_every: usize) -> AnalyzerSettings {
    AnalyzerSettings {
        batch: TimeDelta::hours(1),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: at(1, 0, 0),
        fetch_max: 4,
        commit_every,
    }
}

fn partitions(manifest: &Manifest) -> Vec<PartitionId> {
    manifest
        .partitions
        .iter()
        .map(|partition| partition.partition.clone())
        .collect()
}

fn open_analyzer(database: &Path, manifest: &Manifest, settings: AnalyzerSettings) -> Analyzer {
    Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
}

fn tables(database: &Path) -> Vec<Vec<String>> {
    let connection = duckdb::Connection::open(database).unwrap();
    TABLES
        .iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!(
                    "SELECT CAST(to_json(t) AS VARCHAR) AS row FROM {table} t ORDER BY row"
                ))
                .unwrap();
            statement
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        })
        .collect()
}

async fn channel_tables(workspace: &Workspace, manifest: &Manifest) -> Vec<Vec<String>> {
    let database = workspace.database("channels");
    let mut analyzer = open_analyzer(&database, manifest, analyzer_settings(8));
    let (writer, mut reader) = partition_channels(partitions(manifest), 4);
    let worker = tokio::spawn(run_worker(
        assignments(manifest, &analyzer.resume_positions()),
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        worker_settings(MAX_MESSAGE_SIZE),
        Arc::default(),
    ));
    analyzer.run(&mut reader).await.unwrap();
    worker.await.unwrap().unwrap();
    drop(analyzer);
    tables(&database)
}

async fn connect(nats: &Nats) -> async_nats::Client {
    async_nats::connect(nats.url()).await.unwrap()
}

async fn create_stream(nats: &Nats, manifest: &Manifest, broker: &BrokerSettings) -> EventStream {
    EventStream::create(
        connect(nats).await,
        RUN_ID,
        manifest.partitions.len(),
        broker,
    )
    .await
    .unwrap()
}

async fn reader(
    stream: &EventStream,
    manifest: &Manifest,
    broker: &BrokerSettings,
    settings: AnalyzerSettings,
) -> NatsReader {
    stream
        .reader(
            partitions(manifest),
            broker,
            max_ack_pending(settings.commit_every, settings.fetch_max),
        )
        .await
        .unwrap()
}

async fn publish_all(nats: &Nats, manifest: &Manifest) {
    let writer = NatsWriter::new(connect(nats).await, RUN_ID, MAX_MESSAGE_SIZE).unwrap();
    run_worker(
        assignments(manifest, &vec![Position::START; manifest.partitions.len()]),
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        worker_settings(MAX_MESSAGE_SIZE),
        Arc::default(),
    )
    .await
    .unwrap();
}

async fn stream_messages(stream: &EventStream) -> u64 {
    stream
        .jetstream()
        .get_stream(stream_name(RUN_ID))
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .state
        .messages
}

// Records what the analyzer receives, and can slow it down, stop it after some fetches, restart
// the server before a fetch, or count the consumers that it creates anew.
struct Probe {
    inner: NatsReader,
    delay: Duration,
    fail_after: Option<usize>,
    restart_before: Option<(usize, Arc<Nats>)>,
    fetches: usize,
    received: HashMap<PartitionId, Vec<Position>>,
    resets: Vec<PartitionId>,
}

impl Probe {
    fn new(inner: NatsReader) -> Self {
        Self {
            inner,
            delay: Duration::ZERO,
            fail_after: None,
            restart_before: None,
            fetches: 0,
            received: HashMap::new(),
            resets: Vec::new(),
        }
    }

    fn record(&mut self, partition: &PartitionId, deliveries: &[Delivery]) {
        self.received.entry(partition.clone()).or_default().extend(
            deliveries
                .iter()
                .map(|delivery| delivery.message.position()),
        );
    }

    fn redelivered(&self) -> usize {
        self.received
            .values()
            .map(|positions| positions.len() - positions.iter().collect::<HashSet<_>>().len())
            .sum()
    }
}

#[async_trait]
impl PartitionReader for Probe {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        let deliveries = self.inner.fetch(partition, max).await?;
        self.record(partition, &deliveries);
        Ok(deliveries)
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        self.fetches += 1;
        if self.fail_after.is_some_and(|limit| self.fetches > limit) {
            return Err(TransportError::Closed(partitions[0].clone()));
        }
        if let Some((fetch, nats)) = &self.restart_before
            && *fetch == self.fetches
        {
            let nats = Arc::clone(nats);
            tokio::task::spawn_blocking(move || nats.restart())
                .await
                .unwrap();
        }
        tokio::time::sleep(self.delay).await;
        let (index, deliveries) = self.inner.fetch_ready(partitions, max).await?;
        self.record(&partitions[index], &deliveries);
        Ok((index, deliveries))
    }

    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        self.inner.ack(deliveries).await
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        self.resets.push(partition.clone());
        self.inner.reset(partition).await
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn the_stream_and_the_consumers_are_created_with_the_settings_of_the_design() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let broker = broker("15m");
    let stream = create_stream(&nats, &manifest, &broker).await;
    let _reader = reader(&stream, &manifest, &broker, analyzer_settings(8)).await;

    let mut nats_stream = stream
        .jetstream()
        .get_stream(stream_name(RUN_ID))
        .await
        .unwrap();
    let config = nats_stream.info().await.unwrap().config.clone();
    assert_eq!(config.subjects, vec!["ashiato.case-1.events.>".to_owned()]);
    assert_eq!(config.retention, RetentionPolicy::WorkQueue);
    assert_eq!(config.storage, StorageType::File);
    assert_eq!(config.max_bytes, 1 << 30);
    assert_eq!(config.max_messages_per_subject, 64);
    assert_eq!(config.discard, DiscardPolicy::New);
    assert!(config.discard_new_per_subject);
    assert_eq!(config.max_message_size, 1 << 20);
    assert_eq!(config.duplicate_window, Duration::from_secs(600));

    for partition in partitions(&manifest) {
        let info = nats_stream
            .consumer_info(consumer_name(&partition))
            .await
            .unwrap();
        let config = &info.config;
        assert_eq!(
            config.durable_name.as_deref(),
            Some(consumer_name(&partition).as_str())
        );
        assert_eq!(
            config.filter_subject,
            format!(
                "ashiato.case-1.events.{}.{}",
                partition.account, partition.region
            )
        );
        assert_eq!(config.ack_policy, AckPolicy::Explicit);
        assert_eq!(config.deliver_policy, DeliverPolicy::All);
        assert_eq!(config.ack_wait, Duration::from_secs(900));
        assert_eq!(config.max_ack_pending, 12);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn max_bytes_less_than_the_partitions_may_hold_is_an_error_at_the_start() {
    let nats = Nats::start();
    let broker = BrokerSettings {
        max_bytes: 2 * 64 * MAX_MESSAGE_SIZE as u64 - 1,
        ..broker("15m")
    };

    let error = EventStream::create(connect(&nats).await, RUN_ID, 2, &broker)
        .await
        .err()
        .unwrap();

    assert!(
        matches!(
            error,
            NatsError::Capacity(CapacityError { partitions: 2, .. })
        ),
        "{error:?}"
    );
    let context = async_nats::jetstream::new(connect(&nats).await);
    assert!(context.get_stream(stream_name(RUN_ID)).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn a_full_partition_makes_the_writer_wait_until_a_message_is_acknowledged() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let broker = BrokerSettings {
        max_messages_per_partition: 2,
        ..broker("15m")
    };
    let stream = create_stream(&nats, &manifest, &broker).await;
    let mut reader = reader(&stream, &manifest, &broker, analyzer_settings(8)).await;
    let partition = manifest.partitions[0].partition.clone();
    let messages: Vec<PartitionMessage> = common::worker_messages(&manifest, MAX_MESSAGE_SIZE)
        .await
        .into_iter()
        .filter(|message| *message.partition() == partition)
        .take(3)
        .collect();
    let writer = Arc::new(NatsWriter::new(connect(&nats).await, RUN_ID, MAX_MESSAGE_SIZE).unwrap());
    writer.put(messages[0].clone()).await.unwrap();
    writer.put(messages[1].clone()).await.unwrap();

    let times = Arc::new(SendTimes::default());
    let third = tokio::spawn({
        let (writer, times) = (Arc::clone(&writer), Arc::clone(&times));
        let message = messages[2].clone();
        async move { writer.put_timed(message, &times).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!third.is_finished());

    let delivered = reader.fetch(&partition, 1).await.unwrap();
    assert_eq!(delivered[0].message, messages[0]);
    reader.ack(&[delivered[0].handle]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), third)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        times.rejected_wait.get() >= Duration::from_millis(900),
        "{times:?}"
    );
    assert!(times.publish_wait.get() > Duration::ZERO, "{times:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn the_analyzer_gets_the_same_tables_through_nats_as_through_channels() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = channel_tables(&workspace, &manifest).await;
    let broker = broker("15m");
    let settings = analyzer_settings(8);
    let stream = create_stream(&nats, &manifest, &broker).await;
    let mut reader = reader(&stream, &manifest, &broker, settings).await;
    let database = workspace.database("nats");
    let mut analyzer = open_analyzer(&database, &manifest, settings);

    let publishing = tokio::spawn({
        let manifest = manifest.clone();
        let url = nats.url();
        async move {
            let writer = NatsWriter::new(
                async_nats::connect(url).await.unwrap(),
                RUN_ID,
                MAX_MESSAGE_SIZE,
            )
            .unwrap();
            run_worker(
                assignments(&manifest, &vec![Position::START; manifest.partitions.len()]),
                Arc::new(LocalSource) as Arc<dyn FileSource>,
                Arc::new(writer),
                worker_settings(MAX_MESSAGE_SIZE),
                Arc::default(),
            )
            .await
        }
    });
    analyzer.run(&mut reader).await.unwrap();
    publishing.await.unwrap().unwrap();
    drop(analyzer);

    assert_eq!(tables(&database), expected);
    // Every message was acknowledged, so the work queue holds none of them.
    let mut remaining = stream_messages(&stream).await;
    for _ in 0..50 {
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        remaining = stream_messages(&stream).await;
    }
    assert_eq!(remaining, 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn redeliveries_after_ack_wait_do_not_change_the_tables() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = channel_tables(&workspace, &manifest).await;
    let broker = broker("1s");
    // Nothing is committed before the end, so the messages wait longer than ack_wait for their
    // acknowledgements.
    let settings = analyzer_settings(1000);
    let stream = create_stream(&nats, &manifest, &broker).await;
    publish_all(&nats, &manifest).await;
    let mut probe = Probe::new(reader(&stream, &manifest, &broker, settings).await);
    probe.delay = Duration::from_millis(200);
    let database = workspace.database("nats");
    let mut analyzer = open_analyzer(&database, &manifest, settings);

    analyzer.run(&mut probe).await.unwrap();
    drop(analyzer);

    assert!(probe.redelivered() > 0);
    assert_eq!(tables(&database), expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn an_analyzer_restarted_before_a_commit_receives_the_messages_again() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = channel_tables(&workspace, &manifest).await;
    let broker = broker("15m");
    let settings = analyzer_settings(5);
    let stream = create_stream(&nats, &manifest, &broker).await;
    publish_all(&nats, &manifest).await;
    let database = workspace.database("nats");

    let mut probe = Probe::new(reader(&stream, &manifest, &broker, settings).await);
    probe.fail_after = Some(4);
    let mut analyzer = open_analyzer(&database, &manifest, settings);
    analyzer.run(&mut probe).await.unwrap_err();
    drop(analyzer);
    drop(probe);

    let mut reader = reader(&stream, &manifest, &broker, settings).await;
    let mut analyzer = open_analyzer(&database, &manifest, settings);
    assert_ne!(
        analyzer.resume_positions(),
        vec![Position::START; manifest.partitions.len()]
    );
    analyzer.run(&mut reader).await.unwrap();
    drop(analyzer);

    assert_eq!(tables(&database), expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn a_message_removed_from_the_stream_is_an_error_after_one_new_consumer() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let broker = broker("15m");
    let settings = analyzer_settings(8);
    let stream = create_stream(&nats, &manifest, &broker).await;
    publish_all(&nats, &manifest).await;
    let partition = manifest.partitions[0].partition.clone();
    let nats_stream = stream
        .jetstream()
        .get_stream(stream_name(RUN_ID))
        .await
        .unwrap();
    let mut removed = None;
    for sequence in 1.. {
        let message = nats_stream.get_raw_message(sequence).await.unwrap();
        let decoded = decode(RUN_ID, &message.subject, &message.headers, &message.payload).unwrap();
        if *decoded.partition() == partition && decoded.position() == Position::new(2, 0) {
            removed = Some(sequence);
            break;
        }
    }
    assert!(nats_stream.delete_message(removed.unwrap()).await.unwrap());
    let mut probe = Probe::new(reader(&stream, &manifest, &broker, settings).await);
    let mut analyzer = open_analyzer(&workspace.database("nats"), &manifest, settings);

    let error = analyzer.run(&mut probe).await.unwrap_err();

    assert!(
        matches!(
            &error,
            AnalyzerError::Gap { partition: gap, expected, received }
                if *gap == partition
                    && *expected == Position::new(2, 0)
                    && *received == Position::new(3, 0)
        ),
        "{error:?}"
    );
    assert_eq!(probe.resets, vec![partition]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn the_position_after_the_last_message_in_the_stream_is_read_for_each_partition() {
    let nats = Nats::start();
    let workspace = fixture();
    let manifest = workspace.manifest();
    let broker = broker("15m");
    let stream = create_stream(&nats, &manifest, &broker).await;
    let worker_stream = EventStream::open(connect(&nats).await, RUN_ID)
        .await
        .unwrap();
    for partition in partitions(&manifest) {
        assert_eq!(
            worker_stream.last_published(&partition).await.unwrap(),
            None
        );
    }

    publish_all(&nats, &manifest).await;

    for partition in &manifest.partitions {
        assert_eq!(
            worker_stream
                .last_published(&partition.partition)
                .await
                .unwrap(),
            Some(Position::new(partition.files.len() as u64, 0))
        );
    }
    stream.delete().await.unwrap();
    assert!(
        EventStream::open(connect(&nats).await, RUN_ID)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a NATS server in Docker"]
async fn a_restart_of_the_server_does_not_change_the_tables() {
    let nats = Arc::new(Nats::start());
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = channel_tables(&workspace, &manifest).await;
    let broker = broker("15m");
    let settings = AnalyzerSettings {
        fetch_max: 1,
        ..analyzer_settings(4)
    };
    let stream = create_stream(&nats, &manifest, &broker).await;
    let mut probe = Probe::new(reader(&stream, &manifest, &broker, settings).await);
    probe.delay = Duration::from_millis(50);
    probe.restart_before = Some((10, Arc::clone(&nats)));
    let database = workspace.database("nats");
    let mut analyzer = open_analyzer(&database, &manifest, settings);

    let publishing = tokio::spawn({
        let manifest = manifest.clone();
        let url = nats.url();
        async move {
            let writer = NatsWriter::new(
                async_nats::connect(url).await.unwrap(),
                RUN_ID,
                MAX_MESSAGE_SIZE,
            )
            .unwrap();
            run_worker(
                assignments(&manifest, &vec![Position::START; manifest.partitions.len()]),
                Arc::new(LocalSource) as Arc<dyn FileSource>,
                Arc::new(writer),
                worker_settings(MAX_MESSAGE_SIZE),
                Arc::default(),
            )
            .await
        }
    });
    analyzer.run(&mut probe).await.unwrap();
    publishing.await.unwrap().unwrap();
    drop(analyzer);

    assert!(probe.fetches > 10);
    assert_eq!(tables(&database), expected);
}
