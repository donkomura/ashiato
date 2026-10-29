#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use ashiato_nats::{EventStream, stream_name};
use ashiato_pipeline::{PartitionId, Position};
use ashiato_testkit::fixture::{Expectation, builtin_fixtures};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::minio::{ACCESS_KEY, Minio, REGION, SECRET_KEY};
use ashiato_testkit::nats::Nats;
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use serde_json::{Value, json};
use tempfile::TempDir;

pub const RUN_ID: &str = "distributed-1";
pub const BUCKET: &str = "trail-bucket";
pub const ORG_ID: &str = "o-exampleorgid";
pub const BIG_ACCOUNT: &str = "999999999999";
pub const BIG_REGION: &str = "us-east-1";

const SCENARIO_RULE_IDS: [&str; 4] = [
    "console_takeover_mining",
    "role_chain_escalation",
    "leaked_key_recon_persistence",
    "cross_account_s3_destruction",
];

// The tables of 14.1.3 節 that must match the single process run row for row.
pub const TABLES: [&str; 8] = [
    "signals",
    "lineage_edges",
    "baseline",
    "ingested_files",
    "late_events",
    "batches",
    "data_counters",
    "partition_progress",
];

const FILLER_ACCOUNTS: [&str; 3] = ["444444444444", "555555555555", "666666666666"];
const FILLER_HOURS: i64 = 6;
const FILLER_EVENTS: usize = 2;
// The big partition has ten times the events of the others, so that max_skew holds the
// others back while it is read (14.1.2 節).
const BIG_EVENTS: usize = 10 * FILLER_EVENTS;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(600);
const POLL: Duration = Duration::from_millis(20);

fn event_time(record: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(record["eventTime"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

fn filler(id: String, account: &str, time: DateTime<Utc>) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .set("eventSource", "ec2.amazonaws.com")
        .set("eventName", "DescribeInstances")
        .set("recipientAccountId", account)
        .set("awsRegion", BIG_REGION)
        .set("userIdentity.accountId", account)
        .build()
}

fn analysis_start() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap()
}

// The positive fixtures of the four scenarios spread over several accounts, so that their
// lineage crosses partitions. Every file of the account trail is also delivered by the
// organization trail, one file arrives too late for its events, and one file is broken.
pub fn write_logs(root: &Path) {
    let five_minutes = TimeDelta::minutes(5).num_seconds();
    let mut files: BTreeMap<(String, String, i64), Vec<Value>> = BTreeMap::new();
    let scenarios = builtin_fixtures()
        .unwrap()
        .into_iter()
        .filter(|fixture| {
            fixture.expectation == Expectation::Positive
                && SCENARIO_RULE_IDS.contains(&fixture.rule_id.as_str())
        })
        .enumerate()
        .flat_map(|(fixture, file)| {
            file.records
                .into_iter()
                .enumerate()
                .map(move |(index, mut record)| {
                    record["eventID"] = json!(format!("scenario-{fixture:04}-{index:04}"));
                    record
                })
        });
    let start = analysis_start();
    let steps = FILLER_HOURS * 12;
    let fillers = (0..steps).flat_map(|step| {
        let time = start + TimeDelta::minutes(5 * step + 1);
        let small = FILLER_ACCOUNTS.iter().flat_map(move |account| {
            (0..FILLER_EVENTS)
                .map(move |index| filler(format!("{account}-{step}-{index}"), account, time))
        });
        let big = (0..BIG_EVENTS)
            .map(move |index| filler(format!("big-{step}-{index}"), BIG_ACCOUNT, time));
        small.chain(big)
    });
    for record in scenarios.chain(fillers) {
        let file_time = (event_time(&record).timestamp() / five_minutes + 2) * five_minutes;
        files
            .entry((
                record["recipientAccountId"].as_str().unwrap().to_owned(),
                record["awsRegion"].as_str().unwrap().to_owned(),
                file_time,
            ))
            .or_default()
            .push(record);
    }
    for ((account, region, file_time), records) in files {
        let account_trail = LogFile {
            records,
            ..LogFile::new(
                account,
                region,
                DateTime::from_timestamp(file_time, 0).unwrap(),
            )
        };
        write_log_file(root, &account_trail);
        write_log_file(
            root,
            &LogFile {
                org_id: Some(ORG_ID.to_owned()),
                unique: "OrganizationTrail".to_owned(),
                ..account_trail
            },
        );
    }
    let late_file = start + TimeDelta::hours(6);
    write_log_file(
        root,
        &LogFile {
            records: vec![filler(
                "late-1".to_owned(),
                FILLER_ACCOUNTS[0],
                late_file - TimeDelta::hours(2),
            )],
            unique: "Late".to_owned(),
            ..LogFile::new(FILLER_ACCOUNTS[0], BIG_REGION, late_file)
        },
    );
    let broken = root.join(
        LogFile {
            unique: "Broken".to_owned(),
            ..LogFile::new(FILLER_ACCOUNTS[1], BIG_REGION, start + TimeDelta::hours(3))
        }
        .relative_path(),
    );
    fs::create_dir_all(broken.parent().unwrap()).unwrap();
    fs::write(broken, b"not a gzip stream").unwrap();
}

pub fn file_count(root: &Path) -> u64 {
    fn walk(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() { walk(&path) } else { 1 }
            })
            .sum()
    }
    walk(root)
}

// The MinIO and NATS servers of one test, and the plans of its runs. All runs read the same
// bucket, so that the URIs, version ids and ETags in ingested_files are the same.
pub struct Cluster {
    pub minio: Minio,
    pub nats: Nats,
    pub dir: TempDir,
    pub files: u64,
}

impl Cluster {
    pub fn start() -> Self {
        let minio = Minio::start();
        let nats = Nats::start();
        let dir = TempDir::new().unwrap();
        let logs = dir.path().join("logs");
        write_logs(&logs);
        minio.create_bucket(BUCKET, true);
        minio.upload_dir(&logs, BUCKET);
        let files = file_count(&logs);
        Self {
            minio,
            nats,
            dir,
            files,
        }
    }

    // The plans differ only in placement and broker, which the plan digest leaves out, so the
    // outputs of all runs can be compared byte for byte (4.1 節).
    pub fn write_plan(&self, name: &str, placement: &str, broker: &str) -> PathBuf {
        let plan = self.dir.path().join(name).join("run.yaml");
        fs::create_dir_all(plan.parent().unwrap()).unwrap();
        fs::write(
            &plan,
            format!(
                "run_id: {RUN_ID}\n\
                 source:\n  kind: s3\n  uri: s3://{BUCKET}/AWSLogs/\n  endpoint_url: {}\n\
                 analysis_window:\n  since: 2026-10-01T00:00:00Z\n  until: 2026-10-02T00:00:00Z\n\
                 rules: [builtin]\n\
                 pipeline:\n  batch: 1h\n  allowed_lateness: 30m\n  max_skew: 2h\n\
                 placement:\n{placement}\
                 broker:\n{broker}",
                self.minio.endpoint()
            ),
        )
        .unwrap();
        plan
    }

    pub fn single_process_plan(&self) -> PathBuf {
        self.write_plan(
            "single",
            "  mode: single_process\n",
            "  max_messages_per_partition: 4\n",
        )
    }

    pub fn distributed_plan(&self, name: &str, workers: u32, broker: &str) -> PathBuf {
        self.write_plan(
            name,
            &format!(
                "  mode: distributed\n  nats_url: {}\n  ingest_workers: {workers}\n",
                self.nats.url()
            ),
            broker,
        )
    }

    pub fn ashiato(&self) -> Command {
        ashiato(self.dir.path())
    }

    pub fn spawn_run(&self, plan: &Path) -> Process {
        Process::spawn(
            self.ashiato()
                .arg("run")
                .arg("--plan")
                .arg(plan)
                .arg("--emit-signals")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
    }

    pub fn spawn_worker(&self, worker_id: u32) -> Process {
        Process::spawn(
            self.ashiato()
                .arg("ingest-worker")
                .arg("--nats")
                .arg(self.nats.url())
                .arg("--run")
                .arg(RUN_ID)
                .arg("--worker-id")
                .arg(worker_id.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::piped()),
        )
    }

    pub fn spawn_workers(&self, workers: u32) -> Vec<Process> {
        (0..workers)
            .map(|worker_id| self.spawn_worker(worker_id))
            .collect()
    }

    pub fn run_single_process(&self) -> (PathBuf, Vec<u8>) {
        let plan = self.single_process_plan();
        let output = finish(self.spawn_run(&plan));
        (plan, output.stdout)
    }

    // The messages that the workers have put into the stream so far, which tells how far the
    // run has gone without the metrics of 14.1.5 節.
    pub fn published(&self) -> u64 {
        runtime().block_on(async {
            let client = async_nats::connect(self.nats.url()).await.unwrap();
            let context = async_nats::jetstream::new(client);
            match context.get_stream(stream_name(RUN_ID)).await {
                Ok(mut stream) => stream
                    .info()
                    .await
                    .map_or(0, |info| info.state.last_sequence),
                Err(_) => 0,
            }
        })
    }

    pub fn wait_until_published(&self, fraction: f64, run: &mut Process) {
        let target = (self.files as f64 * fraction).ceil() as u64;
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        while self.published() < target {
            assert!(
                run.is_running(),
                "the run ended before {target} messages were published"
            );
            assert!(Instant::now() < deadline, "the run did not reach {target}");
            std::thread::sleep(POLL);
        }
    }

    pub fn last_published(&self, partition: &PartitionId) -> Option<Position> {
        runtime().block_on(async {
            let client = async_nats::connect(self.nats.url()).await.unwrap();
            let stream = EventStream::open(client, RUN_ID).await.ok()?;
            stream.last_published(partition).await.unwrap()
        })
    }

    pub fn stream_exists(&self) -> bool {
        runtime().block_on(async {
            let client = async_nats::connect(self.nats.url()).await.unwrap();
            async_nats::jetstream::new(client)
                .get_stream(stream_name(RUN_ID))
                .await
                .is_ok()
        })
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// Credentials come from the environment through the default chain of the AWS SDK. The shared
// configuration files are hidden so that a profile of the user does not take over.
pub fn ashiato(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ashiato"));
    command
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_SESSION_TOKEN")
        .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("AWS_REGION", REGION)
        .env("AWS_CONFIG_FILE", dir.join("no-aws-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            dir.join("no-aws-credentials"),
        )
        .env("AWS_EC2_METADATA_DISABLED", "true");
    command
}

// Kills the process if the test ends before it, so that a failed test leaves no process
// behind to wait for servers that are gone.
pub struct Process(Option<Child>);

impl Process {
    fn spawn(command: &mut Command) -> Self {
        Self(Some(command.spawn().unwrap()))
    }

    pub fn is_running(&mut self) -> bool {
        self.0
            .as_mut()
            .is_some_and(|child| child.try_wait().unwrap().is_none())
    }

    pub fn output(mut self) -> Output {
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub fn finish(process: Process) -> Output {
    let output = process.output();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

pub fn finish_all(processes: Vec<Process>) {
    for process in processes {
        finish(process);
    }
}

// SIGKILL, so that the process stops without any of its own clean up.
pub fn kill(process: &mut Process) {
    if let Some(mut child) = process.0.take() {
        child.kill().unwrap();
        child.wait().unwrap();
    }
}

// The final metrics lines of a role, which every role writes to stderr when it ends (14.1.5 節).
pub fn final_metrics(stderr: &[u8], role: &str) -> Vec<Value> {
    String::from_utf8_lossy(stderr)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["metrics"] == role && line["final"] == true)
        .collect()
}

// Compares the tables in both directions with EXCEPT ALL, so that rows that appear a
// different number of times also count as a difference (14.1.3 節).
pub fn assert_same_tables(reference: &Path, run: &Path) {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH '{}' AS ref (READ_ONLY); ATTACH '{}' AS run (READ_ONLY);",
            reference.with_file_name("ashiato.duckdb").display(),
            run.with_file_name("ashiato.duckdb").display(),
        ))
        .unwrap();
    for table in TABLES {
        for (left, right) in [("ref", "run"), ("run", "ref")] {
            let differing: i64 = connection
                .query_row(
                    &format!(
                        "SELECT count(*) FROM (SELECT * FROM {left}.{table} \
                         EXCEPT ALL SELECT * FROM {right}.{table})"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(differing, 0, "rows of {table} in {left} but not in {right}");
        }
    }
    let pending: i64 = connection
        .query_row("SELECT count(*) FROM run.pending", [], |row| row.get(0))
        .unwrap();
    assert_eq!(pending, 0);
}

pub fn assert_same_run(cluster: &Cluster, reference: (&Path, &[u8]), run: (&Path, &[u8])) {
    assert!(!reference.1.is_empty());
    assert!(
        reference.1 == run.1,
        "the outputs differ:\n{}\n---\n{}",
        String::from_utf8_lossy(reference.1),
        String::from_utf8_lossy(run.1)
    );
    assert_same_tables(reference.0, run.0);
    assert!(!cluster.stream_exists(), "the stream is left after the run");
}
