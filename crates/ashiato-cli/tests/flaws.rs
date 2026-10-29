use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use ashiato_testkit::log_file::LogFile;
use ashiato_testkit::minio::{ACCESS_KEY, Minio, REGION, SECRET_KEY};
use chrono::{DateTime, TimeDelta, Utc};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde_json::Value;
use serde_json::value::RawValue;
use tempfile::TempDir;

const DATASET_ENV: &str = "ASHIATO_FLAWS_DIR";
const DEFAULT_SINCE: &str = "2017-03-14";
const DEFAULT_UNTIL: &str = "2020-10-08";
const BUCKET: &str = "flaws-trail";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(20);
// The records are regrouped into files of five minutes of event time, named after the end of
// their five minutes. Every event is then at most FILE_SPAN before the time of its file, so an
// allowed_lateness of FILE_SPAN makes no event late.
const FILE_SPAN_SECONDS: i64 = 5 * 60;
const ALLOWED_LATENESS: &str = "5m";

struct Measured {
    output: Output,
    elapsed: Duration,
    peak_rss_kib: Option<u64>,
    peak_workspace_bytes: u64,
}

// The workspace forbids unsafe code, so getrusage is out of reach. Linux exposes the
// child's peak resident set size as VmHWM, which is sampled until the child exits.
// Growth during the last sampling interval can be missed, and so can the peak of the
// workspace size, which is sampled at the same time.
fn run_measured(command: &mut Command, workspace: &Path) -> Measured {
    let start = Instant::now();
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let status_path = PathBuf::from(format!("/proc/{}/status", child.id()));
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stdout = thread::spawn(move || std::io::read_to_string(stdout).unwrap());
    let stderr = thread::spawn(move || std::io::read_to_string(stderr).unwrap());
    let mut peak_rss_kib = None;
    let mut peak_workspace_bytes = 0;
    let status = loop {
        if let Some(kib) = read_peak_rss_kib(&status_path) {
            peak_rss_kib = peak_rss_kib.max(Some(kib));
        }
        peak_workspace_bytes = peak_workspace_bytes.max(disk_usage(workspace));
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        thread::sleep(SAMPLE_INTERVAL);
    };
    Measured {
        output: Output {
            status,
            stdout: stdout.join().unwrap().into_bytes(),
            stderr: stderr.join().unwrap().into_bytes(),
        },
        elapsed: start.elapsed(),
        peak_rss_kib,
        peak_workspace_bytes,
    }
}

fn read_peak_rss_kib(status_path: &Path) -> Option<u64> {
    fs::read_to_string(status_path)
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()
}

fn disk_usage(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.metadata() {
            Ok(metadata) if metadata.is_dir() => disk_usage(&entry.path()),
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        })
        .sum()
}

#[derive(Debug, Default)]
struct Layout {
    records: u64,
    files: u64,
}

// The dataset is 20 files of up to 100,000 records each in event time order, not in the key
// layout of CloudTrail. The records of each dataset file are grouped by recipientAccountId,
// awsRegion and five minutes of eventTime, and each group is written to its own log file
// under AWSLogs/<account>/CloudTrail/<region>/YYYY/MM/DD/. A five minutes that spans two
// dataset files gets one log file from each, told apart by the unique part of the name.
fn lay_out(dataset: &Path, root: &Path) -> Layout {
    let mut names: Vec<PathBuf> = fs::read_dir(dataset)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".json.gz"))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "no .json.gz files in {dataset:?}");
    let mut layout = Layout::default();
    for (index, path) in names.iter().enumerate() {
        let reader = GzDecoder::new(BufReader::new(File::open(path).unwrap()));
        let content: BTreeMap<String, Vec<Box<RawValue>>> =
            serde_json::from_reader(reader).unwrap();
        let mut groups: BTreeMap<(String, String, i64), Vec<&RawValue>> = BTreeMap::new();
        for record in &content["Records"] {
            let fields: Value = serde_json::from_str(record.get()).unwrap();
            let text = |name: &str| {
                fields[name]
                    .as_str()
                    .unwrap_or_else(|| panic!("a record of {path:?} has no {name}"))
                    .to_owned()
            };
            let time = DateTime::parse_from_rfc3339(&text("eventTime"))
                .unwrap()
                .timestamp();
            let start = time - time.rem_euclid(FILE_SPAN_SECONDS);
            groups
                .entry((text("recipientAccountId"), text("awsRegion"), start))
                .or_default()
                .push(record);
            layout.records += 1;
        }
        for ((account, region, start), records) in groups {
            let end = DateTime::<Utc>::from_timestamp(start, 0).unwrap()
                + TimeDelta::seconds(FILE_SPAN_SECONDS);
            let log = LogFile {
                unique: format!("flaws{index:02}"),
                ..LogFile::new(account, region, end)
            };
            write_records(&root.join(log.relative_path()), &records);
            layout.files += 1;
        }
    }
    layout
}

fn write_records(path: &Path, records: &[&RawValue]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    encoder.write_all(b"{\"Records\":[").unwrap();
    for (index, record) in records.iter().enumerate() {
        if index > 0 {
            encoder.write_all(b",").unwrap();
        }
        encoder.write_all(record.get().as_bytes()).unwrap();
    }
    encoder.write_all(b"]}").unwrap();
    encoder.finish().unwrap();
}

fn count_by(lines: &[Value], kind: &str, id_field: &str) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for line in lines.iter().filter(|line| line["type"] == kind) {
        *counts
            .entry(line[id_field].as_str().unwrap().to_owned())
            .or_default() += 1;
    }
    counts
}

#[test]
#[ignore = "needs the flaws.cloud dataset and Docker for MinIO; see docs/flaws-cloud.md"]
fn flaws_cloud_dataset_is_streamed_from_s3() {
    let dataset = env::var_os(DATASET_ENV)
        .unwrap_or_else(|| panic!("set {DATASET_ENV} to the extracted dataset directory"));
    let since = env::var("ASHIATO_FLAWS_SINCE").unwrap_or_else(|_| DEFAULT_SINCE.to_owned());
    let until = env::var("ASHIATO_FLAWS_UNTIL").unwrap_or_else(|_| DEFAULT_UNTIL.to_owned());
    let dir = TempDir::new().unwrap();
    let workspace = dir.path().join("case");
    fs::create_dir_all(&workspace).unwrap();

    let preparing = Instant::now();
    let layout = lay_out(Path::new(&dataset), &dir.path().join("bucket"));
    let minio = Minio::start();
    minio.create_bucket(BUCKET, true);
    minio.upload_dir(&dir.path().join("bucket"), BUCKET);
    let prepared = preparing.elapsed();

    let plan = workspace.join("run.yaml");
    fs::write(
        &plan,
        format!(
            "run_id: flaws\n\
             source:\n  kind: s3\n  uri: s3://{BUCKET}/AWSLogs/\n  endpoint_url: {}\n\
             analysis_window:\n  since: {since}T00:00:00Z\n  until: {until}T00:00:00Z\n\
             rules: [builtin]\n\
             pipeline:\n  batch: 1h\n  allowed_lateness: {ALLOWED_LATENESS}\n\
             placement:\n  mode: single_process\n",
            minio.endpoint()
        ),
    )
    .unwrap();
    // Credentials come from the environment through the default chain of the AWS SDK. The
    // shared configuration files are hidden so that a profile of the user does not take over.
    let run = run_measured(
        Command::new(env!("CARGO_BIN_EXE_ashiato"))
            .arg("run")
            .arg("--plan")
            .arg(&plan)
            .arg("--emit-signals")
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_SESSION_TOKEN")
            .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .env("AWS_REGION", REGION)
            .env("AWS_CONFIG_FILE", dir.path().join("no-aws-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                dir.path().join("no-aws-credentials"),
            )
            .env("AWS_EC2_METADATA_DISABLED", "true"),
        &workspace,
    );
    let stderr = String::from_utf8_lossy(&run.output.stderr).into_owned();
    assert!(run.output.status.success(), "run failed: {stderr}");

    let lines: Vec<Value> = String::from_utf8(run.output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let record = &lines[0];
    assert_eq!(record["type"], "run", "{record}");
    assert_eq!(record["ingested_files"], layout.files, "{record}");
    assert_eq!(record["file_errors"], 0, "{record}");
    assert_eq!(
        record["late_events"], 0,
        "no event is older than its file by more than allowed_lateness: {record}"
    );
    let metrics: Vec<Value> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["metrics"] == "analyzer" && line["final"] == true)
        .collect();
    assert_eq!(metrics.len(), 1, "{stderr}");
    let pending_max_rows = metrics[0]["pending_max_rows"].as_u64().unwrap();
    assert!(pending_max_rows > 0, "{}", metrics[0]);
    assert_eq!(metrics[0]["ingested_files"], layout.files, "{}", metrics[0]);

    eprintln!("analysis window: {since} to {until}");
    eprintln!(
        "dataset: {} records in {} log files, allowed_lateness {ALLOWED_LATENESS}",
        layout.records, layout.files
    );
    eprintln!("preparing MinIO: {:.1} s", prepared.as_secs_f64());
    let peak = run
        .peak_rss_kib
        .map_or("unknown".to_owned(), |kib| format!("{} MiB", kib / 1024));
    eprintln!("run: {:.1} s, peak RSS {peak}", run.elapsed.as_secs_f64());
    eprintln!(
        "workspace: {} MiB at the peak, {} MiB at the end",
        run.peak_workspace_bytes >> 20,
        disk_usage(&workspace) >> 20
    );
    eprintln!("pending_max_rows: {pending_max_rows}");
    eprintln!(
        "run record: ingested_files {}, file_errors {}, late_events {}, data {}",
        record["ingested_files"], record["file_errors"], record["late_events"], record["data"]
    );
    for line in stderr.lines() {
        eprintln!("run stderr: {line}");
    }
    for (kind, id_field) in [
        ("signal", "signal_id"),
        ("scenario_match", "scenario_id"),
        ("rule_skipped", "rule_id"),
    ] {
        let counts = count_by(&lines, kind, id_field);
        eprintln!("{kind}: {} in total", counts.values().sum::<usize>());
        for (id, count) in counts {
            eprintln!("  {id}: {count}");
        }
    }
}
