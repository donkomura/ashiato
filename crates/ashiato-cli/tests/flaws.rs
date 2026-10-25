use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

const DATASET_ENV: &str = "ASHIATO_FLAWS_DIR";
const DEFAULT_SINCE: &str = "2017-03-14";
const DEFAULT_UNTIL: &str = "2020-10-08";
const MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

struct Measured {
    output: Output,
    elapsed: Duration,
    peak_rss_kib: Option<u64>,
}

impl Measured {
    fn report(&self, phase: &str) {
        let peak = self
            .peak_rss_kib
            .map_or("unknown".to_owned(), |kib| format!("{} MiB", kib / 1024));
        eprintln!(
            "{phase}: {:.1} s, peak RSS {peak}",
            self.elapsed.as_secs_f64()
        );
    }
}

// The workspace forbids unsafe code, so getrusage is out of reach. Linux exposes the
// child's peak resident set size as VmHWM, which is sampled until the child exits.
// Growth during the last sampling interval can be missed.
fn run_measured(command: &mut Command) -> Measured {
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
    let status = loop {
        if let Some(kib) = read_peak_rss_kib(&status_path) {
            peak_rss_kib = peak_rss_kib.max(Some(kib));
        }
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        thread::sleep(MEMORY_SAMPLE_INTERVAL);
    };
    Measured {
        output: Output {
            status,
            stdout: stdout.join().unwrap().into_bytes(),
            stderr: stderr.join().unwrap().into_bytes(),
        },
        elapsed: start.elapsed(),
        peak_rss_kib,
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

fn assert_succeeded(phase: &str, measured: &Measured) {
    assert!(
        measured.output.status.success(),
        "{phase} failed: {}",
        String::from_utf8_lossy(&measured.output.stderr)
    );
}

fn count_by(lines: &[serde_json::Value], kind: &str, id_field: &str) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for line in lines.iter().filter(|line| line["type"] == kind) {
        *counts
            .entry(line[id_field].as_str().unwrap().to_owned())
            .or_default() += 1;
    }
    counts
}

#[test]
#[ignore = "needs the flaws.cloud dataset; see docs/flaws-cloud.md"]
fn flaws_cloud_dataset_is_ingested_and_analyzed() {
    let source = env::var_os(DATASET_ENV)
        .unwrap_or_else(|| panic!("set {DATASET_ENV} to the extracted dataset directory"));
    let since = env::var("ASHIATO_FLAWS_SINCE").unwrap_or_else(|_| DEFAULT_SINCE.to_owned());
    let until = env::var("ASHIATO_FLAWS_UNTIL").unwrap_or_else(|_| DEFAULT_UNTIL.to_owned());
    let dir = TempDir::new().unwrap();
    let workspace = dir.path().join("case");

    let ingest = run_measured(
        Command::new(env!("CARGO_BIN_EXE_ashiato"))
            .arg("ingest")
            .arg("--workspace")
            .arg(&workspace)
            .arg("--source")
            .arg(&source),
    );
    assert_succeeded("ingest", &ingest);
    let analyze = run_measured(
        Command::new(env!("CARGO_BIN_EXE_ashiato"))
            .arg("analyze")
            .arg("--workspace")
            .arg(&workspace)
            .args(["--since", &since, "--until", &until])
            .arg("--emit-signals"),
    );
    assert_succeeded("analyze", &analyze);

    let db = duckdb::Connection::open(workspace.join("ashiato.duckdb")).unwrap();
    let events: i64 = db
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert!(events > 0, "no events were ingested from {source:?}");
    let lines: Vec<serde_json::Value> = String::from_utf8(analyze.output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["type"], "run", "{:?}", lines.first());

    eprintln!("analysis window: {since} to {until}");
    eprintln!("events: {events}");
    ingest.report("ingest");
    analyze.report("analyze");
    eprintln!(
        "ingest stderr: {}",
        String::from_utf8_lossy(&ingest.output.stderr).trim()
    );
    for warning in String::from_utf8_lossy(&analyze.output.stderr).lines() {
        eprintln!("analyze stderr: {warning}");
    }
    for (kind, id_field) in [("signal", "signal_id"), ("scenario_match", "scenario_id")] {
        let counts = count_by(&lines, kind, id_field);
        eprintln!("{kind}: {} in total", counts.values().sum::<usize>());
        for (id, count) in counts {
            eprintln!("  {id}: {count}");
        }
    }
}
