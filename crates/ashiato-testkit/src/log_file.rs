use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq)]
pub struct LogFile {
    pub org_id: Option<String>,
    pub account: String,
    pub region: String,
    pub time: DateTime<Utc>,
    pub unique: String,
    pub records: Vec<Value>,
}

impl LogFile {
    pub fn new(account: impl Into<String>, region: impl Into<String>, time: DateTime<Utc>) -> Self {
        Self {
            org_id: None,
            account: account.into(),
            region: region.into(),
            time,
            unique: "AbCdEfGhIjKlMnOp".to_owned(),
            records: Vec::new(),
        }
    }

    pub fn file_name(&self) -> String {
        format!(
            "{}_CloudTrail_{}_{}_{}.json.gz",
            self.account,
            self.region,
            self.time.format("%Y%m%dT%H%MZ"),
            self.unique
        )
    }

    // CloudTrail delivers log files under this key layout.
    // https://docs.aws.amazon.com/awscloudtrail/latest/userguide/get-and-view-cloudtrail-log-files.html
    pub fn relative_path(&self) -> PathBuf {
        let mut path = PathBuf::from("AWSLogs");
        if let Some(org_id) = &self.org_id {
            path.push(org_id);
        }
        path.push(&self.account);
        path.push("CloudTrail");
        path.push(&self.region);
        path.push(self.time.format("%Y/%m/%d").to_string());
        path.push(self.file_name());
        path
    }
}

pub fn write_log_file(root: &Path, log: &LogFile) -> PathBuf {
    let path = root.join(log.relative_path());
    let dir = path.parent().expect("a log file path has a directory");
    fs::create_dir_all(dir).unwrap_or_else(|error| panic!("failed to create {dir:?}: {error}"));
    let file =
        File::create(&path).unwrap_or_else(|error| panic!("failed to create {path:?}: {error}"));
    let mut encoder = GzEncoder::new(file, Compression::default());
    let content = json!({ "Records": log.records }).to_string();
    encoder
        .write_all(content.as_bytes())
        .and_then(|()| encoder.finish().map(drop))
        .unwrap_or_else(|error| panic!("failed to write {path:?}: {error}"));
    path
}
