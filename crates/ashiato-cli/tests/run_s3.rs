use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::minio::{ACCESS_KEY, Minio, REGION, SECRET_KEY};
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use tempfile::TempDir;

const BUCKET: &str = "trail-bucket";
const ORG_ID: &str = "o-abcdef1234";

const CONSOLE_LOGIN_RULE: &str = r#"
kind: signal
id: test_console_login
title: コンソールへのログイン
severity: low
tactics: [TA0001]
sql: |
  SELECT
    event_time           AS start_time,
    event_time           AS end_time,
    [event_id]           AS event_ids,
    access_key_id,
    principal_key,
    source_ip,
    recipient_account_id AS account_id,
    aws_region           AS region
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name = 'ConsoleLogin'
"#;

const REPEATED_CONSOLE_LOGIN_RULE: &str = r#"
kind: scenario
id: test_repeated_console_login
title: 繰り返されるコンソールへのログイン
severity: medium
tactics: [TA0001]
window: 1h
steps:
  - signal: test_console_login
  - signal: test_console_login
"#;

fn at(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, hour, minute, 0).unwrap()
}

fn login(event_id: &str, account: &str, time: DateTime<Utc>) -> Value {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("eventTime", time.to_rfc3339())
        .set("eventSource", "signin.amazonaws.com")
        .set("eventName", "ConsoleLogin")
        .set("recipientAccountId", account)
        .set("userIdentity.accessKeyId", Value::Null)
        .build()
}

struct Case {
    dir: TempDir,
}

impl Case {
    fn new() -> Self {
        let case = Self {
            dir: TempDir::new().unwrap(),
        };
        for (id, content) in [
            ("test_console_login", CONSOLE_LOGIN_RULE),
            ("test_repeated_console_login", REPEATED_CONSOLE_LOGIN_RULE),
        ] {
            let rule_dir = case.rules().join(id);
            fs::create_dir_all(&rule_dir).unwrap();
            fs::write(rule_dir.join("rule.yaml"), content).unwrap();
        }
        let east = |org_id: Option<&str>, time, records| LogFile {
            org_id: org_id.map(str::to_owned),
            records,
            ..LogFile::new("111111111111", "us-east-1", time)
        };
        for log in [
            east(
                None,
                at(10, 5),
                vec![login("evt-login-1", "111111111111", at(10, 0))],
            ),
            east(
                Some(ORG_ID),
                at(10, 10),
                vec![login("evt-login-1", "111111111111", at(10, 0))],
            ),
            east(
                Some(ORG_ID),
                at(10, 25),
                vec![login("evt-login-2", "111111111111", at(10, 20))],
            ),
            east(
                None,
                at(12, 0),
                vec![login("evt-late", "111111111111", at(11, 0))],
            ),
            LogFile {
                records: vec![login("evt-login-3", "222222222222", at(10, 30))],
                ..LogFile::new("222222222222", "ap-northeast-1", at(10, 35))
            },
        ] {
            write_log_file(&case.logs(), &log);
        }
        let broken = case
            .logs()
            .join(LogFile::new("111111111111", "us-east-1", at(12, 5)).relative_path());
        fs::write(&broken, b"not a gzip stream").unwrap();
        let digest = case.logs().join(
            "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/\
             111111111111_CloudTrail_us-east-1_20261001T1001Z_digest.json.gz",
        );
        fs::create_dir_all(digest.parent().unwrap()).unwrap();
        fs::write(digest, b"not a cloudtrail log").unwrap();
        case
    }

    fn logs(&self) -> PathBuf {
        self.dir.path().join("logs")
    }

    fn rules(&self) -> PathBuf {
        self.dir.path().join("rules")
    }

    // Both plans name the same rule directory by its absolute path, so that the rule sources
    // in the run records are the same.
    fn write_plan(&self, workspace: &str, source: &str) -> PathBuf {
        let plan = self.dir.path().join(workspace).join("run.yaml");
        fs::create_dir_all(plan.parent().unwrap()).unwrap();
        fs::write(
            &plan,
            format!(
                "run_id: case-1\n\
                 source:\n{source}\
                 analysis_window:\n  since: 2026-10-01T00:00:00Z\n  until: 2026-10-02T00:00:00Z\n\
                 rules: [builtin, {}]\n\
                 rule: [test_repeated_console_login]\n\
                 pipeline:\n  batch: 1h\n  allowed_lateness: 30m\n\
                 placement:\n  mode: single_process\n",
                self.rules().display()
            ),
        )
        .unwrap();
        plan
    }
}

fn run(plan: &Path) -> Vec<Value> {
    // Credentials come from the environment through the default chain of the AWS SDK. The
    // shared configuration files are hidden so that a profile of the user does not take over.
    let output = Command::new(env!("CARGO_BIN_EXE_ashiato"))
        .arg("run")
        .arg("--plan")
        .arg(plan)
        .arg("--emit-signals")
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_SESSION_TOKEN")
        .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("AWS_REGION", REGION)
        .env("AWS_CONFIG_FILE", plan.with_file_name("no-aws-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            plan.with_file_name("no-aws-credentials"),
        )
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
fn run_from_s3_gives_the_same_output_as_from_the_local_copy() {
    let minio = Minio::start();
    let case = Case::new();
    minio.create_bucket(BUCKET, true);
    minio.upload_dir(&case.logs(), BUCKET);
    let local = case.write_plan(
        "local",
        &format!("  kind: local\n  uri: {}\n", case.logs().display()),
    );
    let s3 = case.write_plan(
        "s3",
        &format!(
            "  kind: s3\n  uri: s3://{BUCKET}/AWSLogs/\n  endpoint_url: {}\n",
            minio.endpoint()
        ),
    );

    let mut from_local = run(&local);
    let mut from_s3 = run(&s3);

    let run_record = &from_s3[0];
    assert_eq!(run_record["type"], "run", "{from_s3:#?}");
    assert_eq!(run_record["ingested_files"], 5, "{run_record}");
    assert_eq!(run_record["file_errors"], 1, "{run_record}");
    assert_eq!(run_record["late_events"], 1, "{run_record}");
    assert!(
        from_s3.iter().any(|line| line["type"] == "scenario_match"),
        "{from_s3:#?}"
    );
    // The plans differ only in the source, so the plan digest is the only field that differs.
    for lines in [&mut from_local, &mut from_s3] {
        lines[0].as_object_mut().unwrap().remove("plan_digest");
    }
    assert_eq!(from_s3, from_local);

    let db = duckdb::Connection::open(s3.with_file_name("ashiato.duckdb")).unwrap();
    let (files, versioned, uris): (i64, i64, i64) = db
        .query_row(
            "SELECT count(*), count(version_id) FILTER (WHERE etag IS NOT NULL), \
                    count(*) FILTER (WHERE uri LIKE 's3://trail-bucket/AWSLogs/%') \
             FROM ingested_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((files, versioned, uris), (6, 6, 6));
}
