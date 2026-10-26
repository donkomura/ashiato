use std::collections::BTreeMap;
use std::fs;
use std::sync::Mutex;

use ashiato_cli::s3::{DownloadSummary, KeyFilter, Listing, ObjectStore, S3Location, download};
use async_trait::async_trait;
use chrono::NaiveDate;
use tempfile::TempDir;

const ACCOUNT_LOG: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/111111111111_CloudTrail_us-east-1_a.json.gz";
const ORG_LOG: &str = "AWSLogs/o-exampleorg/222222222222/CloudTrail/ap-northeast-1/2026/09/30/222222222222_CloudTrail_ap-northeast-1_b.json.gz";

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

fn filter() -> KeyFilter {
    KeyFilter {
        accounts: vec!["111111111111".to_owned(), "222222222222".to_owned()],
        regions: vec!["us-east-1".to_owned(), "ap-northeast-1".to_owned()],
        from: Some(date("2026-09-30")),
        to: Some(date("2026-10-01")),
    }
}

#[test]
fn location_splits_bucket_and_prefix() {
    assert_eq!(
        S3Location::parse("s3://trail-bucket/AWSLogs/o-exampleorg/").unwrap(),
        S3Location {
            bucket: "trail-bucket".to_owned(),
            prefix: "AWSLogs/o-exampleorg/".to_owned(),
        }
    );
    for uri in ["s3://trail-bucket", "s3://trail-bucket/"] {
        assert_eq!(
            S3Location::parse(uri).unwrap(),
            S3Location {
                bucket: "trail-bucket".to_owned(),
                prefix: String::new(),
            },
            "{uri}"
        );
    }
}

#[test]
fn location_rejects_uris_without_a_bucket() {
    for uri in ["s3://", "s3:///AWSLogs/", "https://trail-bucket/AWSLogs/"] {
        assert!(S3Location::parse(uri).is_err(), "{uri}");
    }
}

#[test]
fn unrestricted_filter_matches_logs_of_account_and_organization_trails() {
    let unrestricted = KeyFilter::default();

    assert!(unrestricted.is_unrestricted());
    assert!(unrestricted.matches(ACCOUNT_LOG));
    assert!(unrestricted.matches(ORG_LOG));
    assert!(unrestricted.matches(&format!("custom/prefix/{ACCOUNT_LOG}")));
    assert!(!filter().is_unrestricted());
}

#[test]
fn filter_matches_keys_within_accounts_regions_and_dates() {
    let filter = filter();

    assert!(filter.matches(ACCOUNT_LOG));
    assert!(filter.matches(ORG_LOG));
    for key in [
        "AWSLogs/333333333333/CloudTrail/us-east-1/2026/10/01/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/eu-west-1/2026/10/01/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/09/29/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/02/a.json.gz",
    ] {
        assert!(!filter.matches(key), "{key}");
    }
}

#[test]
fn filter_rejects_keys_that_are_not_cloudtrail_logs() {
    let filter = KeyFilter::default();

    for key in [
        "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/a.json.gz",
        "AWSLogs/111111111111/CloudTrail-Insight/us-east-1/2026/10/01/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/extra/a.json.gz",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/13/01/a.json.gz",
        "111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz",
    ] {
        assert!(!filter.matches(key), "{key}");
    }
}

#[test]
fn filter_rejects_keys_that_escape_the_evidence_directory() {
    let filter = KeyFilter::default();

    for key in [
        "../AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz",
        "x/./AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz",
        "x//AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz",
        "/AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz",
    ] {
        assert!(!filter.matches(key), "{key}");
    }
}

#[test]
fn prefixes_are_pruned_by_each_level_of_the_hierarchy() {
    let filter = filter();

    for prefix in [
        "",
        "custom/",
        "AWSLogs/",
        "AWSLogs/o-exampleorg/",
        "AWSLogs/111111111111/",
        "AWSLogs/o-exampleorg/222222222222/CloudTrail/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/09/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/",
        "AWSLogs/333",
    ] {
        assert!(filter.may_contain(prefix), "{prefix}");
    }
    for prefix in [
        "AWSLogs/333333333333/",
        "AWSLogs/o-exampleorg/333333333333/",
        "AWSLogs/111111111111/CloudTrail-Digest/",
        "AWSLogs/111111111111/CloudTrail/eu-west-1/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2025/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/08/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/11/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/09/29/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/02/",
    ] {
        assert!(!filter.may_contain(prefix), "{prefix}");
    }
}

#[test]
fn open_ended_date_range_keeps_the_unbounded_side() {
    let since = KeyFilter {
        from: Some(date("2026-10-01")),
        ..KeyFilter::default()
    };
    let until = KeyFilter {
        to: Some(date("2026-09-30")),
        ..KeyFilter::default()
    };

    assert!(since.matches(ACCOUNT_LOG));
    assert!(!since.matches(ORG_LOG));
    assert!(!until.matches(ACCOUNT_LOG));
    assert!(until.matches(ORG_LOG));
}

struct MemoryStore {
    objects: BTreeMap<String, Vec<u8>>,
    listed: Mutex<Vec<String>>,
}

impl MemoryStore {
    fn new(keys: &[&str]) -> Self {
        Self {
            objects: keys
                .iter()
                .map(|key| ((*key).to_owned(), format!("content of {key}").into_bytes()))
                .collect(),
            listed: Mutex::new(Vec::new()),
        }
    }

    fn listed(&self) -> Vec<String> {
        self.listed.lock().unwrap().clone()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn list(&self, bucket: &str, prefix: &str) -> anyhow::Result<Listing> {
        assert_eq!(bucket, "trail-bucket");
        self.listed.lock().unwrap().push(prefix.to_owned());
        let mut listing = Listing::default();
        for key in self.objects.keys() {
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            match rest.find('/') {
                Some(end) => {
                    let child = format!("{prefix}{}", &rest[..=end]);
                    if listing.prefixes.last() != Some(&child) {
                        listing.prefixes.push(child);
                    }
                }
                None => listing.keys.push(key.clone()),
            }
        }
        Ok(listing)
    }

    async fn get(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
        assert_eq!(bucket, "trail-bucket");
        self.objects
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no such key: {key}"))
    }
}

const SKIPPED_LOGS: [&str; 4] = [
    "AWSLogs/333333333333/CloudTrail/us-east-1/2026/10/01/c.json.gz",
    "AWSLogs/111111111111/CloudTrail/eu-west-1/2026/10/01/d.json.gz",
    "AWSLogs/111111111111/CloudTrail/us-east-1/2026/08/31/e.json.gz",
    "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/f.json.gz",
];

fn trail_store() -> MemoryStore {
    let mut keys = vec![ACCOUNT_LOG, ORG_LOG];
    keys.extend(SKIPPED_LOGS);
    MemoryStore::new(&keys)
}

fn location(prefix: &str) -> S3Location {
    S3Location {
        bucket: "trail-bucket".to_owned(),
        prefix: prefix.to_owned(),
    }
}

#[tokio::test]
async fn download_keeps_the_key_hierarchy_of_matching_logs() {
    let store = trail_store();
    let dir = TempDir::new().unwrap();
    let evidence = dir.path().join("evidence");

    let summary = download(&store, &location("AWSLogs/"), &filter(), &evidence)
        .await
        .unwrap();

    assert_eq!(
        summary,
        DownloadSummary {
            downloaded: 2,
            skipped: 0,
        }
    );
    for key in [ACCOUNT_LOG, ORG_LOG] {
        assert_eq!(
            fs::read(evidence.join(key)).unwrap(),
            format!("content of {key}").into_bytes(),
            "{key}"
        );
    }
    for key in SKIPPED_LOGS {
        assert!(!evidence.join(key).exists(), "{key}");
    }
}

#[tokio::test]
async fn download_does_not_list_pruned_prefixes() {
    let store = trail_store();
    let dir = TempDir::new().unwrap();

    download(&store, &location(""), &filter(), dir.path())
        .await
        .unwrap();

    let listed = store.listed();
    assert!(listed.contains(&"AWSLogs/111111111111/".to_owned()));
    for pruned in [
        "AWSLogs/333333333333/",
        "AWSLogs/111111111111/CloudTrail/eu-west-1/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/2026/08/",
        "AWSLogs/111111111111/CloudTrail-Digest/",
    ] {
        assert!(
            !listed.contains(&pruned.to_owned()),
            "{pruned}: {listed:#?}"
        );
    }
}

#[tokio::test]
async fn download_skips_logs_already_in_the_evidence_directory() {
    let store = trail_store();
    let dir = TempDir::new().unwrap();
    download(&store, &location("AWSLogs/"), &filter(), dir.path())
        .await
        .unwrap();

    let summary = download(&store, &location("AWSLogs/"), &filter(), dir.path())
        .await
        .unwrap();

    assert_eq!(
        summary,
        DownloadSummary {
            downloaded: 0,
            skipped: 2,
        }
    );
}

#[tokio::test]
async fn download_creates_the_evidence_directory_when_nothing_matches() {
    let store = MemoryStore::new(&SKIPPED_LOGS);
    let dir = TempDir::new().unwrap();
    let evidence = dir.path().join("evidence");

    let summary = download(&store, &location(""), &filter(), &evidence)
        .await
        .unwrap();

    assert_eq!(
        summary,
        DownloadSummary {
            downloaded: 0,
            skipped: 0,
        }
    );
    assert!(evidence.is_dir());
}
