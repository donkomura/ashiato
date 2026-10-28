use ashiato_ingest::{
    ListScope, S3Listing, S3Location, S3Object, TrailRoot, arrange_objects, day_prefixes,
    trail_root,
};
use ashiato_pipeline::{EventTime, ManifestEntry, PartitionId, PartitionManifest};
use chrono::{TimeZone, Utc};

const BUCKET: &str = "trail-bucket";
const ACCOUNT_DAY: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01";
const ORG_DAY: &str = "AWSLogs/o-abcdef1234/111111111111/CloudTrail/us-east-1/2026/10/01";

fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

fn whole_october() -> ListScope<'static> {
    ListScope {
        accounts: None,
        regions: None,
        period: at(1, 0, 0)..at(31, 0, 0),
    }
}

fn object(key: &str, version: Option<&str>) -> S3Object {
    S3Object {
        key: key.to_owned(),
        version_id: version.map(str::to_owned),
        etag: Some(format!("\"etag-of-{}\"", key.len())),
        size: key.len() as u64,
    }
}

fn log_key(dir: &str, account: &str, region: &str, time: &str, unique: &str) -> String {
    format!("{dir}/{account}_CloudTrail_{region}_{time}_{unique}.json.gz")
}

fn entry(file_seq: u64, object: &S3Object, file_time: EventTime) -> ManifestEntry {
    ManifestEntry {
        file_seq,
        uri: format!("s3://{BUCKET}/{}", object.key),
        version_id: object.version_id.clone(),
        etag: object.etag.clone(),
        size: object.size,
        file_time,
    }
}

fn partition(account: &str, region: &str, files: Vec<ManifestEntry>) -> PartitionManifest {
    PartitionManifest {
        partition: PartitionId::new(account, region),
        files,
    }
}

#[test]
fn location_splits_bucket_and_prefix() {
    assert_eq!(
        S3Location::parse("s3://trail-bucket/AWSLogs/o-abcdef1234/").unwrap(),
        S3Location {
            bucket: "trail-bucket".to_owned(),
            prefix: "AWSLogs/o-abcdef1234/".to_owned(),
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
    for uri in ["s3://", "s3:///AWSLogs/", "https://trail-bucket/AWSLogs/"] {
        assert!(S3Location::parse(uri).is_err(), "{uri}");
    }
}

#[test]
fn trail_root_is_the_aws_logs_directory_or_an_organization_under_it() {
    for (prefix, root) in [
        ("", TrailRoot::AwsLogs("AWSLogs/".to_owned())),
        ("AWSLogs", TrailRoot::AwsLogs("AWSLogs/".to_owned())),
        ("AWSLogs/", TrailRoot::AwsLogs("AWSLogs/".to_owned())),
        ("custom/", TrailRoot::AwsLogs("custom/AWSLogs/".to_owned())),
        (
            "custom/AWSLogs/",
            TrailRoot::AwsLogs("custom/AWSLogs/".to_owned()),
        ),
        (
            "AWSLogs/o-abcdef1234",
            TrailRoot::Organization("AWSLogs/o-abcdef1234/".to_owned()),
        ),
        (
            "custom/AWSLogs/o-abcdef1234/",
            TrailRoot::Organization("custom/AWSLogs/o-abcdef1234/".to_owned()),
        ),
    ] {
        assert_eq!(trail_root(prefix).unwrap(), root, "{prefix}");
    }
}

#[test]
fn trail_root_rejects_prefixes_below_the_organization() {
    for prefix in [
        "AWSLogs/111111111111/",
        "AWSLogs/o-abcdef1234/111111111111/",
        "AWSLogs/111111111111/CloudTrail/us-east-1/",
    ] {
        assert!(trail_root(prefix).is_err(), "{prefix}");
    }
}

#[test]
fn day_prefixes_cover_every_day_of_the_half_open_period() {
    assert_eq!(
        day_prefixes(&(at(2, 0, 0)..at(2, 0, 0))),
        Vec::<String>::new(),
        "an empty period has no days"
    );
    assert_eq!(
        day_prefixes(&(at(1, 0, 0)..at(3, 0, 0))),
        ["2026/10/01/", "2026/10/02/"]
    );
    assert_eq!(
        day_prefixes(&(at(1, 23, 59)..at(3, 0, 1))),
        ["2026/10/01/", "2026/10/02/", "2026/10/03/"]
    );
    let year_end = Utc.with_ymd_and_hms(2026, 12, 31, 10, 0, 0).unwrap();
    let next_year = Utc.with_ymd_and_hms(2027, 1, 1, 10, 0, 0).unwrap();
    assert_eq!(
        day_prefixes(&(year_end..next_year)),
        ["2026/12/31/", "2027/01/01/"]
    );
}

#[test]
fn objects_are_ordered_by_file_time_then_file_name_across_trails() {
    let org_late_b = object(
        &log_key(ORG_DAY, "111111111111", "us-east-1", "20261001T1005Z", "b"),
        Some("v-org"),
    );
    let account_late_a = object(
        &log_key(
            ACCOUNT_DAY,
            "111111111111",
            "us-east-1",
            "20261001T1005Z",
            "a",
        ),
        Some("v-account"),
    );
    let account_early = object(
        &log_key(
            ACCOUNT_DAY,
            "111111111111",
            "us-east-1",
            "20261001T1000Z",
            "z",
        ),
        None,
    );
    let other_region = object(
        &log_key(
            "AWSLogs/111111111111/CloudTrail/ap-northeast-1/2026/10/01",
            "111111111111",
            "ap-northeast-1",
            "20261001T1100Z",
            "a",
        ),
        None,
    );

    let listing = arrange_objects(
        BUCKET,
        vec![
            org_late_b.clone(),
            other_region.clone(),
            account_late_a.clone(),
            account_early.clone(),
        ],
        &whole_october(),
    );

    assert_eq!(
        listing,
        S3Listing {
            manifest: ashiato_pipeline::Manifest {
                partitions: vec![
                    partition(
                        "111111111111",
                        "ap-northeast-1",
                        vec![entry(0, &other_region, at(1, 11, 0))],
                    ),
                    partition(
                        "111111111111",
                        "us-east-1",
                        vec![
                            entry(0, &account_early, at(1, 10, 0)),
                            entry(1, &account_late_a, at(1, 10, 5)),
                            entry(2, &org_late_b, at(1, 10, 5)),
                        ],
                    ),
                ],
            },
            unrecognized: Vec::new(),
        }
    );
}

#[test]
fn objects_outside_the_scope_digest_and_insight_are_left_out() {
    let kept = object(
        &log_key(
            ACCOUNT_DAY,
            "111111111111",
            "us-east-1",
            "20261001T1000Z",
            "a",
        ),
        None,
    );
    let objects = vec![
        kept.clone(),
        object(
            &log_key(
                ACCOUNT_DAY,
                "111111111111",
                "us-east-1",
                "20261003T0000Z",
                "after",
            ),
            None,
        ),
        object(
            &log_key(
                "AWSLogs/222222222222/CloudTrail/us-east-1/2026/10/01",
                "222222222222",
                "us-east-1",
                "20261001T1000Z",
                "a",
            ),
            None,
        ),
        object(
            &log_key(
                "AWSLogs/111111111111/CloudTrail/eu-west-1/2026/10/01",
                "111111111111",
                "eu-west-1",
                "20261001T1000Z",
                "a",
            ),
            None,
        ),
        object(
            &log_key(
                "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01",
                "111111111111",
                "us-east-1",
                "20261001T1001Z",
                "digest",
            ),
            None,
        ),
        object(
            &log_key(
                "AWSLogs/111111111111/CloudTrail-Insight/us-east-1/2026/10/01",
                "111111111111",
                "us-east-1",
                "20261001T1001Z",
                "insight",
            ),
            None,
        ),
        object(&format!("{ACCOUNT_DAY}/README.txt"), None),
    ];
    let accounts = ["111111111111".to_owned()];
    let regions = ["us-east-1".to_owned(), "ap-northeast-1".to_owned()];
    let scope = ListScope {
        accounts: Some(&accounts),
        regions: Some(&regions),
        period: at(1, 0, 0)..at(3, 0, 0),
    };

    let listing = arrange_objects(BUCKET, objects, &scope);

    assert_eq!(
        listing.manifest.partitions,
        vec![
            partition("111111111111", "ap-northeast-1", Vec::new()),
            partition(
                "111111111111",
                "us-east-1",
                vec![entry(0, &kept, at(1, 10, 0))],
            ),
        ],
        "the given partitions are kept even without files"
    );
    assert!(
        listing.unrecognized.is_empty(),
        "{:?}",
        listing.unrecognized
    );
}

#[test]
fn objects_whose_names_do_not_follow_cloudtrail_are_reported() {
    let unrecognized = [
        format!("{ACCOUNT_DAY}/notes.json.gz"),
        format!("{ACCOUNT_DAY}/11111_CloudTrail_us-east-1_20261001T1000Z_a.json.gz"),
        format!("{ACCOUNT_DAY}/111111111111_CloudTrail_us-east-1_20261001T1000Z_.json.gz"),
    ];
    let objects = unrecognized.iter().map(|key| object(key, None)).collect();

    let listing = arrange_objects(BUCKET, objects, &whole_october());

    assert!(listing.manifest.partitions.is_empty());
    let mut expected: Vec<String> = unrecognized
        .iter()
        .map(|key| format!("s3://{BUCKET}/{key}"))
        .collect();
    expected.sort();
    assert_eq!(listing.unrecognized, expected);
}
