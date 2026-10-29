use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};

use super::event::{Call, SDK_AGENT, assumed_role, derived_id, iam_user, user_access_key};
use super::rng::{SplitMix64, hash};

const USERS: u64 = 3;
const APP_ROLE: &str = "app-server";
// CloudTrail delivers the events of about the last five minutes in each file.
const DELIVERY_WINDOW_SECONDS: u64 = 300;
// Only read-only calls, so that the background never completes a scenario of the builtin
// rules and the embedded scenarios are the only matches.
const USER_CALLS: [(&str, &str); 6] = [
    ("ec2.amazonaws.com", "DescribeInstances"),
    ("ec2.amazonaws.com", "DescribeSecurityGroups"),
    ("sts.amazonaws.com", "GetCallerIdentity"),
    ("lambda.amazonaws.com", "ListFunctions20150331"),
    ("logs.amazonaws.com", "DescribeLogGroups"),
    ("cloudformation.amazonaws.com", "DescribeStacks"),
];

pub(crate) fn user_name(index: u64) -> String {
    format!("dev-{index}")
}

pub(crate) fn user_ip(account: &str, index: u64) -> String {
    format!("198.51.100.{}", hash(account) % 50 * 4 + index + 1)
}

pub(crate) fn record(
    rng: &mut SplitMix64,
    account: &str,
    region: &str,
    file_time: DateTime<Utc>,
) -> Value {
    let offset = 1 + rng.below(DELIVERY_WINDOW_SECONDS);
    let time = file_time - TimeDelta::seconds(offset as i64);
    // Applications make most of the calls, and most of theirs are S3 data events, as in the
    // trails that log data events.
    if rng.below(4) == 0 {
        user_call(rng, account, region, time)
    } else {
        app_call(rng, account, region, time)
    }
}

fn user_call(rng: &mut SplitMix64, account: &str, region: &str, time: DateTime<Utc>) -> Value {
    let index = rng.below(USERS);
    let name = user_name(index);
    let key = user_access_key(account, &name);
    let ip = user_ip(account, index);
    let (source, event) = USER_CALLS[rng.below(USER_CALLS.len() as u64) as usize];
    let params = match event {
        "DescribeInstances" => json!({ "instancesSet": {}, "filterSet": {} }),
        "DescribeSecurityGroups" => json!({ "securityGroupSet": {}, "filterSet": {} }),
        "DescribeLogGroups" => json!({ "limit": 50 }),
        _ => Value::Null,
    };
    Call {
        params,
        ..Call::new(iam_user(account, &name, Some(&key)), source, event, &ip)
    }
    .record(rng, account, region, time)
}

fn app_call(rng: &mut SplitMix64, account: &str, region: &str, time: DateTime<Utc>) -> Value {
    let day = time.format("%Y%m%d").to_string();
    let key = derived_id("ASIA", &[account, region, &day]);
    let session = format!("i-{:017x}", hash(&key) >> 4);
    let created = time.date_naive().and_time(Default::default()).and_utc();
    let identity = assumed_role(account, APP_ROLE, &session, &key, created);
    let ip = format!("10.{}.{}.10", hash(account) % 250, hash(region) % 250);
    let bucket = format!("app-data-{account}-{region}");
    let call = Call {
        agent: SDK_AGENT,
        ..Call::new(identity, "s3.amazonaws.com", "PutObject", &ip)
    };
    let call = match rng.below(20) {
        0..10 => object_call(rng, call, &bucket, time, false),
        10..15 => Call {
            name: "HeadObject",
            ..object_call(rng, call, &bucket, time, true)
        },
        15..18 => Call {
            source: "kms.amazonaws.com",
            name: "Decrypt",
            params: json!({ "encryptionAlgorithm": "SYMMETRIC_DEFAULT" }),
            resources: Some(json!([{
                "accountId": account,
                "type": "AWS::KMS::Key",
                "ARN": format!("arn:aws:kms:{region}:{account}:key/{}", derived_id("", &[account, region])),
            }])),
            ..call
        },
        _ => Call {
            source: "ec2.amazonaws.com",
            name: "DescribeInstances",
            params: json!({ "instancesSet": {}, "filterSet": {} }),
            ..call
        },
    };
    call.record(rng, account, region, time)
}

fn object_call<'a>(
    rng: &mut SplitMix64,
    call: Call<'a>,
    bucket: &str,
    time: DateTime<Utc>,
    read_only: bool,
) -> Call<'a> {
    let key = format!(
        "events/{}/{:016x}.json.gz",
        time.format("%Y/%m/%d/%H"),
        rng.next_u64()
    );
    Call {
        params: json!({
            "bucketName": bucket,
            "Host": format!("{bucket}.s3.amazonaws.com"),
            "key": key,
        }),
        read_only,
        resources: Some(json!([
            { "type": "AWS::S3::Object", "ARN": format!("arn:aws:s3:::{bucket}/{key}") },
            { "type": "AWS::S3::Bucket", "ARN": format!("arn:aws:s3:::{bucket}") },
        ])),
        data_event: true,
        ..call
    }
}
