use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};

use super::background::{user_ip, user_name};
use super::event::{
    CLI_AGENT, Call, assumed_role, derived_id, format_time, iam_user, role_arn, user_access_key,
    user_arn,
};
use super::rng::{SplitMix64, hash, mix};
use super::{
    A_REGIONS, ACCOUNTS, B_REGIONS, DatasetKind, ExpectedOrigin, ExpectedScenario, Partition,
    baseline_since, gzip, since,
};

const BROWSER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)";
const ATTACKER_AGENT: &str = "python-requests/2.31.0";
const DELIVERY_MINUTES: i64 = 5;
const MINUTE: i64 = 60;
const HOUR: i64 = 60 * MINUTE;

pub(crate) struct Scripted {
    pub(crate) partition: Partition,
    pub(crate) file_time: DateTime<Utc>,
    pub(crate) record: Value,
}

pub(crate) struct RawFile {
    pub(crate) partition: Partition,
    pub(crate) file_time: DateTime<Utc>,
    pub(crate) content: Vec<u8>,
}

// The files of [start, end] of the partition are also delivered by an account trail.
pub(crate) struct AccountTrail {
    pub(crate) partition: Partition,
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
}

pub(crate) struct Contents {
    rng: SplitMix64,
    pub(crate) events: Vec<Scripted>,
    pub(crate) expected: Vec<ExpectedScenario>,
    pub(crate) raw_files: Vec<RawFile>,
    pub(crate) account_trail: Option<AccountTrail>,
}

fn partition(account: usize, region: &'static str) -> Partition {
    Partition {
        account: ACCOUNTS[account],
        region,
    }
}

fn at(day: i64, seconds: i64) -> DateTime<Utc> {
    since() + TimeDelta::days(day) + TimeDelta::seconds(seconds)
}

// The file that CloudTrail delivers next after the event.
fn delivery(time: DateTime<Utc>) -> DateTime<Utc> {
    let interval = DELIVERY_MINUTES * MINUTE;
    DateTime::from_timestamp((time.timestamp() / interval + 1) * interval, 0)
        .expect("a delivery time is in range")
}

pub(crate) fn contents(kind: DatasetKind, seed: u64) -> Contents {
    let mut contents = Contents {
        rng: SplitMix64::new(mix(&[seed, hash("scripted")])),
        events: Vec::new(),
        expected: Vec::new(),
        raw_files: Vec::new(),
        account_trail: None,
    };
    match kind {
        DatasetKind::A | DatasetKind::ANoDataEvents => contents.dataset_a(),
        DatasetKind::B => contents.dataset_b(),
    }
    if kind == DatasetKind::ANoDataEvents {
        contents
            .expected
            .retain(|scenario| scenario.scenario_id != "cross_account_s3_destruction");
    }
    contents
}

impl Contents {
    fn dataset_a(&mut self) {
        let [east, west, tokyo] = A_REGIONS;
        let console = partition(1, east);
        self.console_takeover_mining(console, 1);
        // The session issued in the baseline is used in the analysis period, and its lineage
        // goes from 111111111111 to 333333333333.
        self.role_chain_escalation(partition(0, east), partition(2, east), 2);
        self.leaked_key_recon_persistence(partition(3, west), 3);
        self.cross_account_s3_destruction(partition(1, tokyo), partition(0, west), 4);
        // The account trail also delivers the console takeover, so a duplicate that is not
        // dropped would make the scenario match twice.
        self.account_trail = Some(AccountTrail {
            partition: console,
            start: at(1, 9 * HOUR + 55 * MINUTE),
            end: at(1, 10 * HOUR + 25 * MINUTE),
        });
        self.late(
            partition(2, east),
            at(3, 12 * HOUR + 2 * MINUTE),
            &[47, 122],
        );
        self.late(partition(3, west), at(-5, 8 * HOUR + 3 * MINUTE), &[43]);
        self.broken(partition(2, west), at(2, 6 * HOUR));
    }

    fn dataset_b(&mut self) {
        let [east, west, _, ireland, frankfurt, singapore] = B_REGIONS;
        self.console_takeover_mining(partition(2, ireland), 5);
        self.role_chain_escalation(partition(3, frankfurt), partition(1, east), 4);
        self.leaked_key_recon_persistence(partition(0, singapore), 3);
        self.cross_account_s3_destruction(partition(2, west), partition(3, ireland), 2);
    }

    fn add(&mut self, partition: Partition, time: DateTime<Utc>, call: Call<'_>) -> &mut Value {
        self.add_at(partition, time, delivery(time), call)
    }

    fn add_at(
        &mut self,
        partition: Partition,
        time: DateTime<Utc>,
        file_time: DateTime<Utc>,
        call: Call<'_>,
    ) -> &mut Value {
        let record = call.record(&mut self.rng, partition.account, partition.region, time);
        self.events.push(Scripted {
            partition,
            file_time,
            record,
        });
        &mut self.events.last_mut().expect("an event was pushed").record
    }

    fn expect(
        &mut self,
        scenario_id: &str,
        (kind, id): (&str, String),
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) {
        self.expected.push(ExpectedScenario {
            scenario_id: scenario_id.to_owned(),
            origin: ExpectedOrigin {
                id,
                kind: kind.to_owned(),
            },
            start_time,
            end_time,
        });
    }

    fn assume_role(
        &mut self,
        partition: Partition,
        time: DateTime<Utc>,
        caller: Value,
        ip: &str,
        (account, role): (&str, &str),
        session: &str,
    ) -> String {
        let child = derived_id("ASIA", &[account, role, session]);
        let call = Call {
            params: json!({ "roleArn": role_arn(account, role), "roleSessionName": session }),
            response: json!({
                "credentials": {
                    "accessKeyId": child,
                    "sessionToken": derived_id("", &[&child, "token"]),
                    "expiration": format_time(time + TimeDelta::hours(1)),
                },
                "assumedRoleUser": {
                    "assumedRoleId": format!("{}:{session}", derived_id("AROA", &[account, role])),
                    "arn": format!("arn:aws:sts::{account}:assumed-role/{role}/{session}"),
                },
            }),
            ..Call::new(caller, "sts.amazonaws.com", "AssumeRole", ip)
        };
        self.add(partition, time, call);
        child
    }

    // The RunInstances event is a little newer than the time of its file.
    fn console_takeover_mining(&mut self, target: Partition, day: i64) {
        let (account, name, ip) = (target.account, "alice-console", "203.0.113.50");
        let login = at(day, 10 * HOUR);
        let login_record = self.add(
            target,
            login,
            Call {
                agent: BROWSER_AGENT,
                response: json!({ "ConsoleLogin": "Success" }),
                read_only: false,
                ..Call::new(
                    iam_user(account, name, None),
                    "signin.amazonaws.com",
                    "ConsoleLogin",
                    ip,
                )
            },
        );
        login_record["eventType"] = json!("AwsConsoleSignIn");
        login_record["additionalEventData"] = json!({
            "LoginTo": "https://console.aws.amazon.com/console/home",
            "MobileVersion": "No",
            "MFAUsed": "No",
        });
        let key = derived_id("ASIA", &[account, name, "console"]);
        let mut identity = iam_user(account, name, Some(&key));
        identity["sessionContext"] = json!({
            "attributes": { "creationDate": format_time(login), "mfaAuthenticated": "false" },
        });
        let shell = self.add(
            target,
            login + TimeDelta::minutes(5),
            Call {
                agent: BROWSER_AGENT,
                params: json!({}),
                read_only: false,
                ..Call::new(
                    identity.clone(),
                    "cloudshell.amazonaws.com",
                    "CreateEnvironment",
                    ip,
                )
            },
        );
        shell["sessionCredentialFromConsole"] = json!("true");
        let launch = login + TimeDelta::minutes(20);
        let agent = format!("{CLI_AGENT} exec-env/CloudShell");
        self.add_at(
            target,
            launch,
            launch - TimeDelta::minutes(1),
            Call {
                agent: &agent,
                params: json!({
                    "instanceType": "p3.16xlarge",
                    "instancesSet": { "items": [{ "imageId": "ami-0123456789abcdef0", "minCount": 1, "maxCount": 10 }] },
                }),
                response: json!({ "reservationId": "r-0123456789abcdef0" }),
                read_only: false,
                ..Call::new(identity, "ec2.amazonaws.com", "RunInstances", ip)
            },
        );
        let origin = ("iam_user", format!("iam_user:{}", user_arn(account, name)));
        self.expect("console_takeover_mining", origin, login, launch);
    }

    // The privilege is modified exactly at a batch boundary.
    fn role_chain_escalation(&mut self, issuer: Partition, target: Partition, day: i64) {
        let (user, ip) = ("dave-chain", "198.51.100.230");
        let key = user_access_key(issuer.account, user);
        let issued = since() - TimeDelta::minutes(30);
        let caller = iam_user(issuer.account, user, Some(&key));
        let entry = self.assume_role(
            issuer,
            issued,
            caller,
            ip,
            (target.account, "chain-entry"),
            user,
        );
        let first = at(day, 13 * HOUR + 52 * MINUTE);
        let caller = assumed_role(target.account, "chain-entry", user, &entry, issued);
        let ops = self.assume_role(
            target,
            first,
            caller,
            ip,
            (target.account, "chain-ops"),
            user,
        );
        let second = first + TimeDelta::minutes(4);
        let caller = assumed_role(target.account, "chain-ops", user, &ops, first);
        let admin = self.assume_role(
            target,
            second,
            caller,
            ip,
            (target.account, "chain-admin"),
            user,
        );
        let modified = at(day, 14 * HOUR);
        let caller = assumed_role(target.account, "chain-admin", user, &admin, second);
        self.add(
            target,
            modified,
            Call {
                params: json!({
                    "roleName": "chain-admin",
                    "policyArn": "arn:aws:iam::aws:policy/AdministratorAccess",
                }),
                read_only: false,
                ..Call::new(caller, "iam.amazonaws.com", "AttachRolePolicy", ip)
            },
        );
        let origin = (
            "external_principal",
            format!("external:{}", user_arn(issuer.account, user)),
        );
        self.expect("role_chain_escalation", origin, first, modified);
    }

    // The probe and the first denial are exactly at the boundaries of their time bins.
    fn leaked_key_recon_persistence(&mut self, target: Partition, day: i64) {
        let (account, user) = (target.account, "carol-leaked");
        let key = user_access_key(account, user);
        let identity = iam_user(account, user, Some(&key));
        let home = "192.0.2.44";
        let baseline = baseline_since() + TimeDelta::days(day) + TimeDelta::hours(9);
        for (offset, source, name) in [
            (0, "s3.amazonaws.com", "ListBuckets"),
            (5, "sts.amazonaws.com", "GetCallerIdentity"),
        ] {
            let call = Call::new(identity.clone(), source, name, home);
            self.add(target, baseline + TimeDelta::minutes(offset), call);
        }
        let attacker = "203.0.113.77";
        let probe = at(day, 9 * HOUR);
        let call = Call {
            agent: ATTACKER_AGENT,
            ..Call::new(
                identity.clone(),
                "sts.amazonaws.com",
                "GetCallerIdentity",
                attacker,
            )
        };
        self.add(target, probe, call);
        let denials = [
            ("iam.amazonaws.com", "ListUsers", "AccessDenied"),
            ("iam.amazonaws.com", "ListRoles", "AccessDenied"),
            (
                "ec2.amazonaws.com",
                "DescribeInstances",
                "Client.UnauthorizedOperation",
            ),
            ("s3.amazonaws.com", "GetBucketPolicy", "AccessDenied"),
            ("iam.amazonaws.com", "ListUsers", "AccessDenied"),
        ];
        for (minute, (source, name, error)) in (10..).zip(denials) {
            let call = Call {
                agent: ATTACKER_AGENT,
                ..Call::new(identity.clone(), source, name, attacker)
            };
            let record = self.add(target, probe + TimeDelta::minutes(minute), call);
            record["errorCode"] = json!(error);
            record["errorMessage"] = json!("User is not authorized to perform this operation");
        }
        let persisted = at(day, 10 * HOUR);
        let call = Call {
            agent: ATTACKER_AGENT,
            params: json!({ "userName": user }),
            response: json!({
                "accessKey": {
                    "userName": user,
                    "accessKeyId": derived_id("AKIA", &[account, user, "persistence"]),
                    "status": "Active",
                    "createDate": format_time(persisted),
                },
            }),
            read_only: false,
            ..Call::new(identity, "iam.amazonaws.com", "CreateAccessKey", attacker)
        };
        self.add(target, persisted, call);
        let origin = ("iam_user", format!("iam_user:{}", user_arn(account, user)));
        self.expect("leaked_key_recon_persistence", origin, probe, persisted);
    }

    // The deletes start exactly at a batch boundary, which is also the boundary of the
    // five-minute bin of s3_delete_burst, and only reach its threshold together.
    fn cross_account_s3_destruction(&mut self, issuer: Partition, target: Partition, day: i64) {
        let (user, ip) = ("bob-vendor", "203.0.113.99");
        let key = user_access_key(issuer.account, user);
        let assumed = at(day, 10 * HOUR + 30 * MINUTE);
        let caller = iam_user(issuer.account, user, Some(&key));
        let role = "vendor-access";
        let session = self.assume_role(issuer, assumed, caller, ip, (target.account, role), user);
        let identity = assumed_role(target.account, role, user, &session, assumed);
        let call = Call {
            params: json!({ "host": ["s3.amazonaws.com"] }),
            ..Call::new(identity.clone(), "s3.amazonaws.com", "ListBuckets", ip)
        };
        self.add(target, assumed + TimeDelta::minutes(2), call);
        let bucket = format!("customer-data-{}", target.account);
        let objects = [
            ("GetObject", 10 * HOUR + 40 * MINUTE),
            ("GetObject", 10 * HOUR + 41 * MINUTE),
            ("DeleteObject", 11 * HOUR),
            ("DeleteObject", 11 * HOUR + 90),
            ("DeleteObject", 11 * HOUR + 3 * MINUTE),
        ];
        for (index, (name, seconds)) in objects.into_iter().enumerate() {
            let object = format!("exports/{}.csv", index % 2);
            let call = Call {
                params: json!({
                    "bucketName": bucket,
                    "Host": format!("{bucket}.s3.amazonaws.com"),
                    "key": object,
                }),
                read_only: name == "GetObject",
                resources: Some(json!([
                    { "type": "AWS::S3::Object", "ARN": format!("arn:aws:s3:::{bucket}/{object}") },
                    { "type": "AWS::S3::Bucket", "ARN": format!("arn:aws:s3:::{bucket}") },
                ])),
                data_event: true,
                ..Call::new(identity.clone(), "s3.amazonaws.com", name, ip)
            };
            self.add(target, at(day, seconds), call);
        }
        let origin = (
            "external_principal",
            format!("external:{}", user_arn(issuer.account, user)),
        );
        self.expect(
            "cross_account_s3_destruction",
            origin,
            assumed,
            at(day, 11 * HOUR + 3 * MINUTE),
        );
    }

    // Harmless calls of a background user, older than the file time by more than
    // allowed_lateness, so that only late_events records them.
    fn late(&mut self, target: Partition, file_time: DateTime<Utc>, minutes_before: &[i64]) {
        let user = user_name(0);
        let ip = user_ip(target.account, 0);
        let key = user_access_key(target.account, &user);
        for minutes in minutes_before {
            let identity = iam_user(target.account, &user, Some(&key));
            let call = Call {
                params: json!({ "instancesSet": {}, "filterSet": {} }),
                ..Call::new(identity, "ec2.amazonaws.com", "DescribeInstances", &ip)
            };
            self.add_at(
                target,
                file_time - TimeDelta::minutes(*minutes),
                file_time,
                call,
            );
        }
    }

    fn broken(&mut self, target: Partition, time: DateTime<Utc>) {
        let contents = [
            b"not a gzip stream\n".to_vec(),
            gzip(br#"{"Records": [{"eventVersion": "1.09", "eventTime": "#),
            gzip(br#"{"Records": []}"#),
        ];
        for (minutes, content) in (2..).step_by(5).zip(contents) {
            self.raw_files.push(RawFile {
                partition: target,
                file_time: time + TimeDelta::minutes(minutes),
                content,
            });
        }
    }
}
