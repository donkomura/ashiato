use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::rng::{SplitMix64, hash};

pub(crate) const CLI_AGENT: &str =
    "aws-cli/2.15.30 Python/3.11.8 Linux/6.5.0 exe/x86_64.ubuntu.22 prompt/off";
pub(crate) const SDK_AGENT: &str = "Boto3/1.34.69 md/Botocore#1.34.69 ua/2.0 os/linux#6.1.0 \
     md/arch#x86_64 lang/python#3.11.8 cfg/retry-mode#legacy Botocore/1.34.69";

pub(crate) struct Call<'a> {
    pub(crate) identity: Value,
    pub(crate) source: &'a str,
    pub(crate) name: &'a str,
    pub(crate) ip: &'a str,
    pub(crate) agent: &'a str,
    pub(crate) params: Value,
    pub(crate) response: Value,
    pub(crate) read_only: bool,
    pub(crate) resources: Option<Value>,
    pub(crate) additional: Option<Value>,
    pub(crate) data_event: bool,
}

impl<'a> Call<'a> {
    pub(crate) fn new(identity: Value, source: &'a str, name: &'a str, ip: &'a str) -> Self {
        Self {
            identity,
            source,
            name,
            ip,
            agent: CLI_AGENT,
            params: Value::Null,
            response: Value::Null,
            read_only: true,
            resources: None,
            additional: None,
            data_event: false,
        }
    }

    pub(crate) fn record(
        self,
        rng: &mut SplitMix64,
        account: &str,
        region: &str,
        time: DateTime<Utc>,
    ) -> Value {
        let host = format!(
            "{}.{region}.amazonaws.com",
            self.source.trim_end_matches(".amazonaws.com")
        );
        let mut record = json!({
            "eventVersion": "1.09",
            "userIdentity": self.identity,
            "eventTime": format_time(time),
            "eventSource": self.source,
            "eventName": self.name,
            "awsRegion": region,
            "sourceIPAddress": self.ip,
            "userAgent": self.agent,
            "requestParameters": self.params,
            "responseElements": self.response,
            "requestID": rng.uuid(),
            "eventID": rng.uuid(),
            "readOnly": self.read_only,
            "eventType": "AwsApiCall",
            "managementEvent": !self.data_event,
            "recipientAccountId": account,
            "eventCategory": if self.data_event { "Data" } else { "Management" },
            "tlsDetails": {
                "tlsVersion": "TLSv1.3",
                "cipherSuite": "TLS_AES_128_GCM_SHA256",
                "clientProvidedHostHeader": host,
            },
        });
        if let Some(resources) = self.resources {
            record["resources"] = resources;
        }
        if let Some(additional) = self.additional {
            record["additionalEventData"] = additional;
        }
        record
    }
}

pub(crate) fn format_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

pub(crate) fn derived_id(prefix: &str, parts: &[&str]) -> String {
    format!("{prefix}{:016X}", hash(&parts.join("/")))
}

pub(crate) fn user_arn(account: &str, name: &str) -> String {
    format!("arn:aws:iam::{account}:user/{name}")
}

pub(crate) fn role_arn(account: &str, role: &str) -> String {
    format!("arn:aws:iam::{account}:role/{role}")
}

pub(crate) fn user_access_key(account: &str, name: &str) -> String {
    derived_id("AKIA", &[account, name])
}

pub(crate) fn iam_user(account: &str, name: &str, access_key: Option<&str>) -> Value {
    json!({
        "type": "IAMUser",
        "principalId": derived_id("AIDA", &[account, name]),
        "arn": user_arn(account, name),
        "accountId": account,
        "accessKeyId": access_key,
        "userName": name,
    })
}

pub(crate) fn assumed_role(
    account: &str,
    role: &str,
    session: &str,
    access_key: &str,
    created: DateTime<Utc>,
) -> Value {
    let role_id = derived_id("AROA", &[account, role]);
    json!({
        "type": "AssumedRole",
        "principalId": format!("{role_id}:{session}"),
        "arn": format!("arn:aws:sts::{account}:assumed-role/{role}/{session}"),
        "accountId": account,
        "accessKeyId": access_key,
        "sessionContext": {
            "sessionIssuer": {
                "type": "Role",
                "principalId": role_id,
                "arn": role_arn(account, role),
                "accountId": account,
                "userName": role,
            },
            "attributes": {
                "creationDate": format_time(created),
                "mfaAuthenticated": "false",
            },
        },
    })
}
