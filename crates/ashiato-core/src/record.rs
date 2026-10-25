use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Map, Value, json};

const ACCOUNT_ID: &str = "111111111111";

static NEXT_EVENT_NUMBER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("a CloudTrail record must be a JSON object, but got {0}")]
    NotAnObject(Value),
}

pub fn complete_record(partial: Value) -> Result<Value, RecordError> {
    match partial {
        Value::Object(partial) => Ok(Value::Object(complete(partial))),
        other => Err(RecordError::NotAnObject(other)),
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecordBuilder {
    partial: Map<String, Value>,
}

impl RecordBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(mut self, path: &str, value: impl Into<Value>) -> Self {
        let mut keys = path.split('.').peekable();
        let mut object = &mut self.partial;
        while let Some(key) = keys.next() {
            if keys.peek().is_none() {
                object.insert(key.to_owned(), value.into());
                break;
            }
            let child = object
                .entry(key)
                .and_modify(|child| {
                    if !child.is_object() {
                        *child = Value::Object(Map::new());
                    }
                })
                .or_insert_with(|| Value::Object(Map::new()));
            object = child
                .as_object_mut()
                .expect("child was made an object above");
        }
        self
    }

    pub fn build(self) -> Value {
        Value::Object(complete(self.partial))
    }
}

fn complete(partial: Map<String, Value>) -> Map<String, Value> {
    let mut record = default_record();
    merge(&mut record, partial);
    record
        .entry("eventID")
        .or_insert_with(|| Value::String(next_event_id()));
    record
}

fn merge(base: &mut Map<String, Value>, overlay: Map<String, Value>) {
    for (key, value) in overlay {
        match (base.get_mut(&key), value) {
            (Some(Value::Object(base_child)), Value::Object(overlay_child)) => {
                merge(base_child, overlay_child);
            }
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

fn next_event_id() -> String {
    let number = NEXT_EVENT_NUMBER.fetch_add(1, Ordering::Relaxed);
    format!("00000000-0000-4000-8000-{number:012x}")
}

fn default_record() -> Map<String, Value> {
    let Value::Object(record) = json!({
        "eventVersion": "1.09",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": "AIDAEXAMPLEPRINCIPAL",
            "arn": format!("arn:aws:iam::{ACCOUNT_ID}:user/alice"),
            "accountId": ACCOUNT_ID,
            "accessKeyId": "AKIAEXAMPLEACCESSKEY",
            "userName": "alice",
        },
        "eventTime": "2026-10-01T00:00:00Z",
        "eventSource": "sts.amazonaws.com",
        "eventName": "GetCallerIdentity",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "198.51.100.1",
        "userAgent": "aws-cli/2.0.0",
        "requestParameters": null,
        "responseElements": null,
        "readOnly": true,
        "eventType": "AwsApiCall",
        "managementEvent": true,
        "recipientAccountId": ACCOUNT_ID,
        "eventCategory": "Management",
    }) else {
        unreachable!("the default record is written as a JSON object literal")
    };
    record
}
