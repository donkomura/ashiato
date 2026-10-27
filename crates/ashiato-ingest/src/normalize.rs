use std::io::Read;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use ashiato_pipeline::events_schema;
use chrono::{DateTime, NaiveDateTime};
use flate2::read::MultiGzDecoder;
use serde::Deserialize;
use serde_json::Value;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

const ROW_OVERHEAD_BYTES: usize = 8 * 29;

#[derive(Debug, thiserror::Error)]
pub enum ContentError {
    #[error("failed to decompress the file")]
    Gzip(#[source] std::io::Error),
    #[error("the file is not a CloudTrail log file of the form {{\"Records\": [...]}}")]
    Json(#[source] serde_json::Error),
    #[error("record {index} is not a valid CloudTrail record")]
    Record {
        index: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("record {index} has no eventID")]
    MissingEventId { index: usize },
    #[error("record {index} has an invalid {field}: {value}")]
    InvalidTime {
        index: usize,
        field: &'static str,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDigest {
    pub size: u64,
    pub sha256: String,
}

impl FileDigest {
    pub fn of(content: &[u8]) -> Self {
        Self {
            size: content.len() as u64,
            sha256: Sha256::digest(content)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct EventRow {
    pub event_id: String,
    pub event_time: i64,
    pub event_source: Option<String>,
    pub event_name: Option<String>,
    pub event_category: Option<String>,
    pub read_only: Option<bool>,
    pub aws_region: Option<String>,
    pub recipient_account_id: Option<String>,
    pub source_ip: Option<String>,
    pub user_agent: Option<String>,
    pub error_code: Option<String>,
    pub identity_type: Option<String>,
    pub principal_arn: Option<String>,
    pub principal_id: Option<String>,
    pub principal_account_id: Option<String>,
    pub access_key_id: Option<String>,
    pub invoked_by: Option<String>,
    pub session_issuer_arn: Option<String>,
    pub session_name: Option<String>,
    pub session_created_at: Option<i64>,
    pub mfa_authenticated: Option<bool>,
    pub credential_from_console: Option<bool>,
    pub principal_key: Option<String>,
    pub request_parameters: Option<String>,
    pub response_elements: Option<String>,
    pub additional_event_data: Option<String>,
    pub resources: Option<String>,
}

#[derive(Deserialize)]
struct LogFile<'a> {
    #[serde(rename = "Records", borrow)]
    records: Vec<&'a RawValue>,
}

#[derive(Deserialize)]
struct RawRecord<'a> {
    #[serde(rename = "eventID", default)]
    event_id: Option<Value>,
    #[serde(rename = "eventTime", default)]
    event_time: Option<Value>,
    #[serde(rename = "eventSource", default)]
    event_source: Option<Value>,
    #[serde(rename = "eventName", default)]
    event_name: Option<Value>,
    #[serde(rename = "eventCategory", default)]
    event_category: Option<Value>,
    #[serde(rename = "readOnly", default)]
    read_only: Option<Value>,
    #[serde(rename = "awsRegion", default)]
    aws_region: Option<Value>,
    #[serde(rename = "recipientAccountId", default)]
    recipient_account_id: Option<Value>,
    #[serde(rename = "sourceIPAddress", default)]
    source_ip: Option<Value>,
    #[serde(rename = "userAgent", default)]
    user_agent: Option<Value>,
    #[serde(rename = "errorCode", default)]
    error_code: Option<Value>,
    #[serde(rename = "userIdentity", default)]
    user_identity: Option<Value>,
    #[serde(rename = "sessionCredentialFromConsole", default)]
    credential_from_console: Option<Value>,
    #[serde(rename = "requestParameters", default, borrow)]
    request_parameters: Option<&'a RawValue>,
    #[serde(rename = "responseElements", default, borrow)]
    response_elements: Option<&'a RawValue>,
    #[serde(rename = "additionalEventData", default, borrow)]
    additional_event_data: Option<&'a RawValue>,
    #[serde(rename = "resources", default, borrow)]
    resources: Option<&'a RawValue>,
}

pub fn decompress(content: &[u8]) -> Result<String, ContentError> {
    let mut text = String::new();
    MultiGzDecoder::new(content)
        .read_to_string(&mut text)
        .map_err(ContentError::Gzip)?;
    Ok(text)
}

pub fn normalize_records(text: &str) -> Result<Vec<EventRow>, ContentError> {
    let file: LogFile<'_> = serde_json::from_str(text).map_err(ContentError::Json)?;
    file.records
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let record: RawRecord<'_> = serde_json::from_str(raw.get())
                .map_err(|source| ContentError::Record { index, source })?;
            normalize_record(index, record)
        })
        .collect()
}

fn normalize_record(index: usize, record: RawRecord<'_>) -> Result<EventRow, ContentError> {
    let identity = record.user_identity.as_ref();
    let at = |path: &[&str]| identity.and_then(|value| path_text(value, path));
    let identity_type = at(&["type"]);
    let principal_arn = at(&["arn"]);
    let principal_id = at(&["principalId"]);
    let principal_account_id = at(&["accountId"]);
    let invoked_by = at(&["invokedBy"]);
    let session_issuer_arn = at(&["sessionContext", "sessionIssuer", "arn"]);
    let session_created_at = at(&["sessionContext", "attributes", "creationDate"])
        .map(|value| {
            parse_time(
                index,
                "userIdentity.sessionContext.attributes.creationDate",
                value,
            )
        })
        .transpose()?;
    let mfa_authenticated = at(&["sessionContext", "attributes", "mfaAuthenticated"])
        .and_then(|value| parse_bool(&value));
    let event_id = text(record.event_id.as_ref()).ok_or(ContentError::MissingEventId { index })?;
    let event_time = text(record.event_time.as_ref())
        .ok_or_else(|| ContentError::InvalidTime {
            index,
            field: "eventTime",
            value: "null".to_owned(),
        })
        .and_then(|value| parse_time(index, "eventTime", value))?;
    let session_name = match identity_type.as_deref() {
        Some("AssumedRole") => principal_arn
            .as_deref()
            .map(|arn| arn.split('/').nth(2).unwrap_or_default().to_owned()),
        _ => None,
    };
    let principal_key = principal_key(
        identity_type.as_deref(),
        principal_arn.as_deref(),
        principal_id.as_deref(),
        principal_account_id.as_deref(),
        invoked_by.as_deref(),
        session_issuer_arn.as_deref(),
    );
    Ok(EventRow {
        event_id,
        event_time,
        event_source: text(record.event_source.as_ref()),
        event_name: text(record.event_name.as_ref()),
        event_category: text(record.event_category.as_ref()),
        read_only: text(record.read_only.as_ref()).and_then(|value| parse_bool(&value)),
        aws_region: text(record.aws_region.as_ref()),
        recipient_account_id: text(record.recipient_account_id.as_ref()),
        source_ip: text(record.source_ip.as_ref()),
        user_agent: text(record.user_agent.as_ref()),
        error_code: text(record.error_code.as_ref()),
        identity_type,
        principal_arn,
        principal_id,
        principal_account_id,
        access_key_id: at(&["accessKeyId"]),
        invoked_by,
        session_issuer_arn,
        session_name,
        session_created_at,
        mfa_authenticated,
        credential_from_console: text(record.credential_from_console.as_ref())
            .and_then(|value| parse_bool(&value)),
        principal_key,
        request_parameters: json(record.request_parameters),
        response_elements: json(record.response_elements),
        additional_event_data: json(record.additional_event_data),
        resources: json(record.resources),
    })
}

fn principal_key(
    identity_type: Option<&str>,
    principal_arn: Option<&str>,
    principal_id: Option<&str>,
    principal_account_id: Option<&str>,
    invoked_by: Option<&str>,
    session_issuer_arn: Option<&str>,
) -> Option<String> {
    match identity_type {
        Some("IAMUser") => principal_arn.map(str::to_owned),
        Some("Root") => principal_account_id.map(|account| format!("arn:aws:iam::{account}:root")),
        Some("AssumedRole" | "FederatedUser") => session_issuer_arn.map(str::to_owned),
        Some("AWSService") => invoked_by.map(|service| format!("service:{service}")),
        Some("AWSAccount") => principal_account_id.map(|account| format!("account:{account}")),
        _ => principal_arn
            .filter(|arn| !arn.is_empty())
            .map(str::to_owned)
            .or_else(|| principal_id.map(|id| format!("unknown:{id}"))),
    }
}

fn path_text(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.as_object()?.get(*key)?;
    }
    text(Some(current))
}

// Mirrors json_extract_string, which normalize_events.sql uses for the same columns.
fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn json(raw: Option<&RawValue>) -> Option<String> {
    raw.map(|raw| raw.get().to_owned())
}

fn parse_time(index: usize, field: &'static str, value: String) -> Result<i64, ContentError> {
    let parsed = DateTime::parse_from_rfc3339(&value)
        .map(|time| time.naive_utc())
        .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f"));
    parsed
        .map(|time| time.and_utc().timestamp_micros())
        .map_err(|_| ContentError::InvalidTime {
            index,
            field,
            value,
        })
}

// Mirrors TRY_CAST(... AS BOOLEAN) in normalize_events.sql.
fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "y" | "yes" => Some(true),
        "false" | "f" | "0" | "n" | "no" => Some(false),
        _ => None,
    }
}

pub fn estimated_size(row: &EventRow) -> usize {
    let strings = [
        Some(&row.event_id),
        row.event_source.as_ref(),
        row.event_name.as_ref(),
        row.event_category.as_ref(),
        row.aws_region.as_ref(),
        row.recipient_account_id.as_ref(),
        row.source_ip.as_ref(),
        row.user_agent.as_ref(),
        row.error_code.as_ref(),
        row.identity_type.as_ref(),
        row.principal_arn.as_ref(),
        row.principal_id.as_ref(),
        row.principal_account_id.as_ref(),
        row.access_key_id.as_ref(),
        row.invoked_by.as_ref(),
        row.session_issuer_arn.as_ref(),
        row.session_name.as_ref(),
        row.principal_key.as_ref(),
        row.request_parameters.as_ref(),
        row.response_elements.as_ref(),
        row.additional_event_data.as_ref(),
        row.resources.as_ref(),
    ];
    ROW_OVERHEAD_BYTES
        + strings
            .iter()
            .flatten()
            .map(|text| text.len())
            .sum::<usize>()
}

pub fn record_batch(rows: &[EventRow], file_id: i64, first_record_index: u64) -> RecordBatch {
    let strings = |field: fn(&EventRow) -> Option<&str>| -> ArrayRef {
        let mut builder = StringBuilder::new();
        for row in rows {
            builder.append_option(field(row));
        }
        Arc::new(builder.finish())
    };
    let booleans = |field: fn(&EventRow) -> Option<bool>| -> ArrayRef {
        let mut builder = BooleanBuilder::new();
        for row in rows {
            builder.append_option(field(row));
        }
        Arc::new(builder.finish())
    };
    let timestamps = |field: fn(&EventRow) -> Option<i64>| -> ArrayRef {
        let mut builder = TimestampMicrosecondBuilder::new();
        for row in rows {
            builder.append_option(field(row));
        }
        Arc::new(builder.finish())
    };
    let mut file_ids = Int64Builder::new();
    let mut record_indexes = Int64Builder::new();
    for offset in 0..rows.len() {
        file_ids.append_value(file_id);
        record_indexes.append_value(
            i64::try_from(first_record_index + offset as u64)
                .expect("a log file holds fewer than 2^63 records"),
        );
    }
    let columns: Vec<ArrayRef> = vec![
        strings(|row| Some(&row.event_id)),
        timestamps(|row| Some(row.event_time)),
        strings(|row| row.event_source.as_deref()),
        strings(|row| row.event_name.as_deref()),
        strings(|row| row.event_category.as_deref()),
        booleans(|row| row.read_only),
        strings(|row| row.aws_region.as_deref()),
        strings(|row| row.recipient_account_id.as_deref()),
        strings(|row| row.source_ip.as_deref()),
        strings(|row| row.user_agent.as_deref()),
        strings(|row| row.error_code.as_deref()),
        strings(|row| row.identity_type.as_deref()),
        strings(|row| row.principal_arn.as_deref()),
        strings(|row| row.principal_id.as_deref()),
        strings(|row| row.principal_account_id.as_deref()),
        strings(|row| row.access_key_id.as_deref()),
        strings(|row| row.invoked_by.as_deref()),
        strings(|row| row.session_issuer_arn.as_deref()),
        strings(|row| row.session_name.as_deref()),
        timestamps(|row| row.session_created_at),
        booleans(|row| row.mfa_authenticated),
        booleans(|row| row.credential_from_console),
        strings(|row| row.principal_key.as_deref()),
        strings(|row| row.request_parameters.as_deref()),
        strings(|row| row.response_elements.as_deref()),
        strings(|row| row.additional_event_data.as_deref()),
        strings(|row| row.resources.as_deref()),
        Arc::new(file_ids.finish()),
        Arc::new(record_indexes.finish()),
    ];
    RecordBatch::try_new(events_schema(), columns)
        .expect("the columns are built in the order and types of events_schema")
}
