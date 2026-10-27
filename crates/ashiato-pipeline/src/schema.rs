use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

pub const EVENT_COLUMNS: [&str; 29] = [
    "event_id",
    "event_time",
    "event_source",
    "event_name",
    "event_category",
    "read_only",
    "aws_region",
    "recipient_account_id",
    "source_ip",
    "user_agent",
    "error_code",
    "identity_type",
    "principal_arn",
    "principal_id",
    "principal_account_id",
    "access_key_id",
    "invoked_by",
    "session_issuer_arn",
    "session_name",
    "session_created_at",
    "mfa_authenticated",
    "credential_from_console",
    "principal_key",
    "request_parameters",
    "response_elements",
    "additional_event_data",
    "resources",
    "file_id",
    "record_index",
];

const REQUIRED_COLUMNS: [&str; 4] = ["event_id", "event_time", "file_id", "record_index"];

static EVENTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let fields: Vec<Field> = EVENT_COLUMNS
        .iter()
        .map(|name| Field::new(*name, column_type(name), !REQUIRED_COLUMNS.contains(name)))
        .collect();
    Arc::new(Schema::new(fields))
});

pub fn events_schema() -> SchemaRef {
    Arc::clone(&EVENTS_SCHEMA)
}

fn column_type(name: &str) -> DataType {
    match name {
        "event_time" | "session_created_at" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "read_only" | "mfa_authenticated" | "credential_from_console" => DataType::Boolean,
        "file_id" | "record_index" => DataType::Int64,
        _ => DataType::Utf8,
    }
}
