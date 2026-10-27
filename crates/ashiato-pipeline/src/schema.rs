use arrow_schema::SchemaRef;

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

pub fn events_schema() -> SchemaRef {
    todo!()
}
