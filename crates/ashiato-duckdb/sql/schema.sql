CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR,
    event_time TIMESTAMP,
    event_source VARCHAR,
    event_name VARCHAR,
    event_category VARCHAR,
    read_only BOOLEAN,
    aws_region VARCHAR,
    recipient_account_id VARCHAR,
    source_ip VARCHAR,
    user_agent VARCHAR,
    error_code VARCHAR,
    identity_type VARCHAR,
    principal_arn VARCHAR,
    principal_id VARCHAR,
    principal_account_id VARCHAR,
    access_key_id VARCHAR,
    invoked_by VARCHAR,
    session_issuer_arn VARCHAR,
    session_name VARCHAR,
    session_created_at TIMESTAMP,
    mfa_authenticated BOOLEAN,
    credential_from_console BOOLEAN,
    principal_key VARCHAR,
    request_parameters JSON,
    response_elements JSON,
    additional_event_data JSON,
    resources JSON
);

CREATE TABLE IF NOT EXISTS events_raw (
    event_id VARCHAR,
    raw JSON
);
