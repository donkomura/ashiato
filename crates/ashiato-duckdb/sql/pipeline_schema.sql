CREATE TABLE IF NOT EXISTS run_meta (
    digest VARCHAR NOT NULL
);

-- A partition without files is kept as one row whose file columns are NULL. The partitions
-- themselves must be saved, because partition_index decides file_id (streaming-pipeline.md 5.4).
CREATE TABLE IF NOT EXISTS manifest (
    partition_index UINTEGER NOT NULL,
    account VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    file_seq UBIGINT,
    uri VARCHAR,
    version_id VARCHAR,
    etag VARCHAR,
    size UBIGINT,
    file_time TIMESTAMP
);

-- Events received but not yet in a finalized batch. The columns follow events_schema in
-- ashiato-pipeline; the JSON columns are kept as VARCHAR so that Arrow batches append as they are.
CREATE TABLE IF NOT EXISTS pending (
    event_id VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL,
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
    request_parameters VARCHAR,
    response_elements VARCHAR,
    additional_event_data VARCHAR,
    resources VARCHAR,
    file_id BIGINT NOT NULL,
    record_index BIGINT NOT NULL
);

-- Holds the events of one message while it is moved into pending within the commit transaction.
CREATE TABLE IF NOT EXISTS incoming (
    event_id VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL,
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
    request_parameters VARCHAR,
    response_elements VARCHAR,
    additional_event_data VARCHAR,
    resources VARCHAR,
    file_id BIGINT NOT NULL,
    record_index BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_files (
    partition_index UINTEGER NOT NULL,
    file_seq UBIGINT NOT NULL,
    file_id BIGINT NOT NULL,
    uri VARCHAR NOT NULL,
    version_id VARCHAR,
    etag VARCHAR,
    size UBIGINT NOT NULL,
    sha256 VARCHAR NOT NULL,
    file_time TIMESTAMP NOT NULL,
    record_count UBIGINT NOT NULL,
    error VARCHAR
);

CREATE TABLE IF NOT EXISTS partition_progress (
    partition_index UINTEGER PRIMARY KEY,
    file_seq UBIGINT NOT NULL,
    record_offset UBIGINT NOT NULL,
    watermark TIMESTAMP,
    ended BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS late_events (
    event_id VARCHAR NOT NULL,
    file_id BIGINT NOT NULL,
    record_index BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_start TIMESTAMP NOT NULL,
    batch_end TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS recent_event_ids (
    partition_index UINTEGER NOT NULL,
    event_id VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL
);

-- event_ids and attrs are JSON texts (a list of strings and an object of strings).
CREATE TABLE IF NOT EXISTS signals (
    rule_id VARCHAR NOT NULL,
    batch_start TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    event_ids VARCHAR NOT NULL,
    access_key_id VARCHAR,
    principal_key VARCHAR NOT NULL,
    source_ip VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    attrs VARCHAR NOT NULL
);

-- seq keeps the order of the lineage SQL within a batch, which is the order of event_time and
-- event_id. Edges for the same child access key are merged when they are read.
CREATE TABLE IF NOT EXISTS lineage_edges (
    batch_start TIMESTAMP NOT NULL,
    seq UBIGINT NOT NULL,
    child_access_key_id VARCHAR NOT NULL,
    parent_access_key_id VARCHAR,
    parent_principal_key VARCHAR NOT NULL,
    parent_principal_arn VARCHAR,
    parent_identity_type VARCHAR NOT NULL,
    parent_account_id VARCHAR,
    parent_invoked_by VARCHAR,
    role_arn VARCHAR,
    issued_by_event_id VARCHAR NOT NULL,
    cross_account BOOLEAN NOT NULL,
    federation_provider VARCHAR,
    federation_subject VARCHAR
);

-- Rules refer to these relations by name. They are filled from the baseline period (段階 17).
CREATE TABLE IF NOT EXISTS baseline (
    principal_key VARCHAR,
    dim VARCHAR,
    value VARCHAR
);

CREATE TABLE IF NOT EXISTS baseline_principals (
    principal_key VARCHAR
);
