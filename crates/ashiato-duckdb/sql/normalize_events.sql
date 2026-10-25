INSERT INTO events
WITH extracted AS (
    SELECT
        json_extract_string(record, '$.eventID') AS event_id,
        CAST(json_extract_string(record, '$.eventTime') AS TIMESTAMP) AS event_time,
        json_extract_string(record, '$.eventSource') AS event_source,
        json_extract_string(record, '$.eventName') AS event_name,
        json_extract_string(record, '$.eventCategory') AS event_category,
        TRY_CAST(json_extract_string(record, '$.readOnly') AS BOOLEAN) AS read_only,
        json_extract_string(record, '$.awsRegion') AS aws_region,
        json_extract_string(record, '$.recipientAccountId') AS recipient_account_id,
        json_extract_string(record, '$.sourceIPAddress') AS source_ip,
        json_extract_string(record, '$.userAgent') AS user_agent,
        json_extract_string(record, '$.errorCode') AS error_code,
        json_extract_string(record, '$.userIdentity.type') AS identity_type,
        json_extract_string(record, '$.userIdentity.arn') AS principal_arn,
        json_extract_string(record, '$.userIdentity.principalId') AS principal_id,
        json_extract_string(record, '$.userIdentity.accountId') AS principal_account_id,
        json_extract_string(record, '$.userIdentity.accessKeyId') AS access_key_id,
        json_extract_string(record, '$.userIdentity.invokedBy') AS invoked_by,
        json_extract_string(record, '$.userIdentity.sessionContext.sessionIssuer.arn') AS session_issuer_arn,
        CAST(json_extract_string(record, '$.userIdentity.sessionContext.attributes.creationDate') AS TIMESTAMP) AS session_created_at,
        TRY_CAST(json_extract_string(record, '$.userIdentity.sessionContext.attributes.mfaAuthenticated') AS BOOLEAN) AS mfa_authenticated,
        TRY_CAST(json_extract_string(record, '$.sessionCredentialFromConsole') AS BOOLEAN) AS credential_from_console,
        NULLIF(json_extract(record, '$.requestParameters'), 'null') AS request_parameters,
        NULLIF(json_extract(record, '$.responseElements'), 'null') AS response_elements,
        NULLIF(json_extract(record, '$.additionalEventData'), 'null') AS additional_event_data,
        NULLIF(json_extract(record, '$.resources'), 'null') AS resources
    FROM staged_records
)
SELECT
    event_id,
    event_time,
    event_source,
    event_name,
    event_category,
    read_only,
    aws_region,
    recipient_account_id,
    source_ip,
    user_agent,
    error_code,
    identity_type,
    principal_arn,
    principal_id,
    principal_account_id,
    access_key_id,
    invoked_by,
    session_issuer_arn,
    CASE
        WHEN identity_type = 'AssumedRole' THEN split_part(principal_arn, '/', 3)
    END AS session_name,
    session_created_at,
    mfa_authenticated,
    credential_from_console,
    CASE identity_type
        WHEN 'IAMUser' THEN principal_arn
        WHEN 'Root' THEN 'arn:aws:iam::' || principal_account_id || ':root'
        WHEN 'AssumedRole' THEN session_issuer_arn
        WHEN 'FederatedUser' THEN session_issuer_arn
        WHEN 'AWSService' THEN 'service:' || invoked_by
        WHEN 'AWSAccount' THEN 'account:' || principal_account_id
        ELSE coalesce(NULLIF(principal_arn, ''), 'unknown:' || principal_id)
    END AS principal_key,
    request_parameters,
    response_elements,
    additional_event_data,
    resources
FROM extracted
