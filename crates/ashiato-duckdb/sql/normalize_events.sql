INSERT INTO events
WITH parsed AS (
    SELECT
        json_extract_string(record, [
            '$.eventID',
            '$.eventTime',
            '$.eventSource',
            '$.eventName',
            '$.eventCategory',
            '$.readOnly',
            '$.awsRegion',
            '$.recipientAccountId',
            '$.sourceIPAddress',
            '$.userAgent',
            '$.errorCode',
            '$.userIdentity.type',
            '$.userIdentity.arn',
            '$.userIdentity.principalId',
            '$.userIdentity.accountId',
            '$.userIdentity.accessKeyId',
            '$.userIdentity.invokedBy',
            '$.userIdentity.sessionContext.sessionIssuer.arn',
            '$.userIdentity.sessionContext.attributes.creationDate',
            '$.userIdentity.sessionContext.attributes.mfaAuthenticated',
            '$.sessionCredentialFromConsole'
        ]) AS strings,
        json_extract(record, [
            '$.requestParameters',
            '$.responseElements',
            '$.additionalEventData',
            '$.resources'
        ]) AS objects
    FROM staged_records
),
extracted AS (
    SELECT
        strings[1] AS event_id,
        CAST(strings[2] AS TIMESTAMP) AS event_time,
        strings[3] AS event_source,
        strings[4] AS event_name,
        strings[5] AS event_category,
        TRY_CAST(strings[6] AS BOOLEAN) AS read_only,
        strings[7] AS aws_region,
        strings[8] AS recipient_account_id,
        strings[9] AS source_ip,
        strings[10] AS user_agent,
        strings[11] AS error_code,
        strings[12] AS identity_type,
        strings[13] AS principal_arn,
        strings[14] AS principal_id,
        strings[15] AS principal_account_id,
        strings[16] AS access_key_id,
        strings[17] AS invoked_by,
        strings[18] AS session_issuer_arn,
        CAST(strings[19] AS TIMESTAMP) AS session_created_at,
        TRY_CAST(strings[20] AS BOOLEAN) AS mfa_authenticated,
        TRY_CAST(strings[21] AS BOOLEAN) AS credential_from_console,
        NULLIF(objects[1], 'null') AS request_parameters,
        NULLIF(objects[2], 'null') AS response_elements,
        NULLIF(objects[3], 'null') AS additional_event_data,
        NULLIF(objects[4], 'null') AS resources
    FROM parsed
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
