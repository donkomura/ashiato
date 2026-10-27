WITH issued AS (
    SELECT
        json_extract_string(response_elements, '$.credentials.accessKeyId') AS child_access_key_id,
        access_key_id,
        principal_key,
        principal_arn,
        identity_type,
        principal_account_id,
        invoked_by,
        json_extract_string(request_parameters, '$.roleArn') AS role_arn,
        event_id,
        event_time,
        CASE event_name
            WHEN 'AssumeRoleWithSAML' THEN json_extract_string(request_parameters, '$.principalArn')
            WHEN 'AssumeRoleWithWebIdentity' THEN json_extract_string(response_elements, '$.provider')
        END AS federation_provider,
        CASE event_name
            WHEN 'AssumeRoleWithSAML' THEN json_extract_string(response_elements, '$.subject')
            WHEN 'AssumeRoleWithWebIdentity' THEN json_extract_string(response_elements, '$.subjectFromWebIdentityToken')
        END AS federation_subject
    FROM events
    WHERE event_source = 'sts.amazonaws.com'
      AND event_name IN (
          'AssumeRole',
          'AssumeRoleWithSAML',
          'AssumeRoleWithWebIdentity',
          'GetSessionToken',
          'GetFederationToken'
      )
      AND error_code IS NULL
      AND event_time >= CAST(? AS TIMESTAMP)
      AND event_time < CAST(? AS TIMESTAMP)
)
SELECT
    child_access_key_id,
    access_key_id,
    principal_key,
    principal_arn,
    identity_type,
    principal_account_id,
    invoked_by,
    role_arn,
    event_id,
    -- Written with IS DISTINCT FROM instead of coalesce(split_part(...) <> ..., false), which
    -- gives the same result. DuckDB 1.2 fails to bind the latter with "BindBuiltinScalarFunction
    -- did not return a BoundFunctionExpression" when request_parameters is NULL in every row,
    -- as in a batch of the pipeline that holds only ConsoleLogin events.
    role_arn IS NOT NULL
        AND principal_account_id IS NOT NULL
        AND split_part(role_arn, ':', 5) IS DISTINCT FROM principal_account_id AS cross_account,
    federation_provider,
    federation_subject
FROM issued
WHERE child_access_key_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY child_access_key_id ORDER BY event_time, event_id) = 1
ORDER BY event_time, event_id
