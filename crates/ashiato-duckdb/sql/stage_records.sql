CREATE OR REPLACE TEMP TABLE staged_records AS
WITH records AS (
    SELECT
        json_extract_string(record, '$.eventID') AS event_id,
        record
    FROM (
        SELECT unnest(Records) AS record
        FROM read_json(?, columns = {'Records': 'JSON[]'}, compression = 'gzip')
    )
)
SELECT record
FROM records
WHERE NOT EXISTS (
    SELECT 1 FROM events_raw WHERE events_raw.event_id = records.event_id
)
QUALIFY row_number() OVER (PARTITION BY event_id) = 1
