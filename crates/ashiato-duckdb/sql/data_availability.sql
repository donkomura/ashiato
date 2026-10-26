SELECT
    count(*) AS events,
    coalesce(
        bool_or(event_source = 's3.amazonaws.com' AND event_category = 'Data'),
        false
    ) AS s3_data_events
FROM events
WHERE event_time >= CAST(? AS TIMESTAMP)
  AND event_time < CAST(? AS TIMESTAMP)
