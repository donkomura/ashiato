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
