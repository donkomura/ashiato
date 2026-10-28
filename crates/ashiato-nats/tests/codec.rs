mod common;

use arrow_array::RecordBatch;
use ashiato_nats::{
    CodecError, FILE_SEQ_HEADER, KIND_HEADER, MESSAGE_ID_HEADER, RECORD_COUNT_HEADER,
    RECORD_OFFSET_HEADER, WATERMARK_HEADER, consumer_name, decode, encode, encode_within,
    events_subject, events_subjects, message_id, partition_of, stream_name, validate_run_id,
};
use ashiato_pipeline::{PartitionId, PartitionMessage, Position};
use async_nats::HeaderMap;
use common::{fixture, worker_messages};

const RUN_ID: &str = "case-2026-10-xx";

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).map(|value| value.as_str().to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn every_message_of_a_worker_decodes_back_to_itself() {
    let workspace = fixture();
    let messages = worker_messages(&workspace.manifest(), 1 << 20).await;
    let kinds: Vec<&str> = messages
        .iter()
        .map(|message| match message {
            PartitionMessage::FileChunk { .. } => "chunk",
            PartitionMessage::FileError { .. } => "error",
            PartitionMessage::EndOfPartition { .. } => "end",
        })
        .collect();
    assert!(kinds.contains(&"chunk") && kinds.contains(&"error") && kinds.contains(&"end"));

    for message in &messages {
        let encoded = encode(RUN_ID, message).unwrap();
        assert_eq!(encoded.subject, events_subject(RUN_ID, message.partition()));
        let decoded = decode(RUN_ID, &encoded.subject, &encoded.headers, &encoded.payload).unwrap();
        assert_eq!(&decoded, message);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_headers_hold_the_kind_the_position_the_watermark_and_the_message_id() {
    let workspace = fixture();
    let messages = worker_messages(&workspace.manifest(), 1 << 20).await;
    for message in &messages {
        let headers = encode(RUN_ID, message).unwrap().headers;
        let position = message.position();
        let partition = message.partition();
        let expected_id = format!(
            "{}/{}/{}/{}",
            partition.account, partition.region, position.file_seq, position.record_offset
        );
        assert_eq!(header(&headers, MESSAGE_ID_HEADER), Some(expected_id));
        assert_eq!(
            header(&headers, FILE_SEQ_HEADER),
            Some(position.file_seq.to_string())
        );
        assert_eq!(
            header(&headers, RECORD_OFFSET_HEADER),
            Some(position.record_offset.to_string())
        );
        match message {
            PartitionMessage::FileChunk {
                record_count,
                watermark,
                ..
            } => {
                assert_eq!(header(&headers, KIND_HEADER).as_deref(), Some("chunk"));
                assert_eq!(
                    header(&headers, RECORD_COUNT_HEADER),
                    Some(record_count.to_string())
                );
                let parsed = chrono::DateTime::parse_from_rfc3339(
                    &header(&headers, WATERMARK_HEADER).unwrap(),
                )
                .unwrap();
                assert_eq!(parsed, *watermark);
            }
            PartitionMessage::FileError { .. } => {
                assert_eq!(header(&headers, KIND_HEADER).as_deref(), Some("error"));
            }
            PartitionMessage::EndOfPartition { .. } => {
                assert_eq!(header(&headers, KIND_HEADER).as_deref(), Some("end"));
            }
        }
    }
}

#[test]
fn the_names_follow_the_run_id_and_the_partition() {
    let partition = PartitionId::new("111111111111", "ap-northeast-1");
    assert_eq!(stream_name(RUN_ID), "ASHIATO_case-2026-10-xx");
    assert_eq!(events_subjects(RUN_ID), "ashiato.case-2026-10-xx.events.>");
    assert_eq!(
        events_subject(RUN_ID, &partition),
        "ashiato.case-2026-10-xx.events.111111111111.ap-northeast-1"
    );
    assert_eq!(
        consumer_name(&partition),
        "analyzer-111111111111-ap-northeast-1"
    );
    assert_eq!(
        message_id(&partition, Position::new(3, 120)),
        "111111111111/ap-northeast-1/3/120"
    );
    assert_eq!(
        partition_of(RUN_ID, &events_subject(RUN_ID, &partition)).unwrap(),
        partition
    );
}

#[test]
fn a_subject_outside_the_partitions_of_the_run_is_an_error() {
    for subject in [
        "ashiato.other-run.events.111111111111.us-east-1",
        "ashiato.case-2026-10-xx.control.plan",
        "ashiato.case-2026-10-xx.events.111111111111",
        "ashiato.case-2026-10-xx.events.111111111111.us-east-1.extra",
    ] {
        assert!(
            matches!(
                partition_of(RUN_ID, subject),
                Err(CodecError::UnknownSubject(_))
            ),
            "{subject}"
        );
    }
}

#[test]
fn a_run_id_that_cannot_name_a_stream_or_a_subject_token_is_an_error() {
    for valid in ["case-2026-10-xx", "run_1", "A"] {
        assert!(validate_run_id(valid).is_ok(), "{valid}");
    }
    for invalid in ["", "a.b", "a b", "a*", "a>", "a/b", "日本"] {
        assert!(
            matches!(validate_run_id(invalid), Err(CodecError::InvalidRunId(_))),
            "{invalid:?}"
        );
    }
}

#[test]
fn a_message_without_its_kind_is_an_error() {
    let subject = events_subject(RUN_ID, &PartitionId::new("111111111111", "us-east-1"));
    let error = decode(RUN_ID, &subject, &HeaderMap::new(), &[]).unwrap_err();
    assert!(
        matches!(error, CodecError::MissingHeader(KIND_HEADER)),
        "{error:?}"
    );
}

fn largest_chunk(messages: &[PartitionMessage]) -> PartitionMessage {
    messages
        .iter()
        .filter(|message| matches!(message, PartitionMessage::FileChunk { .. }))
        .max_by_key(|message| match message {
            PartitionMessage::FileChunk { record_count, .. } => *record_count,
            _ => 0,
        })
        .unwrap()
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_larger_than_the_limit_is_split_into_chunks_whose_positions_continue() {
    let workspace = fixture();
    let chunk = largest_chunk(&worker_messages(&workspace.manifest(), 1 << 20).await);
    let PartitionMessage::FileChunk {
        position,
        record_count,
        events,
        ..
    } = &chunk
    else {
        unreachable!();
    };
    assert_eq!(*record_count, 3);
    let whole = encode(RUN_ID, &chunk).unwrap().size();
    let max = whole - 1;

    let parts = encode_within(RUN_ID, &chunk, max).unwrap();

    assert!(parts.len() > 1);
    let mut next = *position;
    let mut batches = Vec::new();
    for part in &parts {
        assert!(part.size() <= max, "{} > {max}", part.size());
        let PartitionMessage::FileChunk {
            position,
            record_count,
            events,
            ..
        } = decode(RUN_ID, &part.subject, &part.headers, &part.payload).unwrap()
        else {
            panic!("a part of a chunk is a chunk");
        };
        assert_eq!(position, next);
        next = Position::new(
            position.file_seq,
            position.record_offset + u64::from(record_count),
        );
        batches.push(events);
    }
    assert_eq!(next.record_offset, position.record_offset + 3);
    let joined: RecordBatch = arrow_select_concat(&batches);
    assert_eq!(&joined, events);
}

fn arrow_select_concat(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches[0].schema();
    let columns = (0..schema.fields().len())
        .map(|column| {
            let arrays: Vec<&dyn arrow_array::Array> = batches
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect();
            arrow_select::concat::concat(&arrays).unwrap()
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_within_the_limit_is_sent_as_it_is() {
    let workspace = fixture();
    for message in worker_messages(&workspace.manifest(), 1 << 20).await {
        let whole = encode(RUN_ID, &message).unwrap();
        let parts = encode_within(RUN_ID, &message, whole.size()).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].payload, whole.payload);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_record_larger_than_the_limit_is_an_error() {
    let workspace = fixture();
    let chunk = largest_chunk(&worker_messages(&workspace.manifest(), 1 << 20).await);
    let error = encode_within(RUN_ID, &chunk, 64).unwrap_err();
    assert!(
        matches!(error, CodecError::TooLarge { max: 64, .. }),
        "{error:?}"
    );
}
