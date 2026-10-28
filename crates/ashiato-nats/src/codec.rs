use std::io::Cursor;
use std::str::FromStr;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::ArrowError;
use ashiato_pipeline::{EventTime, IngestedFile, PartitionId, PartitionMessage, Position};
use async_nats::HeaderMap;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

pub const KIND_HEADER: &str = "Ashiato-Kind";
pub const FILE_SEQ_HEADER: &str = "Ashiato-File-Seq";
pub const RECORD_OFFSET_HEADER: &str = "Ashiato-Record-Offset";
pub const RECORD_COUNT_HEADER: &str = "Ashiato-Record-Count";
pub const WATERMARK_HEADER: &str = "Ashiato-Watermark";
pub const FILE_HEADER: &str = "Ashiato-File";
pub const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

const SUBJECT_PREFIX: &str = "ashiato";
// The status line and the blank line that end the headers of a NATS message, which count
// toward max_payload together with the payload.
const HEADER_FRAMING_BYTES: usize = "NATS/1.0\r\n\r\n".len();

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(
        "run_id {0:?} names the NATS stream and subjects, so it may hold only ASCII letters, \
         digits, '-' and '_'"
    )]
    InvalidRunId(String),
    #[error("subject {0:?} is not the subject of a partition of this run")]
    UnknownSubject(String),
    #[error("header {0} is missing")]
    MissingHeader(&'static str),
    #[error("header {name} has an invalid value {value:?}")]
    InvalidHeader { name: &'static str, value: String },
    #[error("failed to read or write the file of a message as JSON")]
    File(#[source] serde_json::Error),
    #[error("failed to read or write the events as Arrow IPC")]
    Ipc(#[source] arrow_schema::ArrowError),
    #[error("a message of a single record takes {size} bytes, more than max_message_size {max}")]
    TooLarge { size: usize, max: usize },
}

#[derive(Debug, Clone)]
pub struct EncodedMessage {
    pub subject: String,
    pub headers: HeaderMap,
    pub payload: Bytes,
}

impl EncodedMessage {
    pub fn size(&self) -> usize {
        let headers: usize = self
            .headers
            .iter()
            .flat_map(|(name, values)| {
                values.iter().map(move |value| {
                    AsRef::<str>::as_ref(name).len() + ": \r\n".len() + value.as_str().len()
                })
            })
            .sum();
        HEADER_FRAMING_BYTES + headers + self.payload.len()
    }
}

// The reason of a FileError travels with the file in one header, as 5.3 節 places them.
#[derive(Serialize, Deserialize)]
struct FileHeader {
    #[serde(flatten)]
    file: IngestedFile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

pub fn validate_run_id(run_id: &str) -> Result<(), CodecError> {
    let valid = !run_id.is_empty()
        && run_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(CodecError::InvalidRunId(run_id.to_owned()))
    }
}

pub fn stream_name(run_id: &str) -> String {
    format!("ASHIATO_{run_id}")
}

pub fn events_subjects(run_id: &str) -> String {
    format!("{}>", events_prefix(run_id))
}

pub fn events_subject(run_id: &str, partition: &PartitionId) -> String {
    format!(
        "{}{}.{}",
        events_prefix(run_id),
        partition.account,
        partition.region
    )
}

pub fn consumer_name(partition: &PartitionId) -> String {
    format!("analyzer-{}-{}", partition.account, partition.region)
}

pub fn message_id(partition: &PartitionId, position: Position) -> String {
    format!(
        "{partition}/{}/{}",
        position.file_seq, position.record_offset
    )
}

pub fn partition_of(run_id: &str, subject: &str) -> Result<PartitionId, CodecError> {
    let unknown = || CodecError::UnknownSubject(subject.to_owned());
    let rest = subject
        .strip_prefix(&events_prefix(run_id))
        .ok_or_else(unknown)?;
    match rest.split('.').collect::<Vec<_>>().as_slice() {
        [account, region] if !account.is_empty() && !region.is_empty() => {
            Ok(PartitionId::new(*account, *region))
        }
        _ => Err(unknown()),
    }
}

fn events_prefix(run_id: &str) -> String {
    format!("{SUBJECT_PREFIX}.{run_id}.events.")
}

pub fn encode(run_id: &str, message: &PartitionMessage) -> Result<EncodedMessage, CodecError> {
    let partition = message.partition();
    let position = message.position();
    let mut headers = HeaderMap::new();
    headers.insert(MESSAGE_ID_HEADER, message_id(partition, position));
    headers.insert(FILE_SEQ_HEADER, position.file_seq.to_string());
    headers.insert(RECORD_OFFSET_HEADER, position.record_offset.to_string());
    let payload = match message {
        PartitionMessage::FileChunk {
            record_count,
            file,
            events,
            watermark,
            ..
        } => {
            headers.insert(KIND_HEADER, "chunk");
            headers.insert(RECORD_COUNT_HEADER, record_count.to_string());
            headers.insert(WATERMARK_HEADER, watermark_value(*watermark));
            headers.insert(FILE_HEADER, file_value(file, None)?);
            write_events(events)?
        }
        PartitionMessage::FileError {
            file,
            reason,
            watermark,
            ..
        } => {
            headers.insert(KIND_HEADER, "error");
            headers.insert(WATERMARK_HEADER, watermark_value(*watermark));
            headers.insert(FILE_HEADER, file_value(file, Some(reason))?);
            Bytes::new()
        }
        PartitionMessage::EndOfPartition { .. } => {
            headers.insert(KIND_HEADER, "end");
            Bytes::new()
        }
    };
    Ok(EncodedMessage {
        subject: events_subject(run_id, partition),
        headers,
        payload,
    })
}

// A chunk that does not fit is split in two, which the analyzer accepts because it checks the
// continuity of the record positions rather than how the records are chunked (5.2 節).
pub fn encode_within(
    run_id: &str,
    message: &PartitionMessage,
    max_size: usize,
) -> Result<Vec<EncodedMessage>, CodecError> {
    let encoded = encode(run_id, message)?;
    if encoded.size() <= max_size {
        return Ok(vec![encoded]);
    }
    let PartitionMessage::FileChunk {
        partition,
        position,
        record_count,
        file,
        events,
        watermark,
    } = message
    else {
        return Err(too_large(&encoded, max_size));
    };
    if *record_count <= 1 {
        return Err(too_large(&encoded, max_size));
    }
    let first = *record_count / 2;
    let part = |offset: u32, count: u32| PartitionMessage::FileChunk {
        partition: partition.clone(),
        position: Position::new(
            position.file_seq,
            position.record_offset + u64::from(offset),
        ),
        record_count: count,
        file: file.clone(),
        events: events.slice(offset as usize, count as usize),
        watermark: *watermark,
    };
    let mut parts = encode_within(run_id, &part(0, first), max_size)?;
    parts.extend(encode_within(
        run_id,
        &part(first, *record_count - first),
        max_size,
    )?);
    Ok(parts)
}

fn too_large(encoded: &EncodedMessage, max: usize) -> CodecError {
    CodecError::TooLarge {
        size: encoded.size(),
        max,
    }
}

pub fn decode(
    run_id: &str,
    subject: &str,
    headers: &HeaderMap,
    payload: &[u8],
) -> Result<PartitionMessage, CodecError> {
    let partition = partition_of(run_id, subject)?;
    let kind = header(headers, KIND_HEADER)?;
    let position = Position::new(
        parsed(headers, FILE_SEQ_HEADER)?,
        parsed(headers, RECORD_OFFSET_HEADER)?,
    );
    match kind {
        "chunk" => {
            let FileHeader { file, .. } = file_header(headers)?;
            Ok(PartitionMessage::FileChunk {
                partition,
                position,
                record_count: parsed(headers, RECORD_COUNT_HEADER)?,
                file,
                events: read_events(payload)?,
                watermark: watermark(headers)?,
            })
        }
        "error" => {
            let FileHeader { file, reason } = file_header(headers)?;
            Ok(PartitionMessage::FileError {
                partition,
                position,
                file,
                reason: reason.ok_or_else(|| invalid(FILE_HEADER, "no reason"))?,
                watermark: watermark(headers)?,
            })
        }
        "end" => Ok(PartitionMessage::EndOfPartition {
            partition,
            position,
        }),
        other => Err(invalid(KIND_HEADER, other)),
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, CodecError> {
    headers
        .get(name)
        .map(|value| value.as_str())
        .ok_or(CodecError::MissingHeader(name))
}

fn parsed<T: FromStr>(headers: &HeaderMap, name: &'static str) -> Result<T, CodecError> {
    let value = header(headers, name)?;
    value.parse().map_err(|_| invalid(name, value))
}

fn invalid(name: &'static str, value: &str) -> CodecError {
    CodecError::InvalidHeader {
        name,
        value: value.to_owned(),
    }
}

fn watermark_value(watermark: EventTime) -> String {
    watermark.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn watermark(headers: &HeaderMap) -> Result<EventTime, CodecError> {
    let value = header(headers, WATERMARK_HEADER)?;
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| invalid(WATERMARK_HEADER, value))
}

fn file_value(file: &IngestedFile, reason: Option<&String>) -> Result<String, CodecError> {
    serde_json::to_string(&FileHeader {
        file: file.clone(),
        reason: reason.cloned(),
    })
    .map_err(CodecError::File)
}

fn file_header(headers: &HeaderMap) -> Result<FileHeader, CodecError> {
    serde_json::from_str(header(headers, FILE_HEADER)?).map_err(CodecError::File)
}

fn write_events(events: &RecordBatch) -> Result<Bytes, CodecError> {
    let mut buffer = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buffer, &events.schema()).map_err(CodecError::Ipc)?;
    writer.write(events).map_err(CodecError::Ipc)?;
    writer.finish().map_err(CodecError::Ipc)?;
    drop(writer);
    Ok(Bytes::from(buffer))
}

fn read_events(payload: &[u8]) -> Result<RecordBatch, CodecError> {
    let reader = StreamReader::try_new(Cursor::new(payload), None).map_err(CodecError::Ipc)?;
    let schema = reader.schema();
    let mut batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(CodecError::Ipc)?;
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => Ok(batches.remove(0)),
        count => Err(CodecError::Ipc(ArrowError::IpcError(format!(
            "a message holds {count} record batches instead of one"
        )))),
    }
}
