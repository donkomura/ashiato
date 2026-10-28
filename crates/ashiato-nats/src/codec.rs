use ashiato_pipeline::{PartitionId, PartitionMessage, Position};
use async_nats::HeaderMap;
use bytes::Bytes;

pub const KIND_HEADER: &str = "Ashiato-Kind";
pub const FILE_SEQ_HEADER: &str = "Ashiato-File-Seq";
pub const RECORD_OFFSET_HEADER: &str = "Ashiato-Record-Offset";
pub const RECORD_COUNT_HEADER: &str = "Ashiato-Record-Count";
pub const WATERMARK_HEADER: &str = "Ashiato-Watermark";
pub const FILE_HEADER: &str = "Ashiato-File";
pub const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

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
        todo!()
    }
}

pub fn validate_run_id(run_id: &str) -> Result<(), CodecError> {
    todo!("{run_id}")
}

pub fn stream_name(run_id: &str) -> String {
    todo!("{run_id}")
}

pub fn events_subjects(run_id: &str) -> String {
    todo!("{run_id}")
}

pub fn events_subject(run_id: &str, partition: &PartitionId) -> String {
    todo!("{run_id}{partition}")
}

pub fn consumer_name(partition: &PartitionId) -> String {
    todo!("{partition}")
}

pub fn message_id(partition: &PartitionId, position: Position) -> String {
    todo!("{partition}{position:?}")
}

pub fn partition_of(run_id: &str, subject: &str) -> Result<PartitionId, CodecError> {
    todo!("{run_id}{subject}")
}

pub fn encode(run_id: &str, message: &PartitionMessage) -> Result<EncodedMessage, CodecError> {
    todo!("{run_id}{message:?}")
}

pub fn encode_within(
    run_id: &str,
    message: &PartitionMessage,
    max_size: usize,
) -> Result<Vec<EncodedMessage>, CodecError> {
    todo!("{run_id}{message:?}{max_size}")
}

pub fn decode(
    run_id: &str,
    subject: &str,
    headers: &HeaderMap,
    payload: &[u8],
) -> Result<PartitionMessage, CodecError> {
    todo!("{run_id}{subject}{headers:?}{payload:?}")
}
