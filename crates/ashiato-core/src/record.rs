use serde_json::{Map, Value};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("a CloudTrail record must be a JSON object, but got {0}")]
    NotAnObject(Value),
}

pub fn complete_record(_partial: Value) -> Result<Value, RecordError> {
    todo!()
}

#[derive(Debug, Clone, Default)]
pub struct RecordBuilder {
    partial: Map<String, Value>,
}

impl RecordBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(self, _path: &str, _value: impl Into<Value>) -> Self {
        todo!()
    }

    pub fn build(self) -> Value {
        let _ = self.partial;
        todo!()
    }
}
