use ashiato_core::record::{RecordBuilder, RecordError, complete_record};
use serde_json::{Value, json};

const REQUIRED_STRING_FIELDS: [&str; 8] = [
    "/eventID",
    "/eventTime",
    "/eventVersion",
    "/eventSource",
    "/eventName",
    "/awsRegion",
    "/recipientAccountId",
    "/userIdentity/type",
];

fn assert_has_required_fields(record: &Value) {
    for pointer in REQUIRED_STRING_FIELDS {
        assert!(
            record.pointer(pointer).is_some_and(Value::is_string),
            "{pointer} is missing in {record}"
        );
    }
}

#[test]
fn builder_fills_required_fields_by_default() {
    assert_has_required_fields(&RecordBuilder::new().build());
}

#[test]
fn builder_generates_different_event_id_for_each_record() {
    let first = RecordBuilder::new().build();
    let second = RecordBuilder::new().build();

    assert_ne!(first["eventID"], second["eventID"]);
}

#[test]
fn builder_keeps_specified_fields() {
    let record = RecordBuilder::new()
        .set("eventID", "evt-1")
        .set("eventTime", "2026-10-01T12:34:56Z")
        .set("eventName", "ConsoleLogin")
        .set("readOnly", false)
        .build();

    assert_eq!(record["eventID"], "evt-1");
    assert_eq!(record["eventTime"], "2026-10-01T12:34:56Z");
    assert_eq!(record["eventName"], "ConsoleLogin");
    assert_eq!(record["readOnly"], false);
}

#[test]
fn builder_sets_nested_field_without_dropping_sibling_defaults() {
    let default = RecordBuilder::new().build();
    let record = RecordBuilder::new()
        .set(
            "userIdentity.sessionContext.attributes.mfaAuthenticated",
            "true",
        )
        .build();

    assert_eq!(
        record["userIdentity"]["sessionContext"]["attributes"]["mfaAuthenticated"],
        "true"
    );
    assert_eq!(
        record["userIdentity"]["type"],
        default["userIdentity"]["type"]
    );
    assert_eq!(
        record["userIdentity"]["arn"],
        default["userIdentity"]["arn"]
    );
}

#[test]
fn builder_replaces_non_object_value_when_nested_path_is_set() {
    let record = RecordBuilder::new()
        .set("requestParameters", "placeholder")
        .set("requestParameters.bucketName", "evidence")
        .build();

    assert_eq!(
        record["requestParameters"],
        json!({"bucketName": "evidence"})
    );
}

#[test]
fn complete_record_fills_missing_fields() {
    let record = complete_record(json!({"eventName": "ListBuckets"})).unwrap();

    assert_has_required_fields(&record);
    assert_eq!(record["eventName"], "ListBuckets");
}

#[test]
fn complete_record_keeps_specified_fields() {
    let record = complete_record(json!({
        "eventID": "evt-1",
        "eventTime": "2026-10-01T12:34:56Z",
        "awsRegion": "ap-northeast-1",
        "resources": [{"ARN": "arn:aws:s3:::evidence"}],
    }))
    .unwrap();

    assert_eq!(record["eventID"], "evt-1");
    assert_eq!(record["eventTime"], "2026-10-01T12:34:56Z");
    assert_eq!(record["awsRegion"], "ap-northeast-1");
    assert_eq!(
        record["resources"],
        json!([{"ARN": "arn:aws:s3:::evidence"}])
    );
}

#[test]
fn complete_record_merges_nested_objects() {
    let default = RecordBuilder::new().build();
    let record = complete_record(json!({
        "userIdentity": {
            "type": "AssumedRole",
            "sessionContext": {"sessionIssuer": {"arn": "arn:aws:iam::111111111111:role/admin"}},
        },
    }))
    .unwrap();

    assert_eq!(record["userIdentity"]["type"], "AssumedRole");
    assert_eq!(
        record["userIdentity"]["sessionContext"]["sessionIssuer"]["arn"],
        "arn:aws:iam::111111111111:role/admin"
    );
    assert_eq!(
        record["userIdentity"]["accountId"],
        default["userIdentity"]["accountId"]
    );
}

#[test]
fn complete_record_keeps_explicit_null() {
    let record = complete_record(json!({"userIdentity": {"accessKeyId": null}})).unwrap();

    assert_eq!(record["userIdentity"]["accessKeyId"], Value::Null);
}

#[test]
fn complete_record_generates_different_event_id_for_each_record() {
    let first = complete_record(json!({})).unwrap();
    let second = complete_record(json!({})).unwrap();

    assert_ne!(first["eventID"], second["eventID"]);
}

#[test]
fn complete_record_rejects_non_object() {
    assert_eq!(
        complete_record(json!(["not", "a", "record"])),
        Err(RecordError::NotAnObject(json!(["not", "a", "record"])))
    );
}
