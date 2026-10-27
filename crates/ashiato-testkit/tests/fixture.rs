use ashiato_core::fixture::fixture_window;
use ashiato_testkit::fixture::{Expectation, Fixture, FixtureError, FixtureFile, builtin_fixtures};
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

fn fixture_file(path: &str, content: Value) -> FixtureFile {
    FixtureFile {
        path: path.to_owned(),
        content: content.to_string(),
    }
}

#[test]
fn positive_fixture_is_identified_by_its_directory() {
    let fixture = Fixture::parse(&fixture_file(
        "console_login/fixtures/positive/login.json",
        json!({ "Records": [] }),
    ))
    .unwrap();

    assert_eq!(fixture.rule_id, "console_login");
    assert_eq!(fixture.expectation, Expectation::Positive);
    assert_eq!(fixture.path, "console_login/fixtures/positive/login.json");
}

#[test]
fn negative_fixture_is_identified_by_its_directory() {
    let fixture = Fixture::parse(&fixture_file(
        "rules/console_login/fixtures/negative/with_mfa.json",
        json!({ "Records": [] }),
    ))
    .unwrap();

    assert_eq!(fixture.rule_id, "console_login");
    assert_eq!(fixture.expectation, Expectation::Negative);
}

#[test]
fn records_are_completed_with_defaults() {
    let fixture = Fixture::parse(&fixture_file(
        "console_login/fixtures/positive/login.json",
        json!({ "Records": [
            { "eventName": "ConsoleLogin", "userIdentity": { "userName": "bob" } },
            { "eventName": "RunInstances" },
        ] }),
    ))
    .unwrap();

    assert_eq!(fixture.records.len(), 2);
    let login = &fixture.records[0];
    assert_eq!(login["eventName"], "ConsoleLogin");
    assert_eq!(login["userIdentity"]["userName"], "bob");
    assert_eq!(login["userIdentity"]["type"], "IAMUser");
    assert!(login["eventID"].is_string(), "eventID is missing: {login}");
    assert_ne!(fixture.records[0]["eventID"], fixture.records[1]["eventID"]);
}

#[test]
fn fixture_outside_the_fixture_layout_is_rejected() {
    for path in [
        "console_login/positive/login.json",
        "console_login/fixtures/other/login.json",
        "console_login/fixtures/positive/login.yaml",
        "fixtures/positive/login.json",
    ] {
        let error = Fixture::parse(&fixture_file(path, json!({ "Records": [] }))).unwrap_err();

        assert!(
            matches!(&error, FixtureError::Layout { path: p } if p == path),
            "unexpected error for {path}: {error:?}"
        );
    }
}

#[test]
fn fixture_without_records_is_rejected() {
    let error = Fixture::parse(&fixture_file(
        "console_login/fixtures/positive/login.json",
        json!({ "records": [] }),
    ))
    .unwrap_err();

    assert!(
        matches!(&error, FixtureError::Json { path, .. }
            if path == "console_login/fixtures/positive/login.json"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn fixture_with_a_non_object_record_is_rejected_with_its_index() {
    let error = Fixture::parse(&fixture_file(
        "console_login/fixtures/positive/login.json",
        json!({ "Records": [{}, "ConsoleLogin"] }),
    ))
    .unwrap_err();

    assert!(
        matches!(&error, FixtureError::Record { index: 1, .. }),
        "unexpected error: {error:?}"
    );
}

#[test]
fn builtin_fixtures_can_be_parsed() {
    builtin_fixtures().unwrap();
}

#[test]
fn fixture_window_contains_the_default_event_time() {
    let window = fixture_window();
    let event_time: DateTime<Utc> = RecordBuilder::new().build()["eventTime"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(window.since() <= event_time && event_time < window.until());
    assert!(window.baseline_since() < window.baseline_until());
}
