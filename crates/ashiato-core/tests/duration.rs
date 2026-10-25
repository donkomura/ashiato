use ashiato_core::{Duration, DurationError};
use chrono::TimeDelta;

#[test]
fn duration_is_parsed_from_amount_and_unit() {
    for (text, expected) in [
        ("30s", TimeDelta::seconds(30)),
        ("5m", TimeDelta::minutes(5)),
        ("1h", TimeDelta::hours(1)),
        ("30d", TimeDelta::days(30)),
    ] {
        let duration: Duration = text.parse().unwrap();
        assert_eq!(duration.to_time_delta(), expected, "{text}");
    }
}

#[test]
fn duration_is_displayed_as_written() {
    for text in ["30s", "5m", "1h", "30d"] {
        assert_eq!(text.parse::<Duration>().unwrap().to_string(), text);
    }
}

#[test]
fn malformed_duration_is_rejected() {
    for text in [
        "", "5", "m", "5x", "0m", "-5m", "1.5h", " 5m", "5m ", "5 m", "1hour",
    ] {
        assert_eq!(
            text.parse::<Duration>(),
            Err(DurationError::Invalid(text.to_owned())),
            "{text:?}"
        );
    }
}

#[test]
fn duration_too_long_to_represent_is_rejected() {
    let text = format!("{}d", u64::MAX);

    assert_eq!(
        text.parse::<Duration>(),
        Err(DurationError::Invalid(text.clone()))
    );
}
