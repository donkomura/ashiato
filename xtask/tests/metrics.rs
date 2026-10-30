use xtask::metrics::{
    AnalyzerMetrics, WorkerMetrics, WorkerTimes, latest_analyzer, latest_worker, reached,
};

fn analyzer_line(unix_ms: i64, manifest: u64, ingested: u64, last: bool) -> String {
    format!(
        "{{\"metrics\":\"analyzer\",\"unix_ms\":{unix_ms},\"final\":{last},\
         \"manifest_files\":{manifest},\"ingested_files\":{ingested},\
         \"global_watermark\":null,\"pending_rows\":5,\"pending_max_rows\":9,\
         \"time_ms\":{{\"fetch_wait\":10,\"commit\":20,\"batch\":30}},\
         \"phases_unix_ms\":{{\"first_plan_response\":1730000000000,\
         \"last_end_committed\":null,\"correlated\":null}}}}"
    )
}

const WORKER_LINE: &str = "{\"metrics\":\"worker\",\"worker_id\":0,\"unix_ms\":1730000000500,\
    \"final\":false,\"partitions\":[{\"partition\":\"111111111111/us-east-1\",\"files\":10,\
    \"bytes\":12345,\"events\":678}],\"time_ms\":{\"fetch_wait\":10,\"decode\":20,\
    \"publish_wait\":30,\"rejected_wait\":40}}";

#[test]
fn the_latest_complete_analyzer_line_is_taken_from_a_log_with_other_lines() {
    let log = [
        "Error: failed to connect to nats://nats:4222".to_owned(),
        analyzer_line(1730000001000, 100, 10, false),
        WORKER_LINE.to_owned(),
        analyzer_line(1730000002000, 100, 40, false),
        "{\"metrics\":\"run\",\"pending_max_rows\":3}".to_owned(),
        "{\"metrics\":\"analyzer\",\"unix_ms\":1730000002500}".to_owned(),
        WORKER_LINE.to_owned(),
        "{\"metrics\":\"analyzer\",\"unix_ms\":17300000030".to_owned(),
    ]
    .join("\n");

    let metrics = latest_analyzer(&log).unwrap();

    assert_eq!(metrics.unix_ms, 1730000002000);
    assert_eq!(metrics.manifest_files, 100);
    assert_eq!(metrics.ingested_files, 40);
    assert!(!metrics.is_final);
    assert_eq!(metrics.pending_rows, 5);
    assert_eq!(metrics.pending_max_rows, 9);
    assert_eq!(metrics.time_ms.batch, 30);
    assert_eq!(
        metrics.phases_unix_ms.first_plan_response,
        Some(1730000000000)
    );
    assert_eq!(metrics.phases_unix_ms.last_end_committed, None);
}

#[test]
fn a_log_without_analyzer_lines_has_no_metrics() {
    for log in ["", "starting\n", WORKER_LINE] {
        assert_eq!(latest_analyzer(log), None, "{log:?}");
    }
}

#[test]
fn the_final_line_is_read_with_its_phases() {
    let line = analyzer_line(1730000009000, 7, 7, true).replace(
        "\"last_end_committed\":null,\"correlated\":null",
        "\"last_end_committed\":1730000008000,\"correlated\":1730000008500",
    );
    let metrics: AnalyzerMetrics = latest_analyzer(&format!("{line}\n")).unwrap();

    assert!(metrics.is_final);
    assert_eq!(
        metrics.phases_unix_ms.last_end_committed,
        Some(1730000008000)
    );
    assert_eq!(metrics.phases_unix_ms.correlated, Some(1730000008500));
}

#[test]
fn a_failure_is_injected_once_the_ingested_files_reach_its_share_of_the_manifest() {
    let cases = [
        (10, 3, 30, true),
        (10, 2, 30, false),
        (1234, 371, 30, true),
        (1234, 370, 30, false),
        (3, 2, 50, true),
        (3, 1, 50, false),
        (10, 7, 70, true),
        (10, 6, 70, false),
        (10, 10, 70, true),
        (0, 0, 30, false),
    ];
    for (manifest, ingested, percent, expected) in cases {
        let metrics = latest_analyzer(&analyzer_line(1, manifest, ingested, false)).unwrap();
        assert_eq!(
            reached(&metrics, percent),
            expected,
            "{ingested}/{manifest} at {percent}%"
        );
    }
}

#[test]
fn the_latest_complete_worker_line_is_taken_from_a_log_with_other_lines() {
    let final_line = WORKER_LINE
        .replace("\"final\":false", "\"final\":true")
        .replace("\"unix_ms\":1730000000500", "\"unix_ms\":1730000009000")
        .replace("\"rejected_wait\":40", "\"rejected_wait\":45");
    let log = [
        WORKER_LINE.to_owned(),
        "Error: publish was rejected".to_owned(),
        final_line,
        analyzer_line(1730000009500, 7, 7, true),
        "{\"metrics\":\"worker\",\"worker_id\":0,\"unix_ms\":17300000".to_owned(),
    ]
    .join("\n");

    assert_eq!(
        latest_worker(&log),
        Some(WorkerMetrics {
            worker_id: 0,
            unix_ms: 1730000009000,
            is_final: true,
            time_ms: WorkerTimes {
                fetch_wait: 10,
                decode: 20,
                publish_wait: 30,
                rejected_wait: 45,
            },
        })
    );
}

#[test]
fn a_log_without_worker_lines_has_no_worker_metrics() {
    let analyzer = analyzer_line(1, 1, 1, true);
    for log in ["", "starting\n", analyzer.as_str()] {
        assert_eq!(latest_worker(log), None, "{log:?}");
    }
}
