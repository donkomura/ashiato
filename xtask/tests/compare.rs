use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use xtask::compare::{
    Comparison, TABLES, TableDifference, WorkspaceComparison, compare_workspaces, outputs_match,
    stream_left,
};

// Each table has one column, which is enough to tell rows and their multiplicity apart.
fn workspace(dir: &Path, name: &str, rows: &[(&str, &[i64])]) -> PathBuf {
    let path = dir.join(name).join("ashiato.duckdb");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let connection = duckdb::Connection::open(&path).unwrap();
    for table in TABLES.iter().chain(&["pending"]) {
        connection
            .execute_batch(&format!("CREATE TABLE {table} (value BIGINT)"))
            .unwrap();
    }
    for (table, values) in rows {
        for value in *values {
            connection
                .execute(&format!("INSERT INTO {table} VALUES (?)"), [value])
                .unwrap();
        }
    }
    path
}

fn no_difference() -> Vec<TableDifference> {
    TABLES
        .iter()
        .map(|table| TableDifference {
            table: (*table).to_owned(),
            only_in_reference: 0,
            only_in_run: 0,
        })
        .collect()
}

const ROWS: [(&str, &[i64]); 3] = [
    ("signals", &[1, 2, 2]),
    ("baseline", &[7]),
    ("partition_progress", &[3, 4]),
];

#[test]
fn the_tables_of_14_1_3_are_compared() {
    assert_eq!(
        TABLES,
        [
            "signals",
            "lineage_edges",
            "baseline",
            "ingested_files",
            "late_events",
            "batches",
            "data_counters",
            "partition_progress",
        ]
    );
}

#[test]
fn workspaces_with_the_same_rows_match() {
    let dir = TempDir::new().unwrap();
    let reference = workspace(dir.path(), "r0", &ROWS);
    let run = workspace(dir.path(), "it's d4", &ROWS);

    let comparison = compare_workspaces(&reference, &run).unwrap();

    assert_eq!(
        comparison,
        WorkspaceComparison {
            tables: no_difference(),
            pending_rows: 0,
        }
    );
    assert!(comparison.matches());
}

#[test]
fn rows_are_compared_in_both_directions_with_their_multiplicity() {
    let dir = TempDir::new().unwrap();
    let reference = workspace(dir.path(), "r0", &ROWS);
    let run = workspace(
        dir.path(),
        "d4",
        &[
            ("signals", &[1, 2]),
            ("baseline", &[7]),
            ("partition_progress", &[3, 4, 5, 5]),
            ("pending", &[9]),
        ],
    );

    let comparison = compare_workspaces(&reference, &run).unwrap();

    let mut expected = no_difference();
    expected[0].only_in_reference = 1;
    expected[7].only_in_run = 2;
    assert_eq!(
        comparison,
        WorkspaceComparison {
            tables: expected,
            pending_rows: 1,
        }
    );
    assert!(!comparison.matches());
}

#[test]
fn rows_left_in_pending_alone_fail_the_comparison() {
    let dir = TempDir::new().unwrap();
    let reference = workspace(dir.path(), "r0", &ROWS);
    let mut rows = ROWS.to_vec();
    rows.push(("pending", &[1, 2]));
    let run = workspace(dir.path(), "d4", &rows);

    let comparison = compare_workspaces(&reference, &run).unwrap();

    assert_eq!(comparison.tables, no_difference());
    assert_eq!(comparison.pending_rows, 2);
    assert!(!comparison.matches());
}

#[test]
fn a_workspace_without_a_table_cannot_be_compared() {
    let dir = TempDir::new().unwrap();
    let reference = workspace(dir.path(), "r0", &ROWS);
    let run = workspace(dir.path(), "d4", &ROWS);
    duckdb::Connection::open(&run)
        .unwrap()
        .execute_batch("DROP TABLE late_events")
        .unwrap();

    assert!(compare_workspaces(&reference, &run).is_err());
    assert!(compare_workspaces(&reference, &dir.path().join("missing.duckdb")).is_err());
}

#[test]
fn outputs_match_only_when_they_are_the_same_bytes() {
    let cases: [(&[u8], &[u8], bool); 5] = [
        (b"{\"a\":1}\n", b"{\"a\":1}\n", true),
        (b"{\"a\":1}\n", b"{\"a\":1}", false),
        (b"a\nb\n", b"b\na\n", false),
        (b"{\"a\":1}\n", b"{\"a\": 1}\n", false),
        (b"", b"", false),
    ];
    for (reference, run, expected) in cases {
        assert_eq!(
            outputs_match(reference, run),
            expected,
            "{:?} and {:?}",
            String::from_utf8_lossy(reference),
            String::from_utf8_lossy(run)
        );
    }
}

#[test]
fn a_stream_of_the_run_left_in_the_jetstream_store_is_found() {
    let dir = TempDir::new().unwrap();
    let streams = dir.path().join("jetstream").join("$G").join("streams");
    fs::create_dir_all(streams.join("ASHIATO_other-run")).unwrap();
    fs::create_dir_all(streams.join("ASHIATO_integration-ab")).unwrap();

    assert!(!stream_left(dir.path(), "integration-a"));
    assert!(!stream_left(&dir.path().join("missing"), "integration-a"));

    fs::create_dir_all(streams.join("ASHIATO_integration-a").join("msgs")).unwrap();
    assert!(stream_left(dir.path(), "integration-a"));
}

#[test]
fn a_run_passes_only_when_every_check_of_14_1_3_holds() {
    let matching = WorkspaceComparison {
        tables: no_difference(),
        pending_rows: 0,
    };
    let mut differing = matching.clone();
    differing.tables[3].only_in_run = 1;
    let cases = [
        (true, matching.clone(), true, true),
        (false, matching.clone(), true, false),
        (true, differing, true, false),
        (true, matching, false, false),
    ];
    for (output_matches, workspace, stream_removed, expected) in cases {
        let comparison = Comparison {
            output_matches,
            workspace,
            stream_removed,
        };
        assert_eq!(comparison.passed(), expected, "{comparison:?}");
    }
}
