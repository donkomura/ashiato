use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;
use xtask::binary::{
    BinaryError, BuildArtifacts, ashiato_binary, library_dir, parse_build, release_build,
};
use xtask::compose::AshiatoBinary;

fn messages(binary: &str, library_paths: &[&str]) -> String {
    let linked: Vec<String> = library_paths
        .iter()
        .map(|path| format!("\"native={path}\""))
        .collect();
    [
        r#"{"reason":"compiler-artifact","package_id":"registry+https://github.com/rust-lang/crates.io-index#serde@1.0.229","target":{"kind":["lib"],"name":"serde"},"executable":null,"fresh":true}"#.to_owned(),
        r#"{"reason":"build-script-executed","package_id":"registry+https://github.com/rust-lang/crates.io-index#ring@0.17.14","linked_libs":["static=ring_core"],"linked_paths":["native=/repo/target/release/build/ring-1/out"]}"#.to_owned(),
        format!(
            r#"{{"reason":"build-script-executed","package_id":"registry+https://github.com/rust-lang/crates.io-index#libduckdb-sys@1.20000.0","linked_libs":["dylib=duckdb"],"linked_paths":[{}],"out_dir":"/repo/target/release/build/libduckdb-sys-1/out"}}"#,
            linked.join(",")
        ),
        r#"{"reason":"compiler-artifact","package_id":"path+file:///repo/crates/ashiato-cli#0.1.0","target":{"kind":["lib"],"name":"ashiato_cli"},"executable":null,"fresh":false}"#.to_owned(),
        format!(
            r#"{{"reason":"compiler-artifact","package_id":"path+file:///repo/crates/ashiato-cli#0.1.0","target":{{"kind":["bin"],"name":"ashiato"}},"executable":"{binary}","fresh":false}}"#
        ),
        r#"{"reason":"build-finished","success":true}"#.to_owned(),
    ]
    .join("\n")
}

#[test]
fn ashiato_is_built_on_the_host_in_release_with_messages_in_json() {
    assert_eq!(
        release_build(),
        [
            "build",
            "--release",
            "--package",
            "ashiato-cli",
            "--message-format=json-render-diagnostics",
        ]
    );
}

#[test]
fn the_binary_and_the_link_paths_of_libduckdb_are_read_from_the_messages_of_cargo() {
    let artifacts = parse_build(&messages(
        "/repo/target/release/ashiato",
        &[
            "/repo/target/duckdb-download/x86_64-unknown-linux-gnu/v2.0.0",
            "/repo/target/release/build/libduckdb-sys-1/out",
        ],
    ))
    .unwrap();

    assert_eq!(
        artifacts,
        BuildArtifacts {
            binary: PathBuf::from("/repo/target/release/ashiato"),
            library_paths: vec![
                PathBuf::from("/repo/target/duckdb-download/x86_64-unknown-linux-gnu/v2.0.0"),
                PathBuf::from("/repo/target/release/build/libduckdb-sys-1/out"),
            ],
        }
    );
}

#[test]
fn a_build_without_the_binary_or_libduckdb_is_an_error() {
    let without_binary = messages("/repo/target/release/ashiato", &["/repo/lib"])
        .lines()
        .filter(|line| !line.contains("\"executable\":\"/repo"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(matches!(
        parse_build(&without_binary),
        Err(BinaryError::NoBinary)
    ));

    let without_duckdb = messages("/repo/target/release/ashiato", &["/repo/lib"])
        .lines()
        .filter(|line| !line.contains("libduckdb-sys"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(matches!(
        parse_build(&without_duckdb),
        Err(BinaryError::NoLibrary(paths)) if paths.is_empty()
    ));
}

#[test]
fn the_library_directory_is_the_link_path_that_holds_libduckdb() {
    let dir = TempDir::new().unwrap();
    let download = dir.path().join("duckdb-download");
    let out = dir.path().join("out");
    fs::create_dir_all(&download).unwrap();
    fs::create_dir_all(&out).unwrap();
    fs::write(download.join("libduckdb.so"), b"").unwrap();
    let paths = vec![out.clone(), download.clone()];

    assert_eq!(library_dir(&paths), Some(download.as_path()));
    assert_eq!(library_dir(std::slice::from_ref(&out)), None);

    let binary = dir.path().join("ashiato");
    let found = ashiato_binary(&messages(
        binary.to_str().unwrap(),
        &[out.to_str().unwrap(), download.to_str().unwrap()],
    ))
    .unwrap();
    assert_eq!(
        found,
        AshiatoBinary {
            binary,
            library_dir: download,
        }
    );
    assert!(matches!(
        ashiato_binary(&messages("/repo/ashiato", &[out.to_str().unwrap()])),
        Err(BinaryError::NoLibrary(paths)) if paths == vec![out]
    ));
}
