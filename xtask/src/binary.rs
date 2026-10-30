use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::compose::AshiatoBinary;

const BINARY: &str = "ashiato";
const DUCKDB_PACKAGE: &str = "libduckdb-sys";
const DUCKDB_LIBRARY: &str = "libduckdb.so";
// https://doc.rust-lang.org/cargo/reference/build-scripts.html#rustc-link-search
const SEARCH_KINDS: [&str; 5] = ["dependency", "crate", "native", "framework", "all"];

#[derive(Debug, thiserror::Error)]
pub enum BinaryError {
    #[error("cargo did not report the ashiato binary")]
    NoBinary,
    #[error("cargo did not report a directory with libduckdb.so; looked in {0:?}")]
    NoLibrary(Vec<PathBuf>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildArtifacts {
    pub binary: PathBuf,
    pub library_paths: Vec<PathBuf>,
}

#[derive(Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum Message {
    CompilerArtifact {
        target: Target,
        executable: Option<PathBuf>,
    },
    BuildScriptExecuted {
        package_id: String,
        linked_paths: Vec<String>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
}

pub fn release_build() -> Vec<String> {
    [
        "build",
        "--release",
        "--package",
        "ashiato-cli",
        "--message-format=json-render-diagnostics",
    ]
    .map(str::to_owned)
    .to_vec()
}

// The binary links libduckdb.so without a run path (ldd reports it as not found), so its
// directory is taken from the link search paths of the libduckdb-sys build script.
pub fn parse_build(messages: &str) -> Result<BuildArtifacts, BinaryError> {
    let mut binary = None;
    let mut library_paths = Vec::new();
    for line in messages.lines() {
        match serde_json::from_str(line) {
            Ok(Message::CompilerArtifact {
                target,
                executable: Some(executable),
            }) if target.name == BINARY && target.kind.iter().any(|kind| kind == "bin") => {
                binary = Some(executable);
            }
            Ok(Message::BuildScriptExecuted {
                package_id,
                linked_paths,
            }) if package_id.contains(DUCKDB_PACKAGE) => {
                library_paths.extend(linked_paths.iter().map(|path| search_path(path)));
            }
            _ => {}
        }
    }
    let binary = binary.ok_or(BinaryError::NoBinary)?;
    if library_paths.is_empty() {
        return Err(BinaryError::NoLibrary(library_paths));
    }
    Ok(BuildArtifacts {
        binary,
        library_paths,
    })
}

fn search_path(path: &str) -> PathBuf {
    let path = match path.split_once('=') {
        Some((kind, rest)) if SEARCH_KINDS.contains(&kind) => rest,
        _ => path,
    };
    PathBuf::from(path)
}

pub fn library_dir(paths: &[PathBuf]) -> Option<&Path> {
    paths
        .iter()
        .map(PathBuf::as_path)
        .find(|path| path.join(DUCKDB_LIBRARY).is_file())
}

pub fn ashiato_binary(messages: &str) -> Result<AshiatoBinary, BinaryError> {
    let artifacts = parse_build(messages)?;
    let library_dir = library_dir(&artifacts.library_paths)
        .ok_or_else(|| BinaryError::NoLibrary(artifacts.library_paths.clone()))?
        .to_path_buf();
    Ok(AshiatoBinary {
        binary: artifacts.binary,
        library_dir,
    })
}
