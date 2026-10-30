use std::path::{Path, PathBuf};

use crate::compose::AshiatoBinary;

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

pub fn release_build() -> Vec<String> {
    todo!()
}

pub fn parse_build(messages: &str) -> Result<BuildArtifacts, BinaryError> {
    let _ = messages;
    todo!()
}

pub fn library_dir(paths: &[PathBuf]) -> Option<&Path> {
    let _ = paths;
    todo!()
}

pub fn ashiato_binary(messages: &str) -> Result<AshiatoBinary, BinaryError> {
    let _ = messages;
    todo!()
}
