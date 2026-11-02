use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, ensure};

use crate::binary::{BINARY, DUCKDB_LIBRARY, release_build};
use crate::cli::{DistArgs, DistTarget};
use crate::compose::AshiatoBinary;
use crate::correctness::repository;
use crate::runner::build_ashiato_with;

pub const RUN_PATH_RUSTFLAGS: &str = "-Clink-arg=-Wl,-rpath,$ORIGIN/../lib";
const LICENSES: [&str; 2] = ["LICENSE", "LICENSE-duckdb"];

pub fn release_name(version: &str, target: DistTarget) -> String {
    format!("{BINARY}-{version}-{}", target.triple())
}

pub fn build_args(target: DistTarget) -> Vec<String> {
    let mut args = release_build();
    args.extend(["--target".to_owned(), target.triple().to_owned()]);
    args
}

pub fn stage(
    ashiato: &AshiatoBinary,
    repository: &Path,
    output: &Path,
    name: &str,
) -> anyhow::Result<PathBuf> {
    let root = output.join(name);
    if root.exists() {
        fs::remove_dir_all(&root)
            .with_context(|| format!("failed to remove {}", root.display()))?;
    }
    for dir in [root.join("bin"), root.join("lib")] {
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }
    copy(&ashiato.binary, &root.join("bin").join(BINARY))?;
    copy(
        &ashiato.library_dir.join(DUCKDB_LIBRARY),
        &root.join("lib").join(DUCKDB_LIBRARY),
    )?;
    for license in LICENSES {
        copy(&repository.join(license), &root.join(license))?;
    }
    Ok(root)
}

fn copy(from: &Path, to: &Path) -> anyhow::Result<()> {
    fs::copy(from, to).with_context(|| format!("failed to copy {}", from.display()))?;
    Ok(())
}

pub fn archive(output: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let path = output.join(format!("{name}.tar.gz"));
    let status = Command::new("tar")
        .args(["--owner=0", "--group=0", "--numeric-owner", "-czf"])
        .arg(&path)
        .arg("-C")
        .arg(output)
        .arg(name)
        .status()
        .context("failed to run tar")?;
    ensure!(status.success(), "tar failed to write {}", path.display());
    Ok(path)
}

pub fn dist(args: &DistArgs) -> anyhow::Result<PathBuf> {
    let repository = repository();
    let ashiato = build_ashiato_with(build_args(args.target), Some(RUN_PATH_RUSTFLAGS))?;
    let output = repository.join(&args.output);
    let name = release_name(env!("CARGO_PKG_VERSION"), args.target);
    stage(&ashiato, repository, &output, &name)?;
    archive(&output, &name)
}
