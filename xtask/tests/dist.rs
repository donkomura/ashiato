use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;
use xtask::cli::DistTarget;
use xtask::compose::AshiatoBinary;
use xtask::dist::{RUN_PATH_RUSTFLAGS, archive, build_args, release_name, stage};

struct Inputs {
    dir: TempDir,
    ashiato: AshiatoBinary,
}

impl Inputs {
    fn repository(&self) -> &Path {
        self.dir.path()
    }

    fn output(&self) -> std::path::PathBuf {
        self.dir.path().join("dist")
    }
}

fn inputs() -> Inputs {
    let dir = TempDir::new().unwrap();
    let build = dir.path().join("build");
    fs::create_dir(&build).unwrap();
    let binary = build.join("ashiato");
    fs::write(&binary, b"binary").unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(build.join("libduckdb.so"), b"library").unwrap();
    fs::write(dir.path().join("LICENSE"), b"ashiato license").unwrap();
    fs::write(dir.path().join("LICENSE-duckdb"), b"duckdb license").unwrap();
    Inputs {
        ashiato: AshiatoBinary {
            binary,
            library_dir: build,
        },
        dir,
    }
}

#[test]
fn the_release_is_named_by_the_version_and_the_target() {
    assert_eq!(
        release_name("1.2.3", DistTarget::X86_64),
        "ashiato-1.2.3-x86_64-unknown-linux-gnu"
    );
    assert_eq!(
        release_name("1.2.3", DistTarget::Aarch64),
        "ashiato-1.2.3-aarch64-unknown-linux-gnu"
    );
}

#[test]
fn ashiato_is_built_for_the_target_so_the_run_path_stays_out_of_build_scripts() {
    assert_eq!(
        build_args(DistTarget::Aarch64),
        [
            "build",
            "--release",
            "--package",
            "ashiato-cli",
            "--message-format=json-render-diagnostics",
            "--target",
            "aarch64-unknown-linux-gnu",
        ]
    );
}

#[test]
fn the_binary_finds_the_library_relative_to_itself() {
    assert_eq!(RUN_PATH_RUSTFLAGS, "-Clink-arg=-Wl,-rpath,$ORIGIN/../lib");
}

#[test]
fn the_binary_the_library_and_the_licenses_are_laid_out_for_the_run_path() {
    let inputs = inputs();

    let root = stage(
        &inputs.ashiato,
        inputs.repository(),
        &inputs.output(),
        "ashiato-1.2.3-x86_64-unknown-linux-gnu",
    )
    .unwrap();

    assert_eq!(
        root,
        inputs
            .output()
            .join("ashiato-1.2.3-x86_64-unknown-linux-gnu")
    );
    assert_eq!(fs::read(root.join("bin/ashiato")).unwrap(), b"binary");
    assert_eq!(fs::read(root.join("lib/libduckdb.so")).unwrap(), b"library");
    assert_eq!(fs::read(root.join("LICENSE")).unwrap(), b"ashiato license");
    assert_eq!(
        fs::read(root.join("LICENSE-duckdb")).unwrap(),
        b"duckdb license"
    );
}

#[test]
fn the_binary_stays_executable() {
    let inputs = inputs();

    let root = stage(&inputs.ashiato, inputs.repository(), &inputs.output(), "r").unwrap();

    let mode = fs::metadata(root.join("bin/ashiato"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111);
}

#[test]
fn files_of_an_earlier_run_do_not_reach_the_release() {
    let inputs = inputs();
    let stale = inputs.output().join("r/bin/stale");
    fs::create_dir_all(stale.parent().unwrap()).unwrap();
    fs::write(&stale, b"stale").unwrap();

    stage(&inputs.ashiato, inputs.repository(), &inputs.output(), "r").unwrap();

    assert!(!stale.exists());
}

#[test]
fn a_missing_library_is_an_error_that_names_the_file() {
    let inputs = inputs();
    fs::remove_file(inputs.ashiato.library_dir.join("libduckdb.so")).unwrap();

    let error = stage(&inputs.ashiato, inputs.repository(), &inputs.output(), "r").unwrap_err();

    assert!(format!("{error:#}").contains("libduckdb.so"), "{error:#}");
}

#[test]
fn a_missing_license_is_an_error_that_names_the_file() {
    let inputs = inputs();
    fs::remove_file(inputs.repository().join("LICENSE-duckdb")).unwrap();

    let error = stage(&inputs.ashiato, inputs.repository(), &inputs.output(), "r").unwrap_err();

    assert!(format!("{error:#}").contains("LICENSE-duckdb"), "{error:#}");
}

#[test]
fn the_archive_holds_one_top_level_directory_named_after_the_release() {
    let inputs = inputs();
    stage(&inputs.ashiato, inputs.repository(), &inputs.output(), "r").unwrap();

    let path = archive(&inputs.output(), "r").unwrap();

    assert_eq!(path, inputs.output().join("r.tar.gz"));
    let listing = Command::new("tar").arg("-tzf").arg(&path).output().unwrap();
    assert!(listing.status.success());
    let listing = String::from_utf8(listing.stdout).unwrap();
    let mut entries: Vec<&str> = listing.lines().collect();
    entries.sort_unstable();
    assert_eq!(
        entries,
        [
            "r/",
            "r/LICENSE",
            "r/LICENSE-duckdb",
            "r/bin/",
            "r/bin/ashiato",
            "r/lib/",
            "r/lib/libduckdb.so",
        ]
    );
}
