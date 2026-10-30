use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use ashiato_testkit::dataset::{DatasetSpec, DatasetSummary, generate};
use ashiato_testkit::minio::MINIO_IMAGE;
use ashiato_testkit::nats::NATS_IMAGE;

use crate::binary::{ashiato_binary, release_build};
use crate::compare::{Comparison, compare_workspaces, outputs_match, stream_left};
use crate::compose::{AshiatoBinary, ComposeSpec, RUNTIME_IMAGE, RunDir, Service, compose_file};
use crate::correctness::{Options, count_lines, repository};
use crate::docker::{
    ComposeProject, MinioSetup, NATS_READY, SETUP_LABEL, dataset_dir, minio_volume, parse_ps,
    project_name,
};
use crate::metrics::latest_analyzer;
use crate::plan::{PlanSpec, write_plan};
use crate::report::{Environment, ImageVersion, Injection, RunResult};
use crate::schedule::{Fault, RunSpec, Step};
use crate::topology::{CpuAssignment, Cpuset};

pub const CPU_DIR: &str = "/sys/devices/system/cpu";
const VOLUME_LABEL: &str = "ashiato-integration=minio";
const SUMMARY_FILE: &str = "dataset.json";
const COMPOSE_LOG: &str = "compose.log";
const POLL: Duration = Duration::from_millis(500);
const SERVER_TIMEOUT: Duration = Duration::from_secs(60);

pub fn environment(cores: Vec<Cpuset>, cpus: CpuAssignment) -> anyhow::Result<Environment> {
    let first_line = |args: &[&str]| -> anyhow::Result<String> {
        let output = docker(&strings(args))?;
        Ok(output.lines().next().unwrap_or_default().trim().to_owned())
    };
    let run = ["run", "--rm", "--label", SETUP_LABEL];
    Ok(Environment {
        cores,
        cpus,
        docker: first_line(&["version", "--format", "{{.Server.Version}}"])?,
        compose: first_line(&["compose", "version", "--short"])?,
        nats: ImageVersion {
            image: NATS_IMAGE.to_owned(),
            version: first_line(&[&run[..], &[NATS_IMAGE, "--version"]].concat())?,
        },
        minio: ImageVersion {
            image: MINIO_IMAGE.to_owned(),
            version: first_line(
                &[
                    &run[..],
                    &["--entrypoint", "minio", MINIO_IMAGE, "--version"],
                ]
                .concat(),
            )?,
        },
        runtime_image: RUNTIME_IMAGE.to_owned(),
    })
}

pub fn prepare_dataset(options: &Options) -> anyhow::Result<(PathBuf, DatasetSummary)> {
    let spec = &options.dataset;
    let dir = dataset_dir(&options.integration_dir, spec);
    let summary_path = dir.join(SUMMARY_FILE);
    if !summary_path.is_file() {
        // The generator writes the summary last, so a directory without it was left unfinished.
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        eprintln!("integration: generating {}", dir.display());
        generate(spec, &dir)?;
    }
    let summary: DatasetSummary = serde_json::from_slice(&fs::read(&summary_path)?)
        .with_context(|| format!("failed to read {}", summary_path.display()))?;
    ensure!(
        summary.kind == spec.kind && summary.seed == spec.seed && summary.scale == spec.scale,
        "{} holds another dataset than {spec:?}; remove the directory",
        dir.display()
    );
    Ok((dir, summary))
}

pub fn place_dataset(spec: &DatasetSpec, dir: &Path) -> anyhow::Result<String> {
    let volume = minio_volume(spec);
    docker(&strings(&[
        "volume",
        "create",
        "--label",
        VOLUME_LABEL,
        &volume,
    ]))?;
    let setup = MinioSetup {
        container: format!("ashiato-integration-setup-{}", std::process::id()),
        volume: volume.clone(),
        dataset: dir.to_path_buf(),
    };
    docker(&setup.run())?;
    let uploaded = upload(&setup, dir);
    let removed = docker(&setup.remove());
    uploaded?;
    removed?;
    Ok(volume)
}

fn upload(setup: &MinioSetup, dir: &Path) -> anyhow::Result<()> {
    wait_for(
        "MinIO",
        SERVER_TIMEOUT,
        || Ok(docker(&setup.live()).is_ok()),
    )?;
    let summary = fs::read_to_string(dir.join(SUMMARY_FILE))?;
    if docker(&setup.uploaded_summary()).is_ok_and(|uploaded| uploaded == summary) {
        eprintln!("integration: {} already holds the dataset", setup.volume);
        return Ok(());
    }
    eprintln!(
        "integration: uploading {} to {}",
        dir.display(),
        setup.volume
    );
    for args in setup.upload() {
        docker(&args)?;
    }
    Ok(())
}

pub fn build_ashiato() -> anyhow::Result<AshiatoBinary> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(release_build())
        .current_dir(repository())
        .stderr(Stdio::inherit())
        .output()
        .context("failed to run cargo")?;
    ensure!(output.status.success(), "cargo failed to build ashiato");
    Ok(ashiato_binary(&String::from_utf8_lossy(&output.stdout))?)
}

pub fn compare(reference: &RunDir, run: &RunDir, run_id: &str) -> anyhow::Result<Comparison> {
    let output = |dir: &RunDir| fs::read(dir.stdout(Service::Analyzer)).unwrap_or_default();
    Ok(Comparison {
        output_matches: outputs_match(&output(reference), &output(run)),
        workspace: compare_workspaces(&reference.database(), &run.database())?,
        stream_removed: !stream_left(&run.nats_store(), run_id),
    })
}

pub struct Runner {
    pub integration_dir: PathBuf,
    pub dataset: &'static str,
    pub cpus: CpuAssignment,
    pub ashiato: AshiatoBinary,
    pub minio_volume: String,
    pub plan: PlanSpec,
    pub timeout: Duration,
}

impl Runner {
    pub fn run_dir(&self, name: &str) -> RunDir {
        RunDir::new(self.integration_dir.join(name))
    }

    pub fn execute(&self, spec: RunSpec) -> RunResult {
        eprintln!("integration: run {}", spec.name);
        let run_dir = self.run_dir(spec.name);
        let project = ComposeProject {
            file: run_dir.compose_file(),
            project: project_name(self.dataset, spec.name),
        };
        let started = Instant::now();
        let mut injection = None;
        let outcome = self
            .prepare(spec, &run_dir, &project.project)
            .and_then(|()| {
                drive(
                    &project,
                    &run_dir,
                    spec,
                    started + self.timeout,
                    &mut injection,
                )
            });
        let elapsed = started.elapsed();
        let finished = if run_dir.compose_file().is_file() {
            finish(&project, &run_dir)
        } else {
            Ok(())
        };
        let errors: Vec<String> = [outcome.as_ref().err(), finished.as_ref().err()]
            .into_iter()
            .flatten()
            .map(|error| format!("{error:#}"))
            .collect();
        let output = fs::read(run_dir.stdout(Service::Analyzer)).unwrap_or_default();
        let result = RunResult {
            spec,
            elapsed,
            analyzer_exit: outcome.ok(),
            output_lines: count_lines(&output),
            injection,
            comparison: None,
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        };
        eprintln!(
            "integration: run {} ended in {:.1} s with {:?}",
            spec.name,
            elapsed.as_secs_f64(),
            result.analyzer_exit
        );
        result
    }

    fn prepare(&self, spec: RunSpec, run_dir: &RunDir, project: &str) -> anyhow::Result<()> {
        run_dir.prepare()?;
        write_plan(&self.plan, spec.layout, &run_dir.workspace())?;
        let owner = fs::metadata(run_dir.root())?;
        let mut cpus = self.cpus.clone();
        cpus.workers.truncate(spec.layout.workers() as usize);
        let compose = ComposeSpec {
            project: project.to_owned(),
            layout: spec.layout,
            cpus,
            user: format!("{}:{}", owner.uid(), owner.gid()),
            run_id: self.plan.run_id.clone(),
            run_dir: run_dir.clone(),
            ashiato: self.ashiato.clone(),
            minio_volume: self.minio_volume.clone(),
        };
        fs::write(run_dir.compose_file(), compose_file(&compose))?;
        Ok(())
    }
}

fn drive(
    project: &ComposeProject,
    run_dir: &RunDir,
    spec: RunSpec,
    deadline: Instant,
    injection: &mut Option<Injection>,
) -> anyhow::Result<i64> {
    let servers = spec.servers();
    docker(&project.up(&servers))?;
    wait_for("MinIO", SERVER_TIMEOUT, || {
        Ok(docker(&project.minio_live()).is_ok())
    })?;
    if servers.contains(&Service::Nats) {
        wait_for("NATS", SERVER_TIMEOUT, || {
            Ok(docker(&project.logs(Service::Nats))?.contains(NATS_READY))
        })?;
    }
    // docker compose start does nothing for a service without a container, so the worker that
    // F4 starts late is created now.
    if let Some(late) = spec.fault.and_then(Fault::held_back) {
        docker(&project.create(&[late]))?;
    }
    docker(&project.up(&spec.roles()))?;
    let roles_started = Instant::now();
    loop {
        let state = parse_ps(&docker(&project.ps(Service::Analyzer))?)
            .context("compose did not report the analyzer")?;
        if state.exited() {
            return Ok(state.exit_code);
        }
        ensure!(
            Instant::now() < deadline,
            "the run did not end within its time limit"
        );
        if let Some(fault) = spec.fault
            && injection.is_none()
        {
            let log = fs::read_to_string(run_dir.stderr(Service::Analyzer)).unwrap_or_default();
            // The final line comes after the ingestion, when a fault can no longer hit it.
            let metrics = latest_analyzer(&log).filter(|metrics| !metrics.is_final);
            let since_start = roles_started.elapsed();
            if fault.trigger().is_due(metrics.as_ref(), since_start) {
                *injection = Some(Injection {
                    since_start,
                    ingested_files: metrics.as_ref().map_or(0, |metrics| metrics.ingested_files),
                    manifest_files: metrics.as_ref().map_or(0, |metrics| metrics.manifest_files),
                });
                inject(project, &fault.steps())?;
                continue;
            }
        }
        thread::sleep(POLL);
    }
}

fn inject(project: &ComposeProject, steps: &[Step]) -> anyhow::Result<()> {
    for step in steps {
        match *step {
            Step::Kill(service) => docker(&project.kill(service))?,
            Step::Pause(duration) => {
                thread::sleep(duration);
                String::new()
            }
            Step::Start(service) => docker(&project.start(service))?,
        };
    }
    Ok(())
}

// The logs of MinIO and NATS exist only in Docker, so they are kept before the containers go.
fn finish(project: &ComposeProject, run_dir: &RunDir) -> anyhow::Result<()> {
    let logs = docker(&project.all_logs())
        .and_then(|logs| Ok(fs::write(run_dir.logs().join(COMPOSE_LOG), logs)?));
    docker(&project.down())?;
    logs
}

fn wait_for(
    what: &str,
    timeout: Duration,
    mut ready: impl FnMut() -> anyhow::Result<bool>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while !ready()? {
        ensure!(
            Instant::now() < deadline,
            "{what} was not ready within {} s",
            timeout.as_secs()
        );
        thread::sleep(POLL);
    }
    Ok(())
}

pub fn docker(args: &[String]) -> anyhow::Result<String> {
    let output = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .context("failed to run docker")?;
    ensure!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}
