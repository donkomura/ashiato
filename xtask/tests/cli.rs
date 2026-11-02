use clap::Parser;
use std::path::PathBuf;

use xtask::cli::{
    Cli, Command, CorrectnessArgs, CorrectnessDataset, DistArgs, DistTarget, Integration,
    ScalingArgs, ScalingDataset, parse_scale,
};

fn parse(args: &[&str]) -> Result<Integration, clap::Error> {
    let cli = Cli::try_parse_from(["xtask", "integration"].iter().chain(args))?;
    let Command::Integration(integration) = cli.command else {
        panic!("expected an integration command");
    };
    Ok(integration)
}

fn parse_dist(args: &[&str]) -> Result<DistArgs, clap::Error> {
    let cli = Cli::try_parse_from(["xtask", "dist"].iter().chain(args))?;
    let Command::Dist(dist) = cli.command else {
        panic!("expected the dist command");
    };
    Ok(dist)
}

#[test]
fn dist_writes_under_target_unless_told_otherwise() {
    assert_eq!(
        parse_dist(&["--target", "x86_64-unknown-linux-gnu"]).unwrap(),
        DistArgs {
            target: DistTarget::X86_64,
            output: PathBuf::from("target/dist"),
        }
    );
    assert_eq!(
        parse_dist(&["--target", "aarch64-unknown-linux-gnu", "--output", "out"]).unwrap(),
        DistArgs {
            target: DistTarget::Aarch64,
            output: PathBuf::from("out"),
        }
    );
}

#[test]
fn dist_rejects_targets_whose_loader_does_not_read_origin() {
    assert!(parse_dist(&["--target", "aarch64-apple-darwin"]).is_err());
    assert!(parse_dist(&[]).is_err());
}

#[test]
fn the_commands_of_14_1_6_are_accepted() {
    let cases = [
        (
            vec!["correctness", "--dataset", "a"],
            Integration::Correctness(CorrectnessArgs {
                dataset: CorrectnessDataset::A,
                scale: 1.0,
            }),
        ),
        (
            vec!["correctness", "--dataset", "a-no-data-events"],
            Integration::Correctness(CorrectnessArgs {
                dataset: CorrectnessDataset::ANoDataEvents,
                scale: 1.0,
            }),
        ),
        (
            vec!["correctness", "--dataset", "a", "--scale", "0.01"],
            Integration::Correctness(CorrectnessArgs {
                dataset: CorrectnessDataset::A,
                scale: 0.01,
            }),
        ),
        (
            vec![
                "scaling",
                "--dataset",
                "b",
                "--workers",
                "1,2,3,4",
                "--repeat",
                "3",
            ],
            Integration::Scaling(ScalingArgs {
                dataset: ScalingDataset::B,
                workers: vec![1, 2, 3, 4],
                repeat: 3,
                scale: 1.0,
            }),
        ),
        (
            vec!["scaling", "--dataset", "b"],
            Integration::Scaling(ScalingArgs {
                dataset: ScalingDataset::B,
                workers: vec![1, 2, 3, 4],
                repeat: 3,
                scale: 1.0,
            }),
        ),
        (
            vec![
                "scaling",
                "--dataset",
                "b",
                "--workers",
                "2",
                "--repeat",
                "1",
                "--scale",
                "0.5",
            ],
            Integration::Scaling(ScalingArgs {
                dataset: ScalingDataset::B,
                workers: vec![2],
                repeat: 1,
                scale: 0.5,
            }),
        ),
    ];
    for (args, expected) in cases {
        assert_eq!(parse(&args).unwrap(), expected, "{args:?}");
    }
}

#[test]
fn arguments_outside_the_plan_are_refused() {
    let cases = [
        vec!["correctness", "--dataset", "b"],
        vec!["correctness"],
        vec!["scaling", "--dataset", "a"],
        vec!["scaling", "--dataset", "b", "--workers", "0"],
        vec!["scaling", "--dataset", "b", "--workers", "1,5"],
        vec!["scaling", "--dataset", "b", "--repeat", "0"],
        vec!["correctness", "--dataset", "a", "--scale", "0"],
        vec!["correctness", "--dataset", "a", "--scale", "-1"],
    ];
    for args in cases {
        assert!(parse(&args).is_err(), "{args:?}");
    }
}

#[test]
fn a_scale_is_a_positive_finite_number() {
    let cases = [
        ("1.0", Some(1.0)),
        ("0.25", Some(0.25)),
        ("2", Some(2.0)),
        ("0", None),
        ("-0.5", None),
        ("inf", None),
        ("NaN", None),
        ("half", None),
    ];
    for (text, expected) in cases {
        assert_eq!(parse_scale(text).ok(), expected, "{text}");
    }
}
