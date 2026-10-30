use std::fs;
use std::path::Path;

use tempfile::TempDir;
use xtask::topology::{
    Cpuset, TopologyError, assign_cpus, parse_cpu_list, physical_cores, read_physical_cores,
};

fn cores(cpus: &[&[u32]]) -> Vec<Cpuset> {
    cpus.iter()
        .map(|core| Cpuset::new(core.iter().copied()))
        .collect()
}

fn six_cores() -> Vec<Cpuset> {
    cores(&[&[0, 1], &[2, 3], &[4, 5], &[6, 7], &[8, 9], &[10, 11]])
}

#[test]
fn cpu_lists_of_the_kernel_are_read_as_sets_of_cpus() {
    let cases: [(&str, &[u32]); 5] = [
        ("0-1", &[0, 1]),
        ("0,6", &[0, 6]),
        ("3", &[3]),
        ("0-1,4-5\n", &[0, 1, 4, 5]),
        ("10-11", &[10, 11]),
    ];
    for (text, expected) in cases {
        assert_eq!(
            parse_cpu_list(text).unwrap(),
            Cpuset::new(expected.iter().copied()),
            "{text:?}"
        );
    }
}

#[test]
fn malformed_cpu_lists_are_errors() {
    for text in ["", "\n", "a", "3-1", "1-", "-1", "0,,1"] {
        assert!(
            matches!(parse_cpu_list(text), Err(TopologyError::InvalidCpuList(_))),
            "{text:?}"
        );
    }
}

#[test]
fn a_cpuset_is_written_as_docker_takes_it() {
    let cases: [(&[u32], &str); 3] = [(&[0, 1], "0,1"), (&[6, 0], "0,6"), (&[3], "3")];
    for (cpus, expected) in cases {
        assert_eq!(Cpuset::new(cpus.iter().copied()).to_string(), expected);
    }
}

#[test]
fn each_physical_core_is_the_set_of_its_sibling_cpus_in_the_order_of_their_numbers() {
    let cases: [(&[&str], Vec<Cpuset>); 3] = [
        (&["0-1", "0-1", "2-3", "2-3"], cores(&[&[0, 1], &[2, 3]])),
        (&["0,6", "1,7", "0,6", "1,7"], cores(&[&[0, 6], &[1, 7]])),
        (
            &["10-11", "0-1", "10-11", "0-1"],
            cores(&[&[0, 1], &[10, 11]]),
        ),
    ];
    for (lists, expected) in cases {
        assert_eq!(
            physical_cores(lists.iter().copied()).unwrap(),
            expected,
            "{lists:?}"
        );
    }
}

fn write_siblings(dir: &Path, cpu: u32, siblings: &str) {
    let topology = dir.join(format!("cpu{cpu}")).join("topology");
    fs::create_dir_all(&topology).unwrap();
    fs::write(
        topology.join("thread_siblings_list"),
        format!("{siblings}\n"),
    )
    .unwrap();
}

#[test]
fn the_topology_is_read_from_the_cpu_directories_of_sysfs() {
    let dir = TempDir::new().unwrap();
    for (cpu, siblings) in [
        (0, "0-1"),
        (1, "0-1"),
        (2, "2-3"),
        (3, "2-3"),
        (10, "10-11"),
    ] {
        write_siblings(dir.path(), cpu, siblings);
    }
    fs::create_dir_all(dir.path().join("cpufreq")).unwrap();
    fs::create_dir_all(dir.path().join("cpu4")).unwrap();
    fs::write(dir.path().join("online"), "0-11\n").unwrap();

    assert_eq!(
        read_physical_cores(dir.path()).unwrap(),
        cores(&[&[0, 1], &[2, 3], &[10, 11]])
    );
}

#[test]
fn a_directory_without_cpu_topology_is_an_error() {
    let dir = TempDir::new().unwrap();
    assert!(matches!(
        read_physical_cores(dir.path()),
        Err(TopologyError::NoCpus)
    ));
    assert!(matches!(
        read_physical_cores(&dir.path().join("missing")),
        Err(TopologyError::Read { .. })
    ));
}

#[test]
fn servers_analyzer_and_workers_take_whole_physical_cores_in_order() {
    let assignment = assign_cpus(&six_cores(), 4).unwrap();

    assert_eq!(assignment.servers.to_string(), "0,1");
    assert_eq!(assignment.analyzer.to_string(), "2,3");
    let workers: Vec<String> = assignment.workers.iter().map(Cpuset::to_string).collect();
    assert_eq!(workers, ["4,5", "6,7", "8,9", "10,11"]);
}

#[test]
fn the_number_of_workers_needs_as_many_physical_cores_after_the_first_two() {
    let cases = [
        (six_cores(), 0, Some(0)),
        (six_cores(), 1, Some(1)),
        (cores(&[&[0, 1], &[2, 3], &[4, 5]]), 1, Some(1)),
        (cores(&[&[0, 1], &[2, 3], &[4, 5]]), 2, None),
        (cores(&[&[0, 1]]), 0, None),
    ];
    for (cores, workers, expected) in cases {
        let assigned = assign_cpus(&cores, workers).ok().map(|a| a.workers.len());
        assert_eq!(
            assigned,
            expected,
            "{} cores, {workers} workers",
            cores.len()
        );
    }
    assert!(matches!(
        assign_cpus(&cores(&[&[0, 1], &[2, 3], &[4, 5]]), 2),
        Err(TopologyError::NotEnoughCores {
            needed: 4,
            available: 3
        })
    ));
}

#[test]
fn more_workers_than_the_plan_allows_are_an_error() {
    let mut many = six_cores();
    many.push(Cpuset::new([12, 13]));
    assert!(matches!(
        assign_cpus(&many, 5),
        Err(TopologyError::TooManyWorkers { workers: 5 })
    ));
}

#[test]
fn a_cpuset_counts_its_logical_cpus() {
    assert_eq!(Cpuset::new([0, 6]).count(), 2);
    assert_eq!(Cpuset::new([0]).count(), 1);
    assert_eq!(parse_cpu_list("0-3,8").unwrap().count(), 5);
}
