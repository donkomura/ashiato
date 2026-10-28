use std::num::NonZeroU32;

use ashiato_pipeline::{PartitionId, assign_partitions};

type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, u32, Vec<Vec<usize>>);

fn partitions(ids: &[(&str, &str)]) -> Vec<PartitionId> {
    ids.iter()
        .map(|(account, region)| PartitionId::new(*account, *region))
        .collect()
}

#[test]
fn partitions_are_dealt_to_the_workers_one_by_one_in_the_order_of_their_identifiers() {
    let a1 = ("111111111111", "ap-northeast-1");
    let a2 = ("111111111111", "us-east-1");
    let b1 = ("222222222222", "ap-northeast-1");
    let b2 = ("222222222222", "us-east-1");
    let c1 = ("333333333333", "eu-west-1");
    let cases: Vec<Case> = vec![
        (
            "one worker takes every partition",
            vec![a1, a2, b1],
            1,
            vec![vec![0, 1, 2]],
        ),
        (
            "two workers take turns",
            vec![a1, a2, b1, b2, c1],
            2,
            vec![vec![0, 2, 4], vec![1, 3]],
        ),
        (
            "as many workers as partitions take one each",
            vec![a1, a2, b1],
            3,
            vec![vec![0], vec![1], vec![2]],
        ),
        (
            "workers beyond the partitions take none",
            vec![a1, a2],
            3,
            vec![vec![0], vec![1], vec![]],
        ),
        ("no partitions", vec![], 2, vec![vec![], vec![]]),
        (
            "partitions out of order are dealt in the order of their identifiers",
            vec![b2, a1, c1, a2, b1],
            2,
            vec![vec![1, 4, 2], vec![3, 0]],
        ),
    ];
    for (name, ids, workers, expected) in cases {
        assert_eq!(
            assign_partitions(&partitions(&ids), NonZeroU32::new(workers).unwrap()),
            expected,
            "{name}"
        );
    }
}
