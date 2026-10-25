use std::collections::BTreeMap;

use ashiato_core::SignalRow;
use ashiato_core::lineage::{
    Caller, DEFAULT_MAX_DEPTH, Federation, LineageEdge, LineageGraph, LineageStep, LineageWarning,
    Origin, OriginKind, Resolution,
};

const ACCOUNT: &str = "111111111111";
const OTHER_ACCOUNT: &str = "222222222222";
const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";
const ALICE_KEY: &str = "AKIAALICE";

fn role_arn(account: &str, name: &str) -> String {
    format!("arn:aws:iam::{account}:role/{name}")
}

fn iam_user() -> Caller {
    Caller {
        access_key_id: Some(ALICE_KEY.to_owned()),
        principal_key: ALICE_ARN.to_owned(),
        principal_arn: Some(ALICE_ARN.to_owned()),
        identity_type: "IAMUser".to_owned(),
        account_id: Some(ACCOUNT.to_owned()),
        invoked_by: None,
    }
}

fn assumed_role(account: &str, role: &str, access_key_id: &str) -> Caller {
    let role_arn = role_arn(account, role);
    Caller {
        access_key_id: Some(access_key_id.to_owned()),
        principal_key: role_arn,
        principal_arn: Some(format!(
            "arn:aws:sts::{account}:assumed-role/{role}/session"
        )),
        identity_type: "AssumedRole".to_owned(),
        account_id: Some(account.to_owned()),
        invoked_by: None,
    }
}

fn edge(child: &str, parent: Caller, role: &str, event_id: &str) -> LineageEdge {
    LineageEdge {
        child_access_key_id: child.to_owned(),
        parent,
        role_arn: Some(role_arn(ACCOUNT, role)),
        issued_by_event_id: event_id.to_owned(),
        cross_account: false,
        federation: None,
    }
}

fn step(access_key_id: &str, role: &str, event_id: &str) -> LineageStep {
    LineageStep {
        access_key_id: access_key_id.to_owned(),
        role_arn: Some(role_arn(ACCOUNT, role)),
        issued_by_event_id: event_id.to_owned(),
    }
}

fn origin(kind: OriginKind, id: &str) -> Origin {
    Origin {
        id: id.to_owned(),
        kind,
    }
}

fn signal(start_time: &str, access_key_id: Option<&str>, principal_key: &str) -> SignalRow {
    SignalRow {
        start_time: start_time.parse().unwrap(),
        end_time: start_time.parse().unwrap(),
        event_ids: vec![format!("evt-{start_time}")],
        access_key_id: access_key_id.map(str::to_owned),
        principal_key: principal_key.to_owned(),
        source_ip: "198.51.100.1".to_owned(),
        account_id: ACCOUNT.to_owned(),
        region: "us-east-1".to_owned(),
        attrs: BTreeMap::new(),
    }
}

#[test]
fn a_session_issued_to_an_iam_user_resolves_to_the_iam_user() {
    let graph = LineageGraph::new([edge("ASIA1", iam_user(), "admin", "evt-assume")]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "admin"));

    assert_eq!(
        resolution,
        Resolution {
            origin: origin(OriginKind::IamUser, &format!("iam_user:{ALICE_ARN}")),
            lineage: vec![step("ASIA1", "admin", "evt-assume")],
            warning: None,
        }
    );
}

#[test]
fn a_session_issued_to_the_root_user_resolves_to_the_root_origin() {
    let root = Caller {
        access_key_id: Some("AKIAROOT".to_owned()),
        principal_key: format!("arn:aws:iam::{ACCOUNT}:root"),
        principal_arn: Some(format!("arn:aws:iam::{ACCOUNT}:root")),
        identity_type: "Root".to_owned(),
        account_id: Some(ACCOUNT.to_owned()),
        invoked_by: None,
    };
    let graph = LineageGraph::new([edge("ASIA1", root, "admin", "evt-assume")]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "admin"));

    assert_eq!(
        resolution.origin,
        origin(OriginKind::Root, &format!("root:{ACCOUNT}"))
    );
}

#[test]
fn a_cross_account_caller_without_its_own_lineage_resolves_to_an_external_principal() {
    let caller = assumed_role(OTHER_ACCOUNT, "partner", "ASIAPARTNER");
    let caller_arn = caller.principal_arn.clone().unwrap();
    let graph = LineageGraph::new([LineageEdge {
        cross_account: true,
        ..edge("ASIA1", caller, "shared", "evt-assume")
    }]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "shared"));

    assert_eq!(
        resolution.origin,
        origin(
            OriginKind::ExternalPrincipal,
            &format!("external:{caller_arn}")
        )
    );
}

#[test]
fn a_cross_account_caller_with_its_own_lineage_is_traced_further() {
    let partner = assumed_role(OTHER_ACCOUNT, "partner", "ASIAPARTNER");
    let graph = LineageGraph::new([
        LineageEdge {
            cross_account: true,
            ..edge("ASIA1", partner, "shared", "evt-cross")
        },
        LineageEdge {
            role_arn: Some(role_arn(OTHER_ACCOUNT, "partner")),
            ..edge("ASIAPARTNER", iam_user(), "partner", "evt-partner")
        },
    ]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "shared"));

    assert_eq!(resolution.origin.kind, OriginKind::IamUser);
    assert_eq!(resolution.lineage.len(), 2);
}

#[test]
fn a_federated_session_resolves_to_the_provider_and_subject() {
    let saml_user = Caller {
        access_key_id: None,
        principal_key: "unknown:SAMLPRINCIPAL".to_owned(),
        principal_arn: None,
        identity_type: "SAMLUser".to_owned(),
        account_id: None,
        invoked_by: None,
    };
    let provider = format!("arn:aws:iam::{ACCOUNT}:saml-provider/corp");
    let graph = LineageGraph::new([LineageEdge {
        federation: Some(Federation {
            provider: provider.clone(),
            subject: "bob@example.com".to_owned(),
        }),
        ..edge("ASIA1", saml_user, "sso", "evt-saml")
    }]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "sso"));

    assert_eq!(
        resolution.origin,
        origin(
            OriginKind::Federated,
            &format!("federated:{provider}:bob@example.com")
        )
    );
}

fn lambda() -> Caller {
    Caller {
        access_key_id: None,
        principal_key: "service:lambda.amazonaws.com".to_owned(),
        principal_arn: None,
        identity_type: "AWSService".to_owned(),
        account_id: None,
        invoked_by: Some("lambda.amazonaws.com".to_owned()),
    }
}

#[test]
fn a_session_assumed_by_an_aws_service_resolves_to_a_workload() {
    let graph = LineageGraph::new([edge("ASIA1", lambda(), "worker", "evt-lambda")]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "worker"));

    assert_eq!(
        resolution.origin,
        origin(
            OriginKind::Workload,
            &format!(
                "workload:lambda.amazonaws.com:{}",
                role_arn(ACCOUNT, "worker")
            )
        )
    );
}

#[test]
fn sessions_assumed_by_an_aws_service_for_the_same_role_share_one_workload_origin() {
    let graph = LineageGraph::new([
        edge("ASIA1", lambda(), "worker", "evt-lambda-1"),
        edge("ASIA2", lambda(), "worker", "evt-lambda-2"),
        edge("ASIA3", lambda(), "worker", "evt-lambda-3"),
    ]);
    let principal_key = role_arn(ACCOUNT, "worker");

    let resolved = graph.resolve_signals(vec![
        signal("2026-10-01T10:00:00Z", Some("ASIA1"), &principal_key),
        signal("2026-10-01T11:00:00Z", Some("ASIA2"), &principal_key),
        signal("2026-10-01T12:00:00Z", Some("ASIA3"), &principal_key),
    ]);

    assert_eq!(
        resolved.origins.len(),
        1,
        "unexpected origins: {resolved:?}"
    );
    assert_eq!(resolved.origins[0].origin.kind, OriginKind::Workload);
    assert_eq!(resolved.origins[0].signals.len(), 3);
    assert_eq!(
        resolved.origins[0].lineage,
        vec![
            step("ASIA1", "worker", "evt-lambda-1"),
            step("ASIA2", "worker", "evt-lambda-2"),
            step("ASIA3", "worker", "evt-lambda-3"),
        ]
    );
}

#[test]
fn a_long_term_key_resolves_to_the_iam_user_through_the_principal_key() {
    let graph = LineageGraph::new([edge("ASIA1", iam_user(), "admin", "evt-assume")]);

    let resolution = graph.resolve(Some(ALICE_KEY), ALICE_ARN);

    assert_eq!(
        resolution,
        Resolution {
            origin: origin(OriginKind::IamUser, &format!("iam_user:{ALICE_ARN}")),
            lineage: Vec::new(),
            warning: None,
        }
    );
}

#[test]
fn a_console_session_resolves_to_the_iam_user_through_the_principal_key() {
    let graph = LineageGraph::new([]);

    let resolution = graph.resolve(Some("ASIACONSOLE"), ALICE_ARN);

    assert_eq!(
        resolution.origin,
        origin(OriginKind::IamUser, &format!("iam_user:{ALICE_ARN}"))
    );
}

#[test]
fn a_console_session_and_an_assumed_role_of_the_same_user_share_one_origin() {
    let graph = LineageGraph::new([edge("ASIA1", iam_user(), "admin", "evt-assume")]);

    let resolved = graph.resolve_signals(vec![
        signal(
            "2026-10-01T10:05:00Z",
            Some("ASIA1"),
            &role_arn(ACCOUNT, "admin"),
        ),
        signal("2026-10-01T10:00:00Z", None, ALICE_ARN),
    ]);

    assert_eq!(
        resolved.origins.len(),
        1,
        "unexpected origins: {resolved:?}"
    );
    let start_times: Vec<String> = resolved.origins[0]
        .signals
        .iter()
        .map(|signal| signal.start_time.to_rfc3339())
        .collect();
    assert_eq!(
        start_times,
        ["2026-10-01T10:00:00+00:00", "2026-10-01T10:05:00+00:00"]
    );
}

#[test]
fn a_root_principal_key_resolves_to_the_root_origin() {
    let graph = LineageGraph::new([]);

    let resolution = graph.resolve(None, &format!("arn:aws:iam::{ACCOUNT}:root"));

    assert_eq!(
        resolution.origin,
        origin(OriginKind::Root, &format!("root:{ACCOUNT}"))
    );
}

#[test]
fn an_aws_service_principal_key_resolves_to_a_workload() {
    let graph = LineageGraph::new([]);

    let resolution = graph.resolve(None, "service:cloudtrail.amazonaws.com");

    assert_eq!(
        resolution.origin,
        origin(OriginKind::Workload, "workload:cloudtrail.amazonaws.com")
    );
}

#[test]
fn an_assumed_role_session_outside_the_lineage_resolves_to_an_unresolved_role() {
    let graph = LineageGraph::new([]);
    let role = role_arn(ACCOUNT, "admin");

    let resolution = graph.resolve(Some("ASIAOLD"), &role);

    assert_eq!(
        resolution.origin,
        origin(OriginKind::UnresolvedRole, &format!("role:{role}"))
    );
}

#[test]
fn a_session_issued_by_an_assumed_role_outside_the_lineage_resolves_to_that_role() {
    let parent = assumed_role(ACCOUNT, "admin", "ASIAOLD");
    let graph = LineageGraph::new([edge("ASIA1", parent, "deploy", "evt-chain")]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "deploy"));

    assert_eq!(
        resolution.origin,
        origin(
            OriginKind::UnresolvedRole,
            &format!("role:{}", role_arn(ACCOUNT, "admin"))
        )
    );
    assert_eq!(
        resolution.lineage,
        vec![step("ASIA1", "deploy", "evt-chain")]
    );
}

#[test]
fn an_unrecognized_principal_key_resolves_to_an_unknown_origin() {
    let graph = LineageGraph::new([]);

    let resolution = graph.resolve(None, "unknown:SOMEPRINCIPAL");

    assert_eq!(
        resolution.origin,
        origin(OriginKind::Unknown, "unknown:unknown:SOMEPRINCIPAL")
    );
}

#[test]
fn a_three_level_role_chain_is_traced_back_to_the_iam_user() {
    let graph = LineageGraph::new([
        edge("ASIA1", iam_user(), "first", "evt-1"),
        edge(
            "ASIA2",
            assumed_role(ACCOUNT, "first", "ASIA1"),
            "second",
            "evt-2",
        ),
        edge(
            "ASIA3",
            assumed_role(ACCOUNT, "second", "ASIA2"),
            "third",
            "evt-3",
        ),
    ]);

    let resolution = graph.resolve(Some("ASIA3"), &role_arn(ACCOUNT, "third"));

    assert_eq!(
        resolution,
        Resolution {
            origin: origin(OriginKind::IamUser, &format!("iam_user:{ALICE_ARN}")),
            lineage: vec![
                step("ASIA1", "first", "evt-1"),
                step("ASIA2", "second", "evt-2"),
                step("ASIA3", "third", "evt-3"),
            ],
            warning: None,
        }
    );
}

#[test]
fn a_cycle_stops_at_the_topmost_node_with_a_warning() {
    let graph = LineageGraph::new([
        edge(
            "ASIA1",
            assumed_role(ACCOUNT, "second", "ASIA2"),
            "first",
            "evt-1",
        ),
        edge(
            "ASIA2",
            assumed_role(ACCOUNT, "first", "ASIA1"),
            "second",
            "evt-2",
        ),
    ]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "first"));

    assert_eq!(
        resolution,
        Resolution {
            origin: origin(
                OriginKind::UnresolvedRole,
                &format!("role:{}", role_arn(ACCOUNT, "first"))
            ),
            lineage: vec![
                step("ASIA2", "second", "evt-2"),
                step("ASIA1", "first", "evt-1"),
            ],
            warning: Some(LineageWarning::Cycle {
                access_key_id: "ASIA1".to_owned()
            }),
        }
    );
}

fn chain(length: usize) -> Vec<LineageEdge> {
    (1..=length)
        .map(|level| {
            let parent = if level == 1 {
                iam_user()
            } else {
                assumed_role(
                    ACCOUNT,
                    &format!("role{}", level - 1),
                    &format!("ASIA{}", level - 1),
                )
            };
            edge(
                &format!("ASIA{level}"),
                parent,
                &format!("role{level}"),
                &format!("evt-{level}"),
            )
        })
        .collect()
}

#[test]
fn a_chain_as_deep_as_the_default_limit_is_traced_to_the_end() {
    let graph = LineageGraph::new(chain(DEFAULT_MAX_DEPTH));

    let resolution = graph.resolve(
        Some(&format!("ASIA{DEFAULT_MAX_DEPTH}")),
        &role_arn(ACCOUNT, "role16"),
    );

    assert_eq!(resolution.origin.kind, OriginKind::IamUser);
    assert_eq!(resolution.lineage.len(), DEFAULT_MAX_DEPTH);
    assert_eq!(resolution.warning, None);
}

#[test]
fn a_chain_deeper_than_the_limit_stops_at_the_limit_with_a_warning() {
    let graph = LineageGraph::new(chain(20));

    let resolution = graph.resolve(Some("ASIA20"), &role_arn(ACCOUNT, "role20"));

    assert_eq!(resolution.lineage.len(), DEFAULT_MAX_DEPTH);
    assert_eq!(resolution.lineage[0].access_key_id, "ASIA5");
    assert_eq!(
        resolution.origin,
        origin(
            OriginKind::UnresolvedRole,
            &format!("role:{}", role_arn(ACCOUNT, "role4"))
        )
    );
    assert_eq!(
        resolution.warning,
        Some(LineageWarning::DepthLimit {
            access_key_id: "ASIA5".to_owned(),
            max_depth: DEFAULT_MAX_DEPTH,
        })
    );
}

#[test]
fn the_depth_limit_can_be_changed() {
    let graph = LineageGraph::new(chain(3)).with_max_depth(2);

    let resolution = graph.resolve(Some("ASIA3"), &role_arn(ACCOUNT, "role3"));

    assert_eq!(
        resolution.lineage,
        vec![
            step("ASIA2", "role2", "evt-2"),
            step("ASIA3", "role3", "evt-3")
        ]
    );
    assert_eq!(
        resolution.warning,
        Some(LineageWarning::DepthLimit {
            access_key_id: "ASIA2".to_owned(),
            max_depth: 2,
        })
    );
}

#[test]
fn edges_with_the_same_child_access_key_are_merged_into_one() {
    let graph = LineageGraph::new([
        edge("ASIA1", iam_user(), "admin", "evt-caller-account"),
        edge("ASIA1", iam_user(), "admin", "evt-role-account"),
    ]);

    let resolution = graph.resolve(Some("ASIA1"), &role_arn(ACCOUNT, "admin"));

    assert_eq!(graph.len(), 1);
    assert_eq!(
        resolution.lineage,
        vec![step("ASIA1", "admin", "evt-caller-account")]
    );
}

#[test]
fn resolve_signals_groups_signals_by_origin_and_collects_warnings() {
    let graph = LineageGraph::new([
        edge("ASIA1", iam_user(), "admin", "evt-1"),
        edge(
            "ASIACYCLE1",
            assumed_role(ACCOUNT, "b", "ASIACYCLE2"),
            "a",
            "evt-c1",
        ),
        edge(
            "ASIACYCLE2",
            assumed_role(ACCOUNT, "a", "ASIACYCLE1"),
            "b",
            "evt-c2",
        ),
    ]);

    let resolved = graph.resolve_signals(vec![
        signal(
            "2026-10-01T10:00:00Z",
            Some("ASIA1"),
            &role_arn(ACCOUNT, "admin"),
        ),
        signal(
            "2026-10-01T10:01:00Z",
            Some("ASIACYCLE1"),
            &role_arn(ACCOUNT, "a"),
        ),
        signal(
            "2026-10-01T10:02:00Z",
            Some("ASIACYCLE1"),
            &role_arn(ACCOUNT, "a"),
        ),
    ]);

    let origin_ids: Vec<&str> = resolved
        .origins
        .iter()
        .map(|resolved| resolved.origin.id.as_str())
        .collect();
    assert_eq!(
        origin_ids,
        [
            format!("iam_user:{ALICE_ARN}"),
            format!("role:{}", role_arn(ACCOUNT, "a")),
        ]
    );
    assert_eq!(
        resolved.warnings,
        vec![LineageWarning::Cycle {
            access_key_id: "ASIACYCLE1".to_owned()
        }]
    );
}

#[test]
fn origin_kinds_are_written_in_snake_case() {
    for (kind, text) in [
        (OriginKind::IamUser, "iam_user"),
        (OriginKind::Root, "root"),
        (OriginKind::ExternalPrincipal, "external_principal"),
        (OriginKind::Federated, "federated"),
        (OriginKind::Workload, "workload"),
        (OriginKind::UnresolvedRole, "unresolved_role"),
        (OriginKind::Unknown, "unknown"),
    ] {
        assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{text}\""));
    }
}
