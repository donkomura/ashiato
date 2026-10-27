use std::fs::{self, File};
use std::io::Write;

use ashiato_core::AnalysisWindow;
use ashiato_core::lineage::{Caller, Federation, LineageEdge};
use ashiato_core::query::QueryBackend;
use ashiato_duckdb::{DuckDbBackend, EventStore};
use ashiato_testkit::record::RecordBuilder;
use chrono::TimeDelta;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

const LOG_DIR: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01";
const ACCOUNT: &str = "111111111111";
const OTHER_ACCOUNT: &str = "222222222222";
const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";

async fn query_lineage(records: &[Value]) -> Vec<LineageEdge> {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    let log = source.join(LOG_DIR).join("log.json.gz");
    fs::create_dir_all(log.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(&log).unwrap(), Compression::default());
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();

    let db_path = dir.path().join("ashiato.duckdb");
    EventStore::open(&db_path)
        .unwrap()
        .ingest_directory(&source)
        .unwrap();
    let backend = DuckDbBackend::open(&db_path).unwrap();
    backend.query_lineage(&window()).await.unwrap()
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

fn role_arn(account: &str, name: &str) -> String {
    format!("arn:aws:iam::{account}:role/{name}")
}

fn credentials(access_key_id: &str) -> Value {
    json!({
        "accessKeyId": access_key_id,
        "sessionToken": "token",
        "expiration": "Oct 1, 2026, 11:00:00 AM",
    })
}

fn sts_call(event_id: &str, event_name: &str) -> RecordBuilder {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("eventTime", "2026-10-01T10:00:00Z")
        .set("eventSource", "sts.amazonaws.com")
        .set("eventName", event_name)
}

fn assume_role(event_id: &str, role: &str, child: &str) -> RecordBuilder {
    sts_call(event_id, "AssumeRole")
        .set(
            "requestParameters",
            json!({ "roleArn": role, "roleSessionName": "session" }),
        )
        .set(
            "responseElements",
            json!({
                "credentials": credentials(child),
                "assumedRoleUser": { "arn": format!("{role}/session") },
            }),
        )
}

fn alice() -> Caller {
    Caller {
        access_key_id: Some("AKIAEXAMPLEACCESSKEY".to_owned()),
        principal_key: ALICE_ARN.to_owned(),
        principal_arn: Some(ALICE_ARN.to_owned()),
        identity_type: "IAMUser".to_owned(),
        account_id: Some(ACCOUNT.to_owned()),
        invoked_by: None,
    }
}

#[tokio::test]
async fn assume_role_yields_an_edge_from_the_caller_to_the_issued_key() {
    let role = role_arn(ACCOUNT, "admin");

    let edges = query_lineage(&[assume_role("evt-assume", &role, "ASIA1").build()]).await;

    assert_eq!(
        edges,
        vec![LineageEdge {
            child_access_key_id: "ASIA1".to_owned(),
            parent: alice(),
            role_arn: Some(role),
            issued_by_event_id: "evt-assume".to_owned(),
            cross_account: false,
            federation: None,
        }]
    );
}

#[tokio::test]
async fn assume_role_by_an_assumed_role_records_the_parent_session() {
    let role = role_arn(ACCOUNT, "deploy");
    let parent_role = role_arn(ACCOUNT, "admin");
    let record = assume_role("evt-chain", &role, "ASIA2")
        .set("userIdentity.type", "AssumedRole")
        .set(
            "userIdentity.arn",
            format!("arn:aws:sts::{ACCOUNT}:assumed-role/admin/session"),
        )
        .set("userIdentity.accessKeyId", "ASIA1")
        .set(
            "userIdentity.sessionContext.sessionIssuer.arn",
            parent_role.as_str(),
        )
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].parent.access_key_id.as_deref(), Some("ASIA1"));
    assert_eq!(edges[0].parent.principal_key, parent_role);
    assert_eq!(edges[0].parent.identity_type, "AssumedRole");
}

#[tokio::test]
async fn assume_role_with_saml_yields_an_edge_with_the_provider_and_subject() {
    let role = role_arn(ACCOUNT, "sso");
    let provider = format!("arn:aws:iam::{ACCOUNT}:saml-provider/corp");
    let record = sts_call("evt-saml", "AssumeRoleWithSAML")
        .set(
            "userIdentity",
            json!({
                "type": "SAMLUser",
                "principalId": "corp:bob@example.com",
                "userName": "bob@example.com",
                "identityProvider": provider,
            }),
        )
        .set("userIdentity.arn", Value::Null)
        .set("userIdentity.accessKeyId", Value::Null)
        .set("userIdentity.accountId", Value::Null)
        .set(
            "requestParameters",
            json!({ "roleArn": role, "principalArn": provider }),
        )
        .set(
            "responseElements",
            json!({
                "credentials": credentials("ASIASAML"),
                "subject": "bob@example.com",
                "issuer": "https://idp.example.com/",
            }),
        )
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].child_access_key_id, "ASIASAML");
    assert_eq!(edges[0].parent.identity_type, "SAMLUser");
    assert_eq!(edges[0].parent.access_key_id, None);
    assert_eq!(edges[0].role_arn.as_deref(), Some(role.as_str()));
    assert_eq!(
        edges[0].federation,
        Some(Federation {
            provider,
            subject: "bob@example.com".to_owned(),
        })
    );
}

#[tokio::test]
async fn assume_role_with_web_identity_yields_an_edge_with_the_provider_and_subject() {
    let role = role_arn(ACCOUNT, "ci");
    let provider =
        format!("arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com");
    let record = sts_call("evt-oidc", "AssumeRoleWithWebIdentity")
        .set(
            "userIdentity",
            json!({
                "type": "WebIdentityUser",
                "principalId": "token.actions.githubusercontent.com:sts.amazonaws.com:repo:org/app",
                "userName": "repo:org/app",
                "identityProvider": "token.actions.githubusercontent.com",
            }),
        )
        .set("userIdentity.arn", Value::Null)
        .set("userIdentity.accessKeyId", Value::Null)
        .set("userIdentity.accountId", Value::Null)
        .set(
            "requestParameters",
            json!({ "roleArn": role, "roleSessionName": "gha" }),
        )
        .set(
            "responseElements",
            json!({
                "credentials": credentials("ASIAOIDC"),
                "provider": provider,
                "subjectFromWebIdentityToken": "repo:org/app:ref:refs/heads/main",
            }),
        )
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].child_access_key_id, "ASIAOIDC");
    assert_eq!(
        edges[0].federation,
        Some(Federation {
            provider,
            subject: "repo:org/app:ref:refs/heads/main".to_owned(),
        })
    );
}

#[tokio::test]
async fn get_session_token_yields_an_edge_without_a_role() {
    let record = sts_call("evt-session-token", "GetSessionToken")
        .set("requestParameters", json!({ "durationSeconds": 3600 }))
        .set(
            "responseElements",
            json!({ "credentials": credentials("ASIASESSION") }),
        )
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(
        edges,
        vec![LineageEdge {
            child_access_key_id: "ASIASESSION".to_owned(),
            parent: alice(),
            role_arn: None,
            issued_by_event_id: "evt-session-token".to_owned(),
            cross_account: false,
            federation: None,
        }]
    );
}

#[tokio::test]
async fn get_federation_token_yields_an_edge_without_a_role() {
    let record = sts_call("evt-federation-token", "GetFederationToken")
        .set("requestParameters", json!({ "name": "carol" }))
        .set(
            "responseElements",
            json!({
                "credentials": credentials("ASIAFEDERATED"),
                "federatedUser": {
                    "arn": format!("arn:aws:sts::{ACCOUNT}:federated-user/carol"),
                    "federatedUserId": format!("{ACCOUNT}:carol"),
                },
            }),
        )
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].child_access_key_id, "ASIAFEDERATED");
    assert_eq!(edges[0].parent, alice());
    assert_eq!(edges[0].role_arn, None);
}

#[tokio::test]
async fn failed_events_and_other_apis_yield_no_edges() {
    let role = role_arn(ACCOUNT, "admin");
    let denied = sts_call("evt-denied", "AssumeRole")
        .set("errorCode", "AccessDenied")
        .set("errorMessage", "not authorized")
        .set("requestParameters", json!({ "roleArn": role }))
        .build();
    let failed_with_response = assume_role("evt-failed", &role, "ASIAFAILED")
        .set("errorCode", "RegionDisabledException")
        .build();
    let other_api = RecordBuilder::new()
        .set("eventID", "evt-other")
        .set("eventTime", "2026-10-01T10:00:00Z")
        .set("eventSource", "iam.amazonaws.com")
        .set("eventName", "CreateAccessKey")
        .set(
            "responseElements",
            json!({ "accessKey": { "accessKeyId": "AKIANEW" }, "credentials": credentials("ASIAIAM") }),
        )
        .build();

    let edges = query_lineage(&[denied, failed_with_response, other_api]).await;

    assert_eq!(edges, Vec::new());
}

#[tokio::test]
async fn a_cross_account_assume_role_recorded_in_both_accounts_yields_one_edge() {
    let role = role_arn(ACCOUNT, "shared");
    let partner_role = role_arn(OTHER_ACCOUNT, "partner");
    let in_account = |event_id: &str, recipient: &str| {
        assume_role(event_id, &role, "ASIASHARED")
            .set("recipientAccountId", recipient)
            .set("userIdentity.type", "AssumedRole")
            .set(
                "userIdentity.arn",
                format!("arn:aws:sts::{OTHER_ACCOUNT}:assumed-role/partner/session"),
            )
            .set("userIdentity.accountId", OTHER_ACCOUNT)
            .set("userIdentity.accessKeyId", "ASIAPARTNER")
            .set(
                "userIdentity.sessionContext.sessionIssuer.arn",
                partner_role.as_str(),
            )
            .build()
    };

    let edges = query_lineage(&[
        in_account("evt-in-caller-account", OTHER_ACCOUNT),
        in_account("evt-in-role-account", ACCOUNT),
    ])
    .await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].child_access_key_id, "ASIASHARED");
    assert!(edges[0].cross_account);
    assert_eq!(edges[0].parent.account_id.as_deref(), Some(OTHER_ACCOUNT));
    assert_eq!(edges[0].parent.principal_key, partner_role);
}

#[tokio::test]
async fn a_service_assuming_a_role_is_recorded_with_invoked_by() {
    let role = role_arn(ACCOUNT, "worker");
    let record = assume_role("evt-lambda", &role, "ASIALAMBDA")
        .set(
            "userIdentity",
            json!({ "type": "AWSService", "invokedBy": "lambda.amazonaws.com" }),
        )
        .set("userIdentity.arn", Value::Null)
        .set("userIdentity.accessKeyId", Value::Null)
        .set("userIdentity.accountId", Value::Null)
        .set("userIdentity.principalId", Value::Null)
        .build();

    let edges = query_lineage(&[record]).await;

    assert_eq!(edges.len(), 1, "unexpected edges: {edges:?}");
    assert_eq!(edges[0].parent.identity_type, "AWSService");
    assert_eq!(
        edges[0].parent.principal_key,
        "service:lambda.amazonaws.com"
    );
    assert_eq!(
        edges[0].parent.invoked_by.as_deref(),
        Some("lambda.amazonaws.com")
    );
    assert!(!edges[0].cross_account);
}

#[tokio::test]
async fn edges_are_taken_from_the_baseline_and_analysis_periods_in_time_order() {
    let role = role_arn(ACCOUNT, "admin");
    let at = |event_id: &str, time: &str, child: &str| {
        assume_role(event_id, &role, child)
            .set("eventTime", time)
            .build()
    };

    let edges = query_lineage(&[
        at("evt-before-baseline", "2026-08-31T23:59:59Z", "ASIA0"),
        at("evt-analysis", "2026-10-01T12:00:00Z", "ASIA2"),
        at("evt-baseline", "2026-09-01T00:00:00Z", "ASIA1"),
        at("evt-after-until", "2026-10-02T00:00:00Z", "ASIA3"),
    ])
    .await;

    let children: Vec<&str> = edges
        .iter()
        .map(|edge| edge.child_access_key_id.as_str())
        .collect();
    assert_eq!(children, ["ASIA1", "ASIA2"]);
}
