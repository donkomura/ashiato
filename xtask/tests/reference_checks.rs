use ashiato_testkit::dataset::{ExpectedOrigin, ExpectedScenario};
use chrono::{TimeDelta, TimeZone, Utc};
use xtask::reference::{
    RuleDifference, SCENARIO_CHECK, SIGNAL_CHECK, ScenarioComparison, SignalComparison,
    scenario_check, signal_check,
};
use xtask::report::ReferenceCheck;

fn scenario(id: &str) -> ExpectedScenario {
    let start = Utc.with_ymd_and_hms(2026, 9, 9, 10, 0, 0).unwrap();
    ExpectedScenario {
        scenario_id: id.to_owned(),
        origin: ExpectedOrigin {
            id: "ASIA0000000000000001".to_owned(),
            kind: "access_key".to_owned(),
        },
        start_time: start,
        end_time: start + TimeDelta::minutes(30),
    }
}

fn check(name: &str, passed: bool, detail: &str) -> ReferenceCheck {
    ReferenceCheck {
        name: name.to_owned(),
        passed,
        detail: detail.to_owned(),
    }
}

#[test]
fn the_names_of_the_checks_follow_14_1_3() {
    assert_eq!(SCENARIO_CHECK, "scenario_match が正解の一覧と一致する");
    assert_eq!(SIGNAL_CHECK, "signals がバッチ処理と一致する");
}

#[test]
fn a_scenario_check_names_the_missing_and_the_unexpected_scenarios() {
    let cases = [
        (
            Ok(ScenarioComparison {
                missing: vec![],
                unexpected: vec![],
            }),
            check(SCENARIO_CHECK, true, "正解の一覧と一致した"),
        ),
        (
            Ok(ScenarioComparison {
                missing: vec![scenario("role_chain_escalation")],
                unexpected: vec![],
            }),
            check(
                SCENARIO_CHECK,
                false,
                "出力にない正解が 1 件（role_chain_escalation）",
            ),
        ),
        (
            Ok(ScenarioComparison {
                missing: vec![
                    scenario("role_chain_escalation"),
                    scenario("console_takeover_mining"),
                ],
                unexpected: vec![scenario("cross_account_s3_destruction")],
            }),
            check(
                SCENARIO_CHECK,
                false,
                "出力にない正解が 2 件（role_chain_escalation、console_takeover_mining）、\
                 正解にない出力が 1 件（cross_account_s3_destruction）",
            ),
        ),
        (
            Err("expected value at line 1 column 1".to_owned()),
            check(
                SCENARIO_CHECK,
                false,
                "エラー: expected value at line 1 column 1",
            ),
        ),
    ];
    for (compared, expected) in cases {
        assert_eq!(scenario_check(compared), expected);
    }
}

#[test]
fn a_signal_check_counts_the_differences_by_rule() {
    let cases = [
        (
            Ok(SignalComparison {
                batch_signals: 12,
                differences: vec![],
            }),
            check(SIGNAL_CHECK, true, "12 件の兆候が一致した"),
        ),
        (
            Ok(SignalComparison {
                batch_signals: 0,
                differences: vec![],
            }),
            check(SIGNAL_CHECK, false, "バッチ処理の兆候が 0 件だった"),
        ),
        (
            Ok(SignalComparison {
                batch_signals: 12,
                differences: vec![
                    RuleDifference {
                        rule_id: "console_login_without_mfa".to_owned(),
                        only_in_reference: 1,
                        only_in_batch: 0,
                    },
                    RuleDifference {
                        rule_id: "s3_mass_delete".to_owned(),
                        only_in_reference: 0,
                        only_in_batch: 2,
                    },
                ],
            }),
            check(
                SIGNAL_CHECK,
                false,
                "console_login_without_mfa（R0 にだけある行 1、バッチ処理にだけある行 0）、\
                 s3_mass_delete（R0 にだけある行 0、バッチ処理にだけある行 2）",
            ),
        ),
        (
            Err("failed to read dataset.json".to_owned()),
            check(SIGNAL_CHECK, false, "エラー: failed to read dataset.json"),
        ),
    ];
    for (compared, expected) in cases {
        assert_eq!(signal_check(compared), expected);
    }
}
