use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Tactic {
    #[serde(rename = "TA0043")]
    Reconnaissance,
    #[serde(rename = "TA0042")]
    ResourceDevelopment,
    #[serde(rename = "TA0001")]
    InitialAccess,
    #[serde(rename = "TA0002")]
    Execution,
    #[serde(rename = "TA0003")]
    Persistence,
    #[serde(rename = "TA0004")]
    PrivilegeEscalation,
    #[serde(rename = "TA0005")]
    DefenseEvasion,
    #[serde(rename = "TA0006")]
    CredentialAccess,
    #[serde(rename = "TA0007")]
    Discovery,
    #[serde(rename = "TA0008")]
    LateralMovement,
    #[serde(rename = "TA0009")]
    Collection,
    #[serde(rename = "TA0011")]
    CommandAndControl,
    #[serde(rename = "TA0010")]
    Exfiltration,
    #[serde(rename = "TA0040")]
    Impact,
}
