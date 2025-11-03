use crate::SableError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerRole {
    #[default]
    Primary,
    Replica,
}

impl FromStr for ServerRole {
    type Err = SableError;
    fn from_str(s: &str) -> Result<Self, SableError> {
        match s.to_lowercase().as_str() {
            "replica" => Ok(ServerRole::Replica),
            _ => Ok(ServerRole::Primary),
        }
    }
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ServerRole::Primary => write!(f, "primary"),
            ServerRole::Replica => write!(f, "replica"),
        }
    }
}
