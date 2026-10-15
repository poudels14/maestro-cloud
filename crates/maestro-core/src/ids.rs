use std::fmt;

use serde::{Deserialize, Serialize};

const SHORT_ID_ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

string_id!(ServiceId);
string_id!(DeploymentId);
string_id!(ReplicaId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn short(self) -> String {
        let alphabet = SHORT_ID_ALPHABET.as_bytes();
        let base = alphabet.len() as u64;
        let digits = std::iter::successors(Some(self.0), |value| {
            (*value >= base).then_some(value / base)
        })
        .map(|value| alphabet[(value % base) as usize] as char)
        .collect::<Vec<_>>();
        digits.into_iter().rev().collect()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl ReplicaId {
    pub fn for_deployment(deployment_id: &DeploymentId, index: u32) -> Self {
        Self(format!("{deployment_id}-{index}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortens_node_ids_with_an_unambiguous_alphabet() {
        assert_eq!(NodeId(0).short(), "0");
        assert_eq!(NodeId(31).short(), "z");
        assert_eq!(NodeId(32).short(), "10");
        assert_eq!(NodeId(1 << 39).short().len(), 8);
        let long = NodeId(2949329139299662).short();
        assert_eq!(long.len(), 11);
        assert!(
            long.chars()
                .all(|character| SHORT_ID_ALPHABET.contains(character))
        );
        assert!(!SHORT_ID_ALPHABET.contains(['i', 'l', 'o', 'u']));
    }
}
