use std::fmt::{Display, Formatter};
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Why a resource identifier was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidIdentifier {
    /// Identifiers must contain at least one character.
    #[error("resource identifiers cannot be empty")]
    Empty,
    /// Identifiers are bounded so they remain safe in keys, labels, and URLs.
    #[error("resource identifier length {length} exceeds the maximum of {maximum}")]
    TooLong {
        /// Observed identifier length in bytes.
        length: usize,
        /// Maximum accepted identifier length in bytes.
        maximum: usize,
    },
    /// Identifiers must have an alphanumeric boundary.
    #[error("resource identifiers must start and end with an ASCII letter or digit")]
    InvalidBoundary,
    /// A character could not be represented consistently in keys, labels, and URLs.
    #[error("resource identifier contains unsupported character `{character}`")]
    UnsupportedCharacter {
        /// First unsupported character in the identifier.
        character: char,
    },
}

macro_rules! identifier {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Constructs a validated `", stringify!($name), "`.")]
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
                let value = value.into();
                validate(&value)?;
                Ok(Self(value))
            }

            #[doc = concat!("Returns this `", stringify!($name), "` as text.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = InvalidIdentifier;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidIdentifier;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

identifier!(NodeId, "Stable identity of a cluster node.");
identifier!(
    NodeNetworkId,
    "Stable identity of a node's published network configuration."
);
identifier!(ServiceId, "Stable identity of a deployable service.");
identifier!(
    DeploymentId,
    "Stable identity of one immutable service deployment."
);
identifier!(
    AssignmentId,
    "Stable identity of a scheduled workload assignment."
);
identifier!(
    WorkloadId,
    "Stable identity of one runtime-managed workload instance."
);
identifier!(
    ReplicaStateId,
    "Stable identity of a deployment replica's observed state."
);
identifier!(IngressRouteId, "Stable identity of an ingress route.");
identifier!(
    TrafficGenerationId,
    "Stable identity of an ingress traffic generation."
);
identifier!(
    FirewallPolicyId,
    "Stable identity of an effective firewall policy."
);
identifier!(DnsRecordId, "Stable identity of a cluster DNS record.");
identifier!(BuildId, "Stable identity of an artifact build.");
identifier!(PreviewId, "Stable identity of a pull-request preview.");
identifier!(
    UpgradeRunId,
    "Stable identity of a persisted cluster upgrade run."
);
identifier!(WebhookId, "Stable identity of a webhook configuration.");

/// A type-safe reference to the identity of any built-in resource.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", content = "id", rename_all = "camelCase")]
pub enum ResourceId {
    /// A cluster node.
    Node(NodeId),
    /// A node network publication.
    NodeNetwork(NodeNetworkId),
    /// A deployable service.
    Service(ServiceId),
    /// An immutable deployment.
    Deployment(DeploymentId),
    /// A scheduled assignment.
    Assignment(AssignmentId),
    /// An observed replica state.
    ReplicaState(ReplicaStateId),
    /// An ingress route.
    IngressRoute(IngressRouteId),
    /// A traffic generation.
    TrafficGeneration(TrafficGenerationId),
    /// A firewall policy.
    FirewallPolicy(FirewallPolicyId),
    /// A DNS record.
    DnsRecord(DnsRecordId),
    /// An artifact build.
    Build(BuildId),
    /// A pull-request preview.
    Preview(PreviewId),
    /// A cluster upgrade run.
    UpgradeRun(UpgradeRunId),
    /// A webhook configuration.
    Webhook(WebhookId),
}

fn validate(value: &str) -> Result<(), InvalidIdentifier> {
    const MAXIMUM_LENGTH: usize = 253;

    if value.is_empty() {
        Err(InvalidIdentifier::Empty)
    } else if value.len() > MAXIMUM_LENGTH {
        Err(InvalidIdentifier::TooLong {
            length: value.len(),
            maximum: MAXIMUM_LENGTH,
        })
    } else if !value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        || !value
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric())
    {
        Err(InvalidIdentifier::InvalidBoundary)
    } else if let Some(character) = value.chars().find(|character| {
        !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
    }) {
        Err(InvalidIdentifier::UnsupportedCharacter { character })
    } else {
        Ok(())
    }
}
