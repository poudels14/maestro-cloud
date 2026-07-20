use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::{
    BootstrapDecision, FixtureNodeName, FormationCluster, FormationMemberRole, FormationSnapshot,
    JoinObservation, LeadershipAgreement, MembershipAgreement, NodePorts, RegistrationCleanup,
    RegistrationObservation, ReservationState, ResourceAvailability,
    scenarios::designated_seed_and_learners_form_registered_cluster,
};

struct FormationWorld {
    nodes: Vec<FixtureNodeName>,
    seed_decision: BootstrapDecision,
    members: BTreeMap<FixtureNodeName, FormationMemberRole>,
    registered: BTreeSet<FixtureNodeName>,
}

impl FormationWorld {
    fn new() -> Self {
        let seed = FixtureNodeName::new("seed");
        let middle = FixtureNodeName::new("middle");
        let later = FixtureNodeName::new("later");
        Self {
            members: BTreeMap::from([(seed.clone(), FormationMemberRole::Voter)]),
            nodes: vec![seed, middle, later],
            seed_decision: BootstrapDecision::BootstrapSeed,
            registered: BTreeSet::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("formation world failed: {0}")]
struct FormationWorldError(&'static str);

#[async_trait]
impl FormationCluster for FormationWorld {
    type Error = FormationWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn seed_decision(&mut self) -> Result<BootstrapDecision, Self::Error> {
        Ok(self.seed_decision)
    }

    async fn mark_seed_starting(&mut self) -> Result<(), Self::Error> {
        self.seed_decision = BootstrapDecision::ResumeSeed;
        Ok(())
    }

    async fn mark_seed_joined(&mut self) -> Result<(), Self::Error> {
        self.seed_decision = BootstrapDecision::Restart;
        Ok(())
    }

    async fn await_seed(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn join_and_promote(
        &mut self,
        node: &FixtureNodeName,
    ) -> Result<JoinObservation, Self::Error> {
        if !self.nodes.contains(node) {
            return Err(FormationWorldError("unknown node"));
        }
        let decision_before_join = BootstrapDecision::Restart;
        self.members
            .insert(node.clone(), FormationMemberRole::Voter);
        Ok(JoinObservation {
            node: node.clone(),
            decision_before_join,
            decision_after_join: BootstrapDecision::JoinExisting,
            initial_members: self.members.keys().cloned().collect(),
            final_role: FormationMemberRole::Voter,
        })
    }

    async fn formation_snapshot(&mut self) -> Result<FormationSnapshot, Self::Error> {
        Ok(FormationSnapshot {
            members: self.members.clone(),
            membership: MembershipAgreement::Consistent,
            leadership: LeadershipAgreement::Consistent,
            writes: ResourceAvailability::Available,
        })
    }

    async fn register_seed(&mut self) -> Result<RegistrationObservation, Self::Error> {
        let seed = self
            .nodes
            .first()
            .cloned()
            .ok_or(FormationWorldError("missing seed"))?;
        self.registered.insert(seed.clone());
        let ports = NodePorts {
            api: 3000,
            gateway: 3002,
            consensus_client: 2379,
            consensus_peer: 2380,
        };
        Ok(RegistrationObservation {
            registered_nodes: self.registered.clone(),
            image_holders: self.registered.clone(),
            reservation_node: seed,
            reservation_state: ReservationState::Active,
            advertised_ports: ports,
            reserved_ports: ports,
        })
    }

    async fn deregister_seed(&mut self) -> Result<RegistrationCleanup, Self::Error> {
        self.registered.clear();
        Ok(RegistrationCleanup {
            registered_nodes: BTreeSet::new(),
            image_holders: BTreeSet::new(),
        })
    }
}

#[tokio::test]
async fn formation_scenario_promotes_voters_and_cleans_leases() {
    let mut cluster = FormationWorld::new();

    designated_seed_and_learners_form_registered_cluster(&mut cluster)
        .await
        .expect("designated-seed formation scenario");
}
