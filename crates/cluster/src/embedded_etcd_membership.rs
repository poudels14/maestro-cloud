use etcd_client::{Client, GetOptions, Member, MemberAddOptions};

use crate::{
    EmbeddedEtcdSettings, MemberActivation, MemberState, StoreJoinTicket, StoreMember,
    StoreProviderConfig, StoreProviderError,
    embedded_etcd_plan::{
        EtcdJoinTicketData, EtcdMemberPlan, TICKET_FORMAT_VERSION, client_url, peer_url,
    },
    embedded_etcd_process::connect_options,
};

pub(crate) async fn stage_member(
    config: &StoreProviderConfig,
    settings: EmbeddedEtcdSettings,
    member: StoreMember,
) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
    validate_stage_target(config, &member)?;
    let expected_peer_url = peer_url(config, member.host_address);
    let mut client = connect_admin(config, settings).await?;
    let listed = client.member_list().await.map_err(unavailable)?;
    if let Some(existing) = member_by_peer(listed.members(), &expected_peer_url) {
        let state = if existing.is_learner() {
            MemberState::Staged
        } else {
            MemberState::Active
        };
        let ticket = ticket_from_members(config, &member.node_id, existing.id(), listed.members())?;
        return Ok((
            ticket,
            MemberActivation {
                node_id: member.node_id,
                state,
            },
        ));
    }

    let response = client
        .member_add(
            [expected_peer_url],
            Some(MemberAddOptions::new().with_is_learner()),
        )
        .await
        .map_err(membership_error)?;
    let added = response
        .member()
        .ok_or_else(|| StoreProviderError::Unavailable {
            reason: "membership response omitted the staged member".to_owned(),
        })?;
    let ticket = ticket_from_members(config, &member.node_id, added.id(), response.member_list())?;
    Ok((
        ticket,
        MemberActivation {
            node_id: member.node_id,
            state: MemberState::Staged,
        },
    ))
}

pub(crate) async fn activate_member(
    config: &StoreProviderConfig,
    settings: EmbeddedEtcdSettings,
    ticket: &StoreJoinTicket,
) -> Result<MemberActivation, StoreProviderError> {
    let ticket_data = validate_membership_ticket(config, ticket)?;
    let mut client = connect_admin(config, settings).await?;
    let listed = client.member_list().await.map_err(unavailable)?;
    let member = listed
        .members()
        .iter()
        .find(|member| member.id() == ticket_data.member_id)
        .ok_or_else(|| StoreProviderError::MembershipConflict {
            reason: format!(
                "staged membership for node `{}` disappeared",
                ticket_data.node_id
            ),
        })?;
    let expected = config
        .known_members()
        .get(&ticket_data.node_id)
        .ok_or(StoreProviderError::InvalidJoinTicket)?;
    if !member
        .peer_urls()
        .contains(&peer_url(config, expected.host_address))
    {
        return Err(StoreProviderError::MembershipConflict {
            reason: format!(
                "membership for node `{}` changed peer address",
                ticket_data.node_id
            ),
        });
    }
    if member.is_learner() && client.member_promote(ticket_data.member_id).await.is_err() {
        return Err(StoreProviderError::MemberNotReady {
            node_id: ticket_data.node_id,
        });
    }
    if client
        .get(
            "/maestro/system/membership-readiness",
            Some(GetOptions::new().with_limit(1)),
        )
        .await
        .is_err()
    {
        return Err(StoreProviderError::MemberNotReady {
            node_id: ticket_data.node_id,
        });
    }
    Ok(MemberActivation {
        node_id: ticket_data.node_id,
        state: MemberState::Active,
    })
}

pub(crate) async fn remove_member(
    config: &StoreProviderConfig,
    settings: EmbeddedEtcdSettings,
    node_id: &kernel_api::NodeId,
) -> Result<(), StoreProviderError> {
    if node_id == &config.local_member().node_id {
        return Err(StoreProviderError::MembershipConflict {
            reason: "the local member cannot remove itself".to_owned(),
        });
    }
    let Some(target) = config.known_members().get(node_id) else {
        return Ok(());
    };
    let expected_peer_url = peer_url(config, target.host_address);
    let mut client = connect_admin(config, settings).await?;
    let listed = client.member_list().await.map_err(unavailable)?;
    let Some(member) = member_by_peer(listed.members(), &expected_peer_url) else {
        return Ok(());
    };
    let active_count = listed
        .members()
        .iter()
        .filter(|member| !member.is_learner())
        .count();
    if !member.is_learner() && active_count <= 1 {
        return Err(StoreProviderError::MembershipConflict {
            reason: "refusing to remove the last active store member".to_owned(),
        });
    }
    client
        .member_remove(member.id())
        .await
        .map_err(membership_error)?;
    Ok(())
}

async fn connect_admin(
    config: &StoreProviderConfig,
    settings: EmbeddedEtcdSettings,
) -> Result<Client, StoreProviderError> {
    let endpoints = config
        .known_members()
        .values()
        .map(|member| client_url(config, member.host_address))
        .collect::<Vec<_>>();
    let mut last_error = None;
    for endpoint in endpoints {
        let connected = Client::connect(
            [endpoint],
            Some(connect_options(config, settings.operation_timeout())),
        )
        .await;
        match connected {
            Ok(mut client) => match client.status().await {
                Ok(_) => return Ok(client),
                Err(error) => last_error = Some(error.to_string()),
            },
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    Err(StoreProviderError::Unavailable {
        reason: last_error.unwrap_or_else(|| "no store member endpoints were declared".to_owned()),
    })
}

fn validate_stage_target(
    config: &StoreProviderConfig,
    member: &StoreMember,
) -> Result<(), StoreProviderError> {
    if member.node_id == config.local_member().node_id {
        return Err(StoreProviderError::MembershipConflict {
            reason: "the local member is already initialized".to_owned(),
        });
    }
    if config.known_members().get(&member.node_id) != Some(member) {
        return Err(StoreProviderError::MembershipConflict {
            reason: format!(
                "node `{}` is absent from declared membership",
                member.node_id
            ),
        });
    }
    Ok(())
}

fn validate_membership_ticket(
    config: &StoreProviderConfig,
    ticket: &StoreJoinTicket,
) -> Result<EtcdJoinTicketData, StoreProviderError> {
    let bound_node = ticket.node_id().clone();
    let ticket_data = EtcdJoinTicketData::decode(ticket)?;
    if ticket_data.format_version != TICKET_FORMAT_VERSION
        || ticket_data.cluster_id != *config.cluster_id()
        || ticket_data.node_id != bound_node
        || ticket_data.member_id == 0
        || !config.known_members().contains_key(&ticket_data.node_id)
    {
        return Err(StoreProviderError::InvalidJoinTicket);
    }
    Ok(ticket_data)
}

fn ticket_from_members(
    config: &StoreProviderConfig,
    node_id: &kernel_api::NodeId,
    member_id: u64,
    members: &[Member],
) -> Result<StoreJoinTicket, StoreProviderError> {
    let mut plans = members
        .iter()
        .map(|member| member_plan(config, member))
        .collect::<Result<Vec<_>, _>>()?;
    plans.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    EtcdJoinTicketData {
        format_version: TICKET_FORMAT_VERSION,
        cluster_id: config.cluster_id().clone(),
        node_id: node_id.clone(),
        member_id,
        members: plans,
    }
    .encode()
}

fn member_plan(
    config: &StoreProviderConfig,
    member: &Member,
) -> Result<EtcdMemberPlan, StoreProviderError> {
    let [member_peer_url] = member.peer_urls() else {
        return Err(StoreProviderError::MembershipConflict {
            reason: format!(
                "store member {} does not have exactly one peer address",
                member.id()
            ),
        });
    };
    let known = config
        .known_members()
        .values()
        .find(|known| peer_url(config, known.host_address) == *member_peer_url)
        .ok_or_else(|| StoreProviderError::MembershipConflict {
            reason: format!("store member {} has an unknown peer address", member.id()),
        })?;
    Ok(EtcdMemberPlan {
        node_id: known.node_id.clone(),
        peer_url: member_peer_url.clone(),
    })
}

fn member_by_peer<'a>(members: &'a [Member], expected_peer_url: &str) -> Option<&'a Member> {
    members.iter().find(|member| {
        member
            .peer_urls()
            .iter()
            .any(|url| url == expected_peer_url)
    })
}

fn unavailable(error: etcd_client::Error) -> StoreProviderError {
    StoreProviderError::Unavailable {
        reason: error.to_string(),
    }
}

fn membership_error(error: etcd_client::Error) -> StoreProviderError {
    match &error {
        etcd_client::Error::GRpcStatus(status) if status.code() == tonic::Code::Unavailable => {
            StoreProviderError::Unavailable {
                reason: error.to_string(),
            }
        }
        _ => StoreProviderError::MembershipConflict {
            reason: error.to_string(),
        },
    }
}
