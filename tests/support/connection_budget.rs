// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
use voteboat::{membership::*, secure::LocalIdentity};
fn budget(
    owner: &Owner,
    limit: usize,
    history: BTreeMap<NodeId, StoreIdentity>,
) -> ConnectionBudget {
    ConnectionBudget::new(
        LocalIdentity {
            node: node(1),
            store: owner.identity().store,
        },
        limit,
        history,
    )
    .unwrap()
}
fn incoming(group_id: u128, peer: u64, physical: u128) -> Event {
    let initial = bootstrap(group_id, 1);
    let next = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        initial.policy,
        initial.voter_stores,
        [(node(peer), identity(physical))].into(),
    )
    .unwrap();
    Event::Receive(Message {
        group: group(group_id),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: StoreBinding {
            identity: identity(2),
            session: StoreSession::new(1).unwrap(),
        },
        to: node(1),
        term: 1,
        context: RequestContext {
            origin: StoreBinding {
                identity: identity(1),
                session: StoreSession::new(1).unwrap(),
            },
            sequence: 800,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            leader_commit: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: OperationId::new(800).unwrap(),
                    expected: ConfigurationId::new(1).unwrap(),
                    change: ConfigurationChange::Learners(next),
                })),
            }],
        },
    })
}
#[test]
fn competing_queued_changes_reserve_union_and_protocol_rejection_releases_only_queue_credits() {
    let (mut owner, _) = single(2);
    owner
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    let first = owner.admit_tracked(group(1), incoming(1, 2, 2)).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    let second = incoming(2, 3, 3);
    let rejected = owner.admit_tracked(group(2), second.clone()).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::PeerCapacity);
    assert_eq!(*rejected.event, second);
    let step = owner.advance(MonoTime(0), 1).unwrap().pop().unwrap();
    assert_eq!(step.admission, Some(first));
    assert_eq!(step.error, Some(RaftError::InvalidMessage));
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(0));
    let next = owner.admit_tracked(group(2), *rejected.event).unwrap();
    assert_eq!(next.sequence, first.sequence + 1);
    owner.close_admission().unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    assert_eq!(
        owner.admit(group(1), incoming(1, 4, 4)).unwrap_err().reason,
        RuntimeError::Closed
    );
    owner.advance(MonoTime(0), 2).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(0));
    assert!(owner.is_drained());
    assert!(!owner.is_failed());
}
#[test]
fn shared_future_peer_is_charged_once_but_conflicting_store_cannot_share_a_slot() {
    let (mut owner, _) = single(2);
    owner
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    owner.admit(group(1), incoming(1, 2, 2)).unwrap();
    owner.admit(group(2), incoming(2, 2, 2)).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    assert_eq!(
        owner
            .admit(group(2), incoming(2, 2, 22))
            .unwrap_err()
            .reason,
        RuntimeError::PeerStoreConflict
    );
    owner.advance(MonoTime(0), 2).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(0));
    assert!(owner
        .connection_budget()
        .unwrap()
        .retained_peers()
        .next()
        .is_none());
}
#[test]
fn budget_replacement_preserves_history_and_rejects_tightening_below_owned_union() {
    let (mut owner, _) = single(2);
    owner
        .set_connection_budget(budget(&owner, 2, [(node(2), identity(2))].into()))
        .unwrap();
    owner.admit(group(1), incoming(1, 3, 3)).unwrap();
    assert_eq!(
        owner.set_connection_budget(budget(&owner, 1, Default::default())),
        Err(EffectOwnerError::Runtime(RuntimeError::PeerCapacity))
    );
    assert_eq!(owner.connection_budget().unwrap().limit(), 2);
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(2));
    owner.advance(MonoTime(0), 1).unwrap();
    owner
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    assert_eq!(
        owner
            .admit(group(2), incoming(2, 2, 22))
            .unwrap_err()
            .reason,
        RuntimeError::PeerStoreConflict
    );
    assert_eq!(
        owner.admit(group(2), incoming(2, 3, 3)).unwrap_err().reason,
        RuntimeError::PeerCapacity
    );
    let mut wrong = LocalIdentity {
        node: node(1),
        store: owner.identity().store,
    };
    wrong.store.session = StoreSession::new(2).unwrap();
    assert_eq!(
        owner.set_connection_budget(ConnectionBudget::new(wrong, 1, Default::default()).unwrap()),
        Err(EffectOwnerError::Runtime(RuntimeError::WrongOwner))
    );
    assert_eq!(
        owner.connection_budget().unwrap().local().store,
        owner.identity().store
    );
    assert!(ConnectionBudget::new(wrong, 0, Default::default()).is_err());
}

#[test]
fn granted_authority_reply_reserves_candidate_until_protocol_rejection() {
    let (mut owner, _) = single(1);
    owner
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    let Event::Receive(mut message) = incoming(1, 2, 2) else {
        unreachable!()
    };
    message.rpc = Rpc::AuthorityReply {
        candidate: voteboat::secure::PeerIdentity {
            node: node(2),
            store: identity(2),
        },
        configuration: ConfigurationId::new(2).unwrap(),
        committed_index: 1,
        committed_term: 1,
        granted: true,
    };
    owner.admit(group(1), Event::Receive(message)).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    assert_eq!(
        owner.admit(group(1), incoming(1, 3, 3)).unwrap_err().reason,
        RuntimeError::PeerCapacity
    );
    let step = owner.advance(MonoTime(0), 1).unwrap().pop().unwrap();
    // A resource reservation is not a matching query or witness authorization.
    assert_eq!(step.error, Some(RaftError::WrongIdentity));
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(0));
    assert!(!owner.is_failed());
}

#[test]
fn stopping_a_group_returns_queued_reservation_and_registration_cannot_overbook_it() {
    let (owner, _) = single(2);
    let mut shard = Shard::new(
        owner.identity(),
        ShardLimits {
            max_groups: 100,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for g in [group(1), group(2)] {
        shard
            .register(
                Raft::recover(
                    node(1),
                    owner.identity().store,
                    owner.core(g).unwrap().state().clone(),
                    LogLimits::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    shard
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    let ticket = shard.admit_tracked(group(1), incoming(1, 2, 2)).unwrap();
    let mut state = owner.core(group(1)).unwrap().state().clone();
    state.bootstrap = bootstrap(3, 3);
    state.bootstrap.policy = voteboat::quorum::Policy::new(
        voteboat::quorum::Tree::Majority(vec![
            voteboat::quorum::Tree::Voter(node(1)),
            voteboat::quorum::Tree::Voter(node(3)),
        ]),
        voteboat::quorum::Limits::default(),
    )
    .unwrap();
    state.bootstrap.voter_stores.remove(&node(2));
    let recovered = || {
        Raft::recover(
            node(1),
            owner.identity().store,
            state.clone(),
            LogLimits::default(),
        )
        .unwrap()
    };
    assert_eq!(shard.register(recovered()), Err(RuntimeError::PeerCapacity));
    assert!(shard.core(group(3)).is_none());
    let stopped = shard.stop_group(group(1)).unwrap();
    assert_eq!(stopped.admissions, vec![ticket]);
    assert_eq!(stopped.queued, vec![incoming(1, 2, 2)]);
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(0)));
    shard.register(recovered()).unwrap();
    shard.admit(group(2), incoming(2, 3, 3)).unwrap();
    assert_eq!(shard.reserved_connection_peers(), Ok(Some(1)));
    assert_eq!(
        shard
            .connection_budget()
            .unwrap()
            .retained_peers()
            .collect::<Vec<_>>(),
        vec![(node(3), identity(3))]
    );
}

fn local_configuration(group_id: u128, peer: u64, physical: u128) -> Event {
    let Event::Receive(Message {
        rpc: Rpc::Append { mut entries, .. },
        ..
    }) = incoming(group_id, peer, physical)
    else {
        unreachable!()
    };
    let EntryPayload::Configuration(record) = entries.remove(0).payload else {
        unreachable!()
    };
    Event::Configure(Box::new(ConfigurationProposal {
        record: *record,
        readiness: vec![],
        requirements: ReadinessRequirements {
            application_schema: 1,
            command_bytes: 8,
            snapshot_bytes: 4096,
        },
    }))
}
#[test]
fn local_configuration_events_reserve_peer_union_before_ownership_and_release_on_rejection() {
    let (mut owner, _) = single(2);
    owner
        .set_connection_budget(budget(&owner, 1, Default::default()))
        .unwrap();
    let event = local_configuration(1, 2, 2);
    let existing = owner
        .core(group(1))
        .unwrap()
        .effect_reservation(1024)
        .unwrap();
    let prospective = owner
        .core(group(1))
        .unwrap()
        .event_effect_reservation(&event, 1024)
        .unwrap();
    assert!(prospective > existing);
    let first = owner.admit_tracked(group(1), event).unwrap();
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(1));
    let rejected = owner
        .admit_tracked(group(2), local_configuration(2, 3, 3))
        .unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::PeerCapacity);
    assert_eq!(*rejected.event, local_configuration(2, 3, 3));
    assert_eq!(
        owner
            .admit(group(2), local_configuration(2, 2, 22))
            .unwrap_err()
            .reason,
        RuntimeError::PeerStoreConflict
    );
    let steps = owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(steps[0].admission, Some(first));
    assert_eq!(steps[0].error, Some(RaftError::NotLeader));
    assert_eq!(owner.reserved_connection_peers().unwrap(), Some(0));
    let second = owner.admit_tracked(group(2), *rejected.event).unwrap();
    assert_eq!(second.sequence, first.sequence + 1);
    owner.advance(MonoTime(0), 1).unwrap();
    assert!(owner.is_drained());
}
